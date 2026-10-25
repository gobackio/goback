// Package index holds what a store index reports about its sets and the
// store policy, as the admin surface shows them.
package index

import (
	"bufio"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/backup/schedule"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"
)

// Set states.
const (
	// SetActive accepts commits.
	SetActive = "active"
	// SetClosing was deleted and keeps its commits through the trash window.
	SetClosing = "closing"
	// SetDeleted has no live commits left.
	SetDeleted = "deleted"
)

// SetInfo is a set with, as the last measurement of its sets found them,
// the size of its newest live commit, and, as of the last garbage
// collection, what its objects take up in the store and the size of the
// distinct content they carry before compression. A set never measured
// reports zero sizes.
type SetInfo struct {
	ID               int64
	Name             string
	State            string
	LogicalSize      int64
	PhysicalSize     int64
	DeduplicatedSize int64
	// UniqueSize is the size of every distinct file version a live commit
	// of the set holds, each counted once: what storing each version of
	// each file once would take.
	UniqueSize int64
	// AloneSize is what the set would take up were it the only set, and
	// ExclusiveSize what of that no other set holds, as of the last
	// garbage collection.
	AloneSize, ExclusiveSize int64
	// DeduplicatedAloneSize is DeduplicatedSize were the set the only set.
	DeduplicatedAloneSize int64
	// Schedule is what an agent last reported it expects of the set; nil
	// when none did within schedule.StaleAfter.
	Schedule *schedule.Reported
	// NewestAt is when the store received the set's newest live commit;
	// zero for none.
	NewestAt time.Time
}

// Late reports whether the set, being active, is late at now by its
// Schedule, as schedule.Reported.Late tells.
func (s SetInfo) Late(now time.Time) bool {
	return s.State == SetActive && s.Schedule != nil && s.Schedule.Late(s.NewestAt, now)
}

// SetQuery picks and orders a page of sets. The zero SetQuery lists
// every set by name.
type SetQuery struct {
	// States keeps the sets in one of these states; none keeps all.
	States []string
	// Match keeps the sets whose name holds it, ignoring case, and those
	// Named lists; an empty Match keeps every set.
	Match string
	Named []string
	// BySize lists only the sets a garbage collection measured, largest
	// PhysicalSize first and by name within a size, instead of by name.
	BySize bool
	// After and AfterSize are the Name and PhysicalSize of the last set of
	// the previous page; AfterSize counts only with BySize. An empty After
	// starts at the first set.
	After     string
	AfterSize int64
	// Limit is the most sets a page holds; zero holds every one.
	Limit int
}

// StorePolicy is the store's write policy with its acknowledgement state.
type StorePolicy struct {
	Policy            storekey.Policy
	KeyAcknowledgedAt *time.Time
}

// SetRetention is a set's retention as an operator sees it: the policy
// set on it, nil when it inherits the store's, the policy in force, and
// whether retirement waits for a policy after a rebuild.
type SetRetention struct {
	Policy    *retention.Policy
	Effective retention.Policy
	Paused    bool
}

// Windows are the store's trash window in days: how long a commit deleted
// by hand waits before its tombstone.
type Windows struct {
	TrashDays int
}

// Version is one version of a path with From, when the store received
// the commit that first held it. No two versions of a path share a From.
type Version struct {
	Node *proto.TreeNode
	From time.Time
}

// Unretired is what UnretireCommits did with one commit.
type Unretired struct {
	backup.Revival
	// Set names the commit's set.
	Set string
	// RetainedBy is why the set's policy keeps the commit once it is back,
	// as CommitDetail names it; empty when the policy retires it again or
	// the commit stays tombstoned.
	RetainedBy string
}

// Retired is a commit retention let go: when the set's policy retired it,
// when its tombstone became durable and under which policy.
type Retired struct {
	Ref *proto.Ref
	// SetID and Set name the commit's set; a set's name is unique only
	// among the sets a caller sees.
	SetID int64
	Set   string
	// Timestamp is when the commit was taken and ReceivedAt when the store
	// received it.
	Timestamp, ReceivedAt time.Time
	// RetiredAt is when the policy retired the commit, zero for one deleted
	// by hand, and TombstonedAt when its tombstone became durable, zero
	// while none has.
	RetiredAt, TombstonedAt time.Time
	// Policy is the set's policy that retired the commit; nil when the
	// retirement predates its recording.
	Policy *retention.Policy
	// Deleted marks a commit deleted by hand, whose trash window let it go
	// unless retention did first.
	Deleted bool
	// Partial marks a checkpoint along the way of a backup.
	Partial bool
	// State is where the commit stands now.
	State RetiredState
}

// RetiredState is where a retired commit stands.
type RetiredState int

// The states of a retired commit.
const (
	// RetiredPending is retired with its tombstone still to be written.
	RetiredPending RetiredState = iota
	// RetiredHeld is tombstoned while the store still holds the commit
	// object, so UnretireCommits may bring it back.
	RetiredHeld
	// RetiredGone is tombstoned and collected: the store holds no copy of
	// the commit object.
	RetiredGone
	// RetiredLive is live again, unretired since.
	RetiredLive
)

// CommitDetail is a commit together with what the index knows about it
// beyond the stored object: how big the set was when it was taken, and
// which retention rule is keeping it.
type CommitDetail struct {
	Commit *proto.Commit
	// Ref is the commit object's own ref, which names it to a caller that
	// wants to pin or read it.
	Ref *proto.Ref
	// LogicalSize is what the set's files held at this commit, and Files
	// how many there were; nil when nothing measured it, which is every
	// commit written before the index started recording it.
	LogicalSize, Files *int64
	// RetainedBy names the retention rules keeping this commit, comma
	// separated: "last", "within", "pinned", "hourly", "daily", "weekly",
	// "monthly", "yearly". It is empty for a commit retention has not evaluated yet
	// or has retired.
	RetainedBy string
	// Incomplete reports a commit indexed around objects the store no
	// longer holds, which cannot be restored whole.
	Incomplete bool
}

// Reindexed is a table whose indexes a reindex rebuilt because enough of
// its rows changed since the last one.
type Reindexed struct {
	Table string
	// Churn is the rows inserted, updated and deleted since the table's
	// last reindex, and Live the rows it holds.
	Churn, Live int64
	Indexes     []RebuiltIndex
	Took        time.Duration
}

// RebuiltIndex is one index a reindex rebuilt, with its size in bytes
// before and after.
type RebuiltIndex struct {
	Name          string
	Before, After int64
}

// OrphanCommit is a commit the store holds that the index has no row for
// and no tombstone retires: retention never sees it, and every collection
// keeps what it reaches.
type OrphanCommit struct {
	Ref *proto.Ref
	// Copies is how many committed archives hold the commit, and Bytes
	// what those copies take up.
	Copies int
	Bytes  int64
}

// PlaceholderSet names the set commits that name no set are indexed
// under; when a set of that name exists, the first free of
// PlaceholderSet-2, PlaceholderSet-3, … is used instead.
const PlaceholderSet = "legacy"

// ErrIndexed refuses to index a commit the index already has a row for.
var ErrIndexed = errors.New("commit is already indexed")

// IndexedSet is what indexing a list of commits did, or on a dry run
// would do, with those of one set.
type IndexedSet struct {
	// SetID is 0 for a set a dry run would create under a fresh id.
	SetID int64
	Set   string
	// Created is a set that did not exist before.
	Created bool
	// Placeholder is a set the commits went to because they name none.
	Placeholder bool
	Commits     int
	// Chained is whether each commit names the one received before it as
	// its parent.
	Chained bool
	// Oldest and Newest are the receipt times of the first and last commit.
	Oldest, Newest time.Time
	// Indexed counts the commits indexed, Tied those of them indexed a
	// microsecond after their set's newest, and Behind those received
	// before the set's newest and left out; all 0 on a dry run.
	Indexed, Tied, Behind int
}

// ReadRefs reads refs printed as hex, one per line, skipping blank lines.
func ReadRefs(r io.Reader) ([]*proto.Ref, error) {
	var refs []*proto.Ref

	lines := bufio.NewScanner(r)
	for n := 1; lines.Scan(); n++ {
		line := strings.TrimSpace(lines.Text())
		if line == "" {
			continue
		}

		hash, err := hex.DecodeString(line)
		if err != nil || len(hash) != proto.HashSize {
			return nil, fmt.Errorf("line %d: %q is not a ref: want %d hex bytes", n, line, proto.HashSize)
		}

		refs = append(refs, &proto.Ref{Hash: hash})
	}

	return refs, lines.Err()
}
