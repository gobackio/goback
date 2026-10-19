// Package views is what goback's commands print as JSON.
package views

import (
	"encoding/hex"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/index"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/maintenance"
	"github.com/gobackio/goback/storage/pack"
)

//go:generate mapper .

// mapper:generate
type Mapper interface {
	// field:Ref from:"." using:"CommitRef"
	// field:Tree using:"Hex"
	// field:Parent using:"Hex"
	// field:Set from:"BackupSet"
	// field:Agent from:"AgentId"
	// field:Time from:"Timestamp" using:"Unix"
	Commit(in *proto.Commit) CommitView

	// field:Name from:"Stat.Name" using:"Text"
	// field:Type from:"Stat.Type" using:"Kind"
	// field:Size from:"Stat.Size"
	// field:Mode from:"Stat.Mode"
	// field:Modified from:"Stat.MtimeNs" using:"UnixNano"
	// field:Target from:"Stat.LinkTarget" using:"Text"
	// field:Ref using:"Hex"
	Node(in *proto.TreeNode) NodeView

	// field:Commit from:"Ref" using:"Hex"
	// field:Base using:"Hex"
	Walk(in *backup.WalkResult) WalkView

	// field:Seconds from:"Duration" using:"Seconds"
	Report(in *pack.CollectReport) ReportView

	// field:Seconds from:"Duration" using:"Seconds"
	Compact(in *pack.CompactReport) CompactView

	// field:Collected from:"-"
	// field:Compacted from:"-"
	Maintenance(in maintenance.Ran) MaintenanceView

	// field:Seconds from:"Took" using:"Seconds"
	Reindexed(in index.Reindexed) ReindexedView

	RebuiltIndex(in index.RebuiltIndex) RebuiltIndexView

	// field:Ref using:"Hex"
	OrphanCommit(in index.OrphanCommit) OrphanCommitView

	ReIndex(in backup.ReIndexReport) ReIndexView

	IndexedSet(in index.IndexedSet) IndexedSetView

	// field:Keep from:"Brackets"
	// field:KeepWithin using:"Within"
	// field:Flags from:"-"
	Policy(in retention.Policy) PolicyView

	Bracket(in retention.Bracket) BracketView

	// field:Pin from:"Ref" using:"Hex"
	// field:Target using:"Hex"
	// field:Received from:"ReceivedAtNs" using:"UnixNano"
	Pin(in *proto.PinInfo) PinView

	// field:CommitView from:"Commit"
	// field:Deleted from:"DeletedAtNs" using:"UnixNano"
	// field:Expires from:"ExpiresAtNs" using:"UnixNano"
	TrashedCommit(in *proto.TrashedCommit) TrashedCommitView
	// field:Commit from:"Revival.Commit" using:"Hex"
	// field:Missing from:"Revival.Missing" using:"Hexes"
	// field:MissingCount from:"Revival.MissingCount"
	Unretired(in index.Unretired) UnretiredView
}

// Hex is a ref as the hex its hash prints as, empty for none.
func Hex(ref *proto.Ref) string {
	if ref == nil {
		return ""
	}

	return hex.EncodeToString(ref.Hash)
}

// Hexes is each ref as Hex prints it.
func Hexes(refs []*proto.Ref) []string {
	out := make([]string, len(refs))
	for i, ref := range refs {
		out[i] = Hex(ref)
	}

	return out
}

// CommitRef is the hex of the ref commit is stored under.
func CommitRef(commit *proto.Commit) string {
	return Hex(proto.NewObject(commit).Ref())
}

// Unix is a time in seconds since the epoch, in UTC.
func Unix(seconds int64) time.Time {
	return time.Unix(seconds, 0).UTC()
}

// UnixNano is a time in nanoseconds since the epoch, in UTC.
func UnixNano(nanos int64) time.Time {
	return time.Unix(0, nanos).UTC()
}

// Text is a stored name as text.
func Text(name []byte) string {
	return string(name)
}

// Kind names a node type: file, directory or symlink.
func Kind(t proto.NodeType) string {
	switch t {
	case proto.NodeType_NODE_DIRECTORY:
		return "directory"
	case proto.NodeType_NODE_SYMLINK:
		return "symlink"
	}

	return "file"
}

// Seconds is d in seconds.
func Seconds(d time.Duration) float64 {
	return d.Seconds()
}

// Within is how long a policy keeps everything, "forever" for no end and
// empty for not at all.
func Within(d time.Duration) string {
	switch {
	case d == time.Duration(1<<63-1):
		return "forever"
	case d > 0:
		return d.String()
	}

	return ""
}

// CommitView is a commit as JSON output shows it.
type CommitView struct {
	Ref        string            `json:"ref"`
	Tree       string            `json:"tree"`
	Parent     string            `json:"parent,omitempty"`
	Set        string            `json:"set"`
	Agent      string            `json:"agent,omitempty"`
	Time       time.Time         `json:"time"`
	Consistent bool              `json:"consistent"`
	Partial    bool              `json:"partial,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// TrashedCommitView is a deleted commit as JSON output shows it: the
// commit, when it was deleted and when retirement tombstones it.
type TrashedCommitView struct {
	CommitView
	Deleted time.Time `json:"deleted"`
	Expires time.Time `json:"expires"`
}

// UnretiredView is what unretiring did with one commit, as JSON output
// shows it: a commit missing anything stays tombstoned.
type UnretiredView struct {
	Commit       string   `json:"commit"`
	Set          string   `json:"set"`
	Missing      []string `json:"missing,omitempty"`
	MissingCount int      `json:"missing_count,omitempty"`
	RetainedBy   string   `json:"retained_by,omitempty"`
}

// NodeView is a file or directory as JSON output shows it.
type NodeView struct {
	Name     string    `json:"name"`
	Type     string    `json:"type"`
	Size     int64     `json:"size"`
	Mode     uint32    `json:"mode"`
	Modified time.Time `json:"modified"`
	Target   string    `json:"target,omitempty"`
	Ref      string    `json:"ref,omitempty"`
}

// WalkView is what a backup run did, as JSON output shows it.
type WalkView struct {
	Commit        string `json:"commit"`
	Base          string `json:"base,omitempty"`
	SameTree      bool   `json:"same_tree,omitempty"`
	Files         int64  `json:"files"`
	Bytes         int64  `json:"bytes"`
	Uploaded      int64  `json:"uploaded"`
	Reused        int64  `json:"reused"`
	Read          int64  `json:"read"`
	Torn          int64  `json:"torn"`
	Unreadable    int64  `json:"unreadable"`
	Skipped       int64  `json:"skipped"`
	Placeholders  int64  `json:"placeholders"`
	Excluded      int64  `json:"excluded"`
	ExcludedBytes int64  `json:"excluded_bytes"`
	Checkpoints   int64  `json:"checkpoints"`
	Assumed       int64  `json:"assumed"`
	Repaired      int64  `json:"repaired"`
}

// ReportView is a collection report as JSON output shows it; the per-set
// maps are keyed by set id.
type ReportView struct {
	Generation           uint64           `json:"generation"`
	Waiting              uint64           `json:"waiting,omitempty"`
	Archives             int              `json:"archives"`
	Objects              uint64           `json:"objects"`
	Roots                int              `json:"roots"`
	Marked               uint64           `json:"marked"`
	DeadObjects          uint64           `json:"dead_objects"`
	DeadBytes            uint64           `json:"dead_bytes"`
	Resumed              int              `json:"resumed"`
	ErasedArchives       int              `json:"erased_archives"`
	Condemned            int              `json:"condemned"`
	SweepSkipped         string           `json:"sweep_skipped,omitempty"`
	Published            int              `json:"published"`
	Swept                int              `json:"swept"`
	ReclaimedObjects     uint64           `json:"reclaimed_objects"`
	ReclaimedBytes       uint64           `json:"reclaimed_bytes"`
	CopiedBytes          uint64           `json:"copied_bytes"`
	Purged               int              `json:"purged"`
	Seconds              float64          `json:"seconds"`
	SetBytes             map[int64]uint64 `json:"set_bytes,omitempty"`
	SetDeduplicated      map[int64]uint64 `json:"set_deduplicated,omitempty"`
	SetAlone             map[int64]uint64 `json:"set_alone,omitempty"`
	SetExclusive         map[int64]uint64 `json:"set_exclusive,omitempty"`
	SetDeduplicatedAlone map[int64]uint64 `json:"set_deduplicated_alone,omitempty"`
	Unattributed         uint64           `json:"unattributed"`
}

// CompactView is a compaction report as JSON output shows it.
type CompactView struct {
	Candidates     int     `json:"candidates"`
	CandidateBytes uint64  `json:"candidate_bytes"`
	Rewritten      int     `json:"rewritten"`
	ReadBytes      uint64  `json:"read_bytes"`
	Written        int     `json:"written"`
	WrittenBytes   uint64  `json:"written_bytes"`
	Moved          uint64  `json:"moved"`
	CopiedBytes    uint64  `json:"copied_bytes"`
	Superseded     uint64  `json:"superseded"`
	ReclaimedBytes uint64  `json:"reclaimed_bytes"`
	Seconds        float64 `json:"seconds"`
}

// OrphanCommitView is a commit the store holds that the index has no row
// for, as JSON output shows it.
type OrphanCommitView struct {
	Ref    string `json:"ref"`
	Copies int    `json:"copies"`
	Bytes  int64  `json:"bytes"`
}

// ReIndexView is what a rebuild of the index found out of order, as JSON
// output shows it.
type ReIndexView struct {
	Tied    int `json:"tied"`
	Behind  int `json:"behind"`
	Unnamed int `json:"unnamed"`
}

// IndexedSetView is what goback index commits did, or would do, with the
// commits of one set, as JSON output shows it.
type IndexedSetView struct {
	SetID       int64     `json:"set_id"`
	Set         string    `json:"set"`
	Created     bool      `json:"created"`
	Placeholder bool      `json:"placeholder"`
	Commits     int       `json:"commits"`
	Chained     bool      `json:"chained"`
	Oldest      time.Time `json:"oldest"`
	Newest      time.Time `json:"newest"`
	Indexed     int       `json:"indexed"`
	Tied        int       `json:"tied"`
	Behind      int       `json:"behind"`
}

// MaintenanceView is what goback maintain did; compacted and collected
// are absent when that job did not run.
type MaintenanceView struct {
	Swept     bool            `json:"swept"`
	Compacted *CompactView    `json:"compacted,omitempty"`
	Retired   int             `json:"retired"`
	Presence  int             `json:"presence"`
	Measured  int             `json:"measured"`
	Collected *ReportView     `json:"collected,omitempty"`
	Reindexed []ReindexedView `json:"reindexed,omitempty"`
}

// ReindexedView is a table whose indexes maintenance rebuilt, as JSON
// output shows it.
type ReindexedView struct {
	Table   string             `json:"table"`
	Churn   int64              `json:"churn"`
	Live    int64              `json:"live"`
	Indexes []RebuiltIndexView `json:"indexes"`
	Seconds float64            `json:"seconds"`
}

// RebuiltIndexView is a rebuilt index with its size in bytes before and
// after, as JSON output shows it.
type RebuiltIndexView struct {
	Name   string `json:"name"`
	Before int64  `json:"before"`
	After  int64  `json:"after"`
}

// PolicyView is a retention policy as JSON output shows it.
type PolicyView struct {
	KeepLast   int           `json:"keep_last"`
	Keep       []BracketView `json:"keep"`
	KeepWithin string        `json:"keep_within,omitempty"`
	Flags      string        `json:"flags"`
}

// BracketView's count is zero for the tail, which keeps forever.
type BracketView struct {
	Period string `json:"period"`
	Count  int    `json:"count"`
}

// PinView is a pin as JSON output shows it.
type PinView struct {
	Pin      string            `json:"pin"`
	Target   string            `json:"target"`
	Received time.Time         `json:"received"`
	Metadata map[string]string `json:"metadata,omitempty"`
}
