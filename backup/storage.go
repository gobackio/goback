package backup

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/gobackio/goback/proto"
)

// ObjectReceiver is called for every object a Walk visits; a non-nil error
// stops the walk.
type ObjectReceiver func(*proto.Object) error

var (
	// ErrNotImplemented is returned by a store or index that lacks the
	// requested feature.
	ErrNotImplemented = errors.New("the store doesn't implement this feature")
	// ErrNotFound is returned when nothing matches the given ref or set.
	ErrNotFound = errors.New("the requested object was not found")
	// ErrLost is returned for an entry of a tree whose content the store
	// lost.
	ErrLost = errors.New("the store lost the entry's content")
	// ErrIncomplete is returned for a restore with Delete of a commit that
	// lost entries, which would remove them from the directory.
	ErrIncomplete = errors.New("the commit lost entries, which a restore deleting what it lacks would remove from the directory")
	// ErrDanglingRef is returned when an object references objects the store
	// does not hold.
	ErrDanglingRef = errors.New("object references missing objects")
	// ErrSetClosed is returned for a commit into a set that is being
	// deleted.
	ErrSetClosed = errors.New("set is closed")
	// ErrTombstoned is returned when a commit or pin already has a
	// tombstone; there is no undo past that point.
	ErrTombstoned = errors.New("object is tombstoned")
	// ErrNewestCommit is returned when deleting the only live commit of an
	// active set; delete the set instead.
	ErrNewestCommit = errors.New("the only commit of a set cannot be deleted")
	// ErrPinned refuses the deletion of a commit a live pin holds.
	ErrPinned = errors.New("commit is pinned")
	// ErrCommitDenied is returned by BeginCommit when the store refuses the
	// run, with the reason.
	ErrCommitDenied = errors.New("commit refused")

	// ErrSetName refuses a set name that reads as a set id.
	ErrSetName = errors.New("a set name cannot be only digits, which name a set by id")

	// ErrInvalidCount refuses a CountCommits call with an unknown period or
	// time zone, an empty range, or more than MaxCountPeriods periods.
	ErrInvalidCount = errors.New("invalid commit count")

	// ErrSessionLost is returned for a commit whose session lost objects it
	// had already been told were stored; backing up again stores them anew.
	ErrSessionLost = errors.New("the session lost objects it had stored")

	// ErrQuotaExceeded is returned when the store refuses a read because
	// the caller used up what it may read; retrying does not help until
	// the quota is raised or renews.
	ErrQuotaExceeded = errors.New("read quota exceeded")
)

// MissingError is what a commit fails with when the store lacks objects its
// session's objects reference: the caller stores Refs and puts the commit
// again. It matches ErrDanglingRef. For several commits put together,
// Commits names what each lacks, by its place among them, and Refs all of
// it; a commit that lacks nothing has none.
type MissingError struct {
	Refs    []*proto.Ref
	Commits [][]*proto.Ref
}

func (e *MissingError) Error() string {
	return fmt.Sprintf("%v: %d objects the session's objects reference", ErrDanglingRef, len(e.Refs))
}

func (e *MissingError) Is(target error) bool {
	return target == ErrDanglingRef
}

// CommitBatcher is a store or index that takes several commits of a
// session at once. PutCommits stores them as one commit of the session
// would be, in the order given, which for each set is the order its
// commits were taken in. Either all of them are stored, stamped in place
// as Put stamps one, or none is; a MissingError names what each lacks,
// and the session stays open to store that and put them all again.
// ErrNotImplemented means the store takes one commit per session.
type CommitBatcher interface {
	PutCommits(ctx context.Context, commits []*proto.Object) error
}

// PutCommits puts commits in store, together when it is a CommitBatcher
// and otherwise one at a time, which in a session is ErrNotImplemented for
// more than one.
func PutCommits(ctx context.Context, store ObjectStore, commits []*proto.Object) error {
	if batcher, ok := store.(CommitBatcher); ok {
		return batcher.PutCommits(ctx, commits)
	}

	if _, ok := SessionFromContext(ctx); ok && len(commits) > 1 {
		return fmt.Errorf("%w: the store takes one commit per session", ErrNotImplemented)
	}

	for _, commit := range commits {
		if err := store.Put(ctx, commit); err != nil {
			return err
		}
	}

	return nil
}

// CommitChecker is a store that may check, as a session commits, that it
// holds everything the session's objects reference, failing with a
// MissingError otherwise. Writes in a session to a store that does are not
// checked one by one, so they may come in any order.
type CommitChecker interface {
	ChecksAtCommit() bool
}

// SetID reads s as a set id, which a set given by digits alone is; ok is
// false for a set name. Digits too many for an id give id 0, which no set
// has.
func SetID(s string) (id uint64, ok bool) {
	if s == "" || strings.Trim(s, "0123456789") != "" {
		return 0, false
	}

	id, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, true
	}

	return id, true
}

// HeadKeeper is a store that keeps each set's latest complete commit
// beside its objects, so a restore finds the set without an index.
type HeadKeeper interface {
	// AdvanceHead points the commit's set at it, unless the set already
	// points at one received later.
	AdvanceHead(commit *proto.Object) error
}

// References lists the refs an object points at directly: a commit's tree,
// a tree's nodes with their security descriptors and streams, a tree's
// splits, a file's parts and splits, a pin's target.
func References(obj *proto.Object) []*proto.Ref {
	switch t := obj.GetObject().(type) {
	case *proto.Object_Commit:
		return []*proto.Ref{t.Commit.GetTree()}
	case *proto.Object_Pin:
		return []*proto.Ref{t.Pin.GetTarget()}
	case *proto.Object_Tree:
		tree := t.Tree
		refs := make([]*proto.Ref, 0, len(tree.Nodes)+len(tree.Splits))
		for _, node := range tree.Nodes {
			if node.Ref != nil {
				refs = append(refs, node.Ref)
			}
			refs = append(refs, node.GetStat().MetadataRefs()...)
		}
		return append(refs, tree.Splits...)
	case *proto.Object_File:
		file := t.File
		refs := make([]*proto.Ref, 0, len(file.Parts)+len(file.Splits))
		for _, part := range file.Parts {
			if part.Ref != nil {
				refs = append(refs, part.Ref)
			}
		}
		return append(refs, file.Splits...)
	}

	return nil
}

// CheckReferences fails with ErrDanglingRef when the store lacks any object
// the given one references. Indexes call it before accepting a Put so a
// stored tree always has its children.
func CheckReferences(ctx context.Context, store ObjectStore, obj *proto.Object) error {
	for _, ref := range References(obj) {
		ok, err := store.Has(ctx, ref)
		if err != nil {
			return err
		}

		if !ok {
			return fmt.Errorf("%w: %s %x needs %x", ErrDanglingRef, obj.Type(), obj.Ref().Hash, ref.Hash)
		}
	}

	return nil
}

// CheckAllReferences is CheckReferences for many objects in one lookup; an
// object may reference others among them.
func CheckAllReferences(ctx context.Context, store ObjectStore, objects []*proto.Object) error {
	among := make(map[string]bool, len(objects))
	for _, obj := range objects {
		among[string(obj.Ref().GetHash())] = true
	}

	var (
		refs []*proto.Ref
		by   []*proto.Object
		seen = map[string]bool{}
	)

	for _, obj := range objects {
		for _, ref := range References(obj) {
			if ref == nil || among[string(ref.GetHash())] || seen[string(ref.GetHash())] {
				continue
			}

			seen[string(ref.GetHash())] = true
			refs = append(refs, ref)
			by = append(by, obj)
		}
	}

	if len(refs) == 0 {
		return nil
	}

	held, err := HasAll(ctx, store, refs)
	if err != nil {
		return err
	}

	for i, ok := range held {
		if !ok {
			return fmt.Errorf("%w: %s %x needs %x", ErrDanglingRef, by[i].Type(), by[i].Ref().Hash, refs[i].Hash)
		}
	}

	return nil
}

// ByReference groups objects so that each group comes after every group
// holding an object it references.
func ByReference(objects []*proto.Object) [][]*proto.Object {
	at := make(map[string]int, len(objects))
	for i, obj := range objects {
		at[string(obj.Ref().GetHash())] = i
	}

	depth := make([]int, len(objects))
	for i := range depth {
		depth[i] = -1
	}

	var levelOf func(i int) int
	levelOf = func(i int) int {
		if depth[i] >= 0 {
			return depth[i]
		}

		depth[i] = 0
		for _, ref := range References(objects[i]) {
			if j, ok := at[string(ref.GetHash())]; ok && j != i {
				depth[i] = max(depth[i], levelOf(j)+1)
			}
		}

		return depth[i]
	}

	var levels [][]*proto.Object

	for i, obj := range objects {
		level := levelOf(i)
		for len(levels) <= level {
			levels = append(levels, nil)
		}

		levels[level] = append(levels[level], obj)
	}

	return levels
}

//go:generate go run github.com/vektra/mockery/v2 --testonly --inpackage --name ObjectStore

// ObjectStore is content-addressed storage of objects by ref.
type ObjectStore interface {
	// Put stores the object.
	Put(context.Context, *proto.Object) error
	// Get returns the object at the ref, or ErrNotFound.
	Get(context.Context, *proto.Ref) (*proto.Object, error)
	// Delete removes the object at the ref.
	Delete(context.Context, *proto.Ref) error
	// Walk calls the receiver for every object of the type, or of every
	// type when it is INVALID; the bool says whether objects are decoded.
	Walk(context.Context, bool, proto.ObjectType, ObjectReceiver) error
	// Has reports whether the store holds the ref.
	Has(context.Context, *proto.Ref) (bool, error)
}

// A RecordLocator is a store that can say where the stored records of
// many refs sit, neighbours under one location, so a reader fetches them
// itself in a few range reads.
type RecordLocator interface {
	// LocateRecords returns runs holding the records of refs, each
	// record's index naming its ref in refs. A ref it cannot locate is in
	// no run and is read the ordinary way. It authorizes nothing: the
	// caller has already decided the refs are readable.
	LocateRecords(ctx context.Context, refs []*proto.Ref) ([]*proto.LocatedRun, error)
}

// A RecordReader is a store that reads the records of many refs together,
// a range read for each run of neighbours.
type RecordReader interface {
	// ReadRecords returns the object of each of refs it could read, nil
	// for the rest, which are read the ordinary way. It authorizes
	// nothing.
	ReadRecords(ctx context.Context, refs []*proto.Ref) ([]*proto.Object, error)
}

// Eraser deletes as an erasure: the tombstone asks garbage collection to
// rewrite the archives holding the target's objects as soon as its rules
// allow, instead of waiting for the dead ratio or the erasure bound.
type Eraser interface {
	// Erase tombstones the ref as an erasure.
	Erase(context.Context, *proto.Ref) error
}

// Reviver is a store that can take tombstoned commits back while it still
// holds everything they reach.
type Reviver interface {
	// Revive checks what each commit reaches and, unless dryRun, takes back
	// the tombstones of every commit missing nothing and of what it
	// reaches, so collections keep them again. A commit missing anything is
	// reported and left as it is.
	Revive(ctx context.Context, commits []*proto.Ref, dryRun bool) ([]Revival, error)
	// Revived reports whether a revival takes back the commit's tombstone.
	Revived(ctx context.Context, commit *proto.Ref) (bool, error)
	// Holds reports, for each commit, whether the store still holds a copy
	// of the commit object, tombstoned or not; one it does not hold cannot
	// be revived.
	Holds(ctx context.Context, commits []*proto.Ref) ([]bool, error)
}

// Revival is what Revive found under one commit.
type Revival struct {
	Commit *proto.Ref
	// Missing names some of the objects the commit reaches that the store
	// holds no copy of, and MissingCount counts them all.
	Missing      []*proto.Ref
	MissingCount int
	// Busy is set when another process maintains the store, which left
	// the commit unchecked and as it is.
	Busy bool
}

// Whole reports whether the commit was checked and misses nothing.
func (r Revival) Whole() bool { return !r.Busy && r.MissingCount == 0 }

// Counter is implemented by stores that can count their objects.
type Counter interface {
	// Count reports how many objects the store holds, in total and distinct.
	Count() (total uint64, unique uint64, err error)
}
