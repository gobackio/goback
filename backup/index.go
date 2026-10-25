package backup

import (
	"context"
	"time"

	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"
)

// An Index stamps a commit or pin it receives with the receipt time, and a
// commit with its set id, before storing it; the caller reads the ref off
// the stamped object afterwards. A commit that already carries a receipt
// time, such as one an agent stored locally while offline, keeps it if the
// index accepts it.
type Index interface {
	// Open readies the index for use.
	Open() error
	// Close releases the index.
	Close() error

	ObjectStore
	// FileInfo lists the versions of name that live commits of the set hold,
	// newest first, none newer than notAfter, at most count.
	FileInfo(ctx context.Context, set string, name string, notAfter time.Time, count int) ([]*proto.TreeNode, error)
	// CommitInfo lists the live, complete commits of the set, newest first,
	// none newer than notAfter, at most count.
	CommitInfo(ctx context.Context, set string, notAfter time.Time, count int) ([]*proto.Commit, error)
	// LatestCommit returns the ref of the set's newest commit, or ErrNotFound.
	LatestCommit(ctx context.Context, set string) (*proto.Ref, error)
	// ReIndex rebuilds the index from the store's objects.
	ReIndex(ctx context.Context) (ReIndexReport, error)
}

// ReIndexReport is what a rebuild of the index found out of order.
type ReIndexReport struct {
	// Tied counts the commits received at the same time as their set's
	// newest, indexed a microsecond after it.
	Tied int
	// Behind counts the commits received before their set's newest,
	// left out of the index.
	Behind int
	// Unnamed counts the commits that name no set, indexed under a
	// placeholder set.
	Unnamed int
}

// CommitSizer is implemented by indexes that measure how big a set was
// at each commit.
type CommitSizer interface {
	// CommitSizes is CommitInfo with the size of each commit beside it, in
	// the same order.
	CommitSizes(ctx context.Context, set string, notAfter time.Time, count int) ([]*proto.Commit, []*proto.CommitSize, error)
}

// DirLister is implemented by indexes that can list a directory as it
// stood, without walking the stored trees.
type DirLister interface {
	// ReadDir lists what the set held directly under dir at notAfter,
	// sorted by name. Directory entries carry a name and a ref only, since
	// the index keeps no stat for them. An unknown set lists nothing.
	ReadDir(ctx context.Context, set string, dir string, notAfter time.Time) ([]*proto.TreeNode, error)
}

// HeaderWalker visits object headers without loading bodies. Tombstones
// have no body and are only reachable this way.
type HeaderWalker interface {
	// WalkHeaders calls fn for every header of type t; an error from fn
	// stops the walk.
	WalkHeaders(ctx context.Context, t proto.ObjectType, fn func(*proto.ObjectHeader) error) error
}

// ArchiveWalker is implemented by stores that keep objects in archives
// which, once finished, never change and whose names are never reused.
type ArchiveWalker interface {
	// WalkArchives calls fn with every object of type t, loaded, in the
	// archives but the finished ones skip reports true for, and returns
	// the names of all the finished archives it walked or skipped.
	WalkArchives(ctx context.Context, t proto.ObjectType, skip func(archive string) bool, fn ObjectReceiver) ([]string, error)
}

// RefScope is implemented by indexes that know which commit, tree and
// file refs the store's sets reach.
type RefScope interface {
	// References reports whether a set of the store references ref.
	References(ctx context.Context, ref *proto.Ref) (bool, error)
	// ReferencesAll reports the same for each of refs, in their order.
	ReferencesAll(ctx context.Context, refs []*proto.Ref) ([]bool, error)
}

// SetScope narrows RefScope to named sets, for a server that grants a
// caller some of the store's sets.
type SetScope interface {
	// Reachable reports whether one of the named sets references ref.
	Reachable(ctx context.Context, sets []string, ref *proto.Ref) (bool, error)
}

// SetReach is implemented by indexes whose callers may act on only some of
// the store's sets, as a key bound to a set does.
type SetReach interface {
	// Reaches reports whether the caller of ctx may act on the named set.
	Reaches(ctx context.Context, set string) (bool, error)
}

// CommitGrant is the answer to an allowed BeginCommit.
type CommitGrant struct {
	// Policy is the store's write policy for this run; nil when the server
	// holds none, which leaves the agent's key file in charge.
	Policy *storekey.Policy
	// Rescan asks the run to read every file again, because the store lost
	// content it could not name a path for.
	Rescan bool
	// Damaged names the paths whose stored content the store lost. A run
	// reads them again whatever its change detection says, and takes no
	// chunk of theirs on trust.
	Damaged []string
	// Every is how often the store expects a commit of the set; zero
	// expects nothing. A run that finds the latest commit's tree commits it
	// again before half of Every has passed.
	Every time.Duration
}

// CommitGate is asked before a run whether the caller may commit to a set.
// A run asks inside its session, if it has one.
type CommitGate interface {
	// BeginCommit returns the grant for a run on set, or ErrCommitDenied.
	BeginCommit(ctx context.Context, set string) (*CommitGrant, error)
}

// PartReader streams the stored objects of a file's parts, skipping the
// given part indexes; the objects are as uploaded, sealed when the store
// is encrypted.
type PartReader interface {
	// ReadParts calls fn with every part of file whose index is not in
	// skip, in any order and possibly from several goroutines at once.
	ReadParts(ctx context.Context, file *proto.Ref, skip []int, fn func(index int, obj *proto.Object) error) error
}

// MaxFilesPerRead is the most files one ReadFiles call names.
const MaxFilesPerRead = 1024

// FileRead names a file to FilesReader and the indexes of the parts not
// to send.
type FileRead struct {
	Ref  *proto.Ref
	Skip []int
}

// FilesReader reads many files in one call: their file objects and the
// stored objects of their parts, sealed when the store is encrypted.
type FilesReader interface {
	// ReadFiles calls object with each file's object in order, then,
	// unless objectsOnly, part with every part of each file not in its
	// Skip, in any order and possibly from several goroutines at once. A
	// part several of the files hold is handed over once, for any of them.
	ReadFiles(ctx context.Context, files []FileRead, objectsOnly bool, object func(file int, obj *proto.Object) error, part func(file, index int, obj *proto.Object) error) error
}

// Retention is implemented by indexes that keep the commit lifecycle: live,
// then retired, or deleted into a trash window, then tombstoned.
type Retention interface {
	// DeleteCommit retires a commit into its trash window, after which
	// LatestCommit no longer returns it; the only live commit of an active
	// set is refused with ErrNewestCommit.
	DeleteCommit(ctx context.Context, ref *proto.Ref) error
	// UndeleteCommit moves a retired commit back to live; a tombstoned
	// one is refused with ErrTombstoned.
	UndeleteCommit(ctx context.Context, ref *proto.Ref) error
	// TrashedCommits pages through the set's deleted commits that are not
	// tombstoned yet, newest deleted first: those deleted before before, or
	// all when it is zero, at most limit, every one when limit is zero, and
	// then the others deleted at the last one's instant, so its
	// DeletedAtNs is the before of the next page.
	TrashedCommits(ctx context.Context, set string, before time.Time, limit int) ([]*proto.TrashedCommit, error)
	// CountCommits counts the set's live complete commits received in
	// [from, to), or with deleted the ones TrashedCommits lists, per period
	// as it falls in the IANA zone, "" for UTC. It returns the non-empty
	// periods oldest first, each starting at its first instant; the first
	// may start before from. Each names its newest commit, the one
	// received last and of those received at one instant the one indexed
	// last.
	CountCommits(ctx context.Context, set string, period proto.Period, from, to time.Time, zone string, deleted bool) ([]*proto.CommitCount, error)
	// DeleteSet closes a set and retires every commit; erase uses a zero
	// window.
	DeleteSet(ctx context.Context, set string, erase bool) error
	// UndeleteSet reopens a closed set and revives its commits.
	UndeleteSet(ctx context.Context, set string) error
	// Unpin tombstones a pin.
	Unpin(ctx context.Context, pin *proto.Ref) error
	// Pins lists the live pins.
	Pins(ctx context.Context) ([]*proto.PinInfo, error)
}

// MaxCountPeriods is the most periods a CountCommits range may span.
const MaxCountPeriods = 1000

// Retirer runs the retirement job: every retired commit past its window
// gets a tombstone and loses its index rows.
type Retirer interface {
	// Retire tombstones the commits whose window has passed at now and
	// returns how many.
	Retire(ctx context.Context, now time.Time) (int, error)
}
