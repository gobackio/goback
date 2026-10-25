package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math/rand"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gobackio/goback/backup/blobcache"
	"github.com/gobackio/goback/backup/presence"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"

	"go4.org/syncutil"
	"golang.org/x/sync/errgroup"
)

// StatCache remembers, per path, the stat data seen when a file was last
// read and the File ref it produced. It only ever adds a reason to re-read;
// a hit alone never justifies reuse.
type StatCache interface {
	// Lookup reports whether an entry exists for path and whether it still
	// matches the file's identity.
	Lookup(path string, info os.FileInfo) (present bool, matches bool)
	// Store records the file's identity and the ref its read produced.
	Store(path string, info os.FileInfo, ref *proto.Ref) error
}

// TreeFetcher is implemented by stores that can stream a subtree in one
// round trip.
type TreeFetcher interface {
	// GetTree returns the tree at ref and the trees below it, maxDepth
	// levels down.
	GetTree(ctx context.Context, ref *proto.Ref, maxDepth uint32) ([]*proto.Object, error)
}

// Keeper is implemented by object stores that keep copies of what they
// serve. Keep hands one an object written around it, such as the commit a
// walker writes through the index.
type Keeper interface {
	Keep(ctx context.Context, obj *proto.Object)
}

// Walker builds a commit by diffing a directory against the set's latest
// commit, reading and uploading only what changed.
type Walker struct {
	Index   Index
	Objects ObjectStore
	Set     string
	AgentID string
	// Root is the directory backed up, a path of FS.
	Root string

	// FS is the tree Root is read from; nil means LocalFS. A run on any
	// other FS takes no Growing files.
	FS FS

	// Metadata is recorded on the commit; the store keeps it and never
	// interprets it.
	Metadata map[string]string

	// Stream, when set, is backed up instead of Root. A stream cannot be
	// read again, so a run with one does not retry a lost session.
	Stream *Stream

	// Carry, when set, keeps every entry of the previous commit's root that
	// is gone from the disk and for which it returns true, so a set can
	// accumulate files that leave the disk once committed. A run with Carry
	// fails with ErrSealChanged over a commit sealed under another key or
	// none.
	Carry func(name string) bool

	// Subdir, when set, puts Root's entries in the directory of that name
	// in the commit's root instead of in the root itself. Carry then applies
	// inside it, and to the previous commit's other root entries. A run with
	// Subdir writes no checkpoints.
	Subdir string

	// Include decides per slash-separated relative path, asked once per
	// entry the walk lists; nil includes all.
	Include func(rel string) bool

	// Unchanged, when set, reports directories known to be as in the
	// previous commit, everything under them included, such as from a
	// change journal read since that commit's scan began. The walk takes
	// such a directory from the previous commit without reading it, so the
	// stat cache and ForceHashPercent don't reach inside. Include must be
	// the previous run's.
	Unchanged func(rel string) bool

	// Growing are files of the root, or of Subdir when it is set, committed
	// up to a length, continuing the previous commit's versions; a run with
	// Stream takes none.
	Growing []Growing

	// Key seals names and blobs; nil backs up in the clear.
	Key *storekey.Key

	// Logger is where the walk reports; nil means slog.Default.
	Logger *slog.Logger

	// Progress, when set, is handed the counters every ProgressInterval
	// while the walk runs, and once more when it ends. It is called from
	// a goroutine of its own and must not block for long.
	Progress func(WalkResult)
	// ProgressInterval is how often Progress hears; zero means
	// DefaultProgressInterval.
	ProgressInterval time.Duration

	// Sessions, when set, wraps the run in a session, so nothing it uploads
	// is visible to others before its commit.
	Sessions SessionStore

	// InitialRetention, when set, is the retention policy Set starts with
	// if this run creates it; it travels with the session, so a run
	// without Sessions sends none.
	InitialRetention *retention.Policy

	// Verify reads every file, even one the change test would reuse, which
	// catches a write that kept a file's mtime and size.
	Verify bool

	// DownloadPlaceholders reads files a cloud sync client such as OneDrive
	// keeps only in the cloud, which downloads them. Without it they are
	// recorded with their stat and security descriptor and no content.
	DownloadPlaceholders bool

	Workers int
	// ForceHashPercent is the share of reused files re-hashed as a check;
	// ignored on an FS whose Traits do not allow a Reread.
	ForceHashPercent   int
	CheckpointInterval time.Duration
	ReadRetries        int

	// LostRetries is how often a commit refused with ErrSessionLost, or
	// missing what a collection took, is retried by walking again in a new
	// session.
	LostRetries int

	// ScanWorkers is how many directories are listed ahead of the walk at
	// once; zero means DefaultScanWorkers and a negative number lists every
	// directory on the walk itself.
	ScanWorkers int
	// ScanWindow bounds the entries held for directories the walk has not
	// reached yet; zero means DefaultScanWindow.
	ScanWindow int

	// Quiesced records that a pre hook paused the application before the
	// walk; a clean run then commits as consistent.
	Quiesced bool
	// Cache is consulted only on an FS whose Traits carry Identity.
	Cache         StatCache
	PrefetchDepth uint32

	// WindowBytes bounds the chunk bytes kept for files whose skipped parts
	// the store has not confirmed yet; 0 means DefaultWindowBytes.
	WindowBytes int

	// BlobCache, when set, receives every uploaded blob.
	BlobCache *blobcache.Cache

	filters   presence.Set
	confirmer Confirmer
	// rescan and damaged come from the commit grant: what the store lost
	// and needs read again rather than assumed unchanged
	rescan    bool
	damaged   map[string]bool
	window    *chunkWindow
	gate      *syncutil.Gate
	scan      *scanner
	foundAll  atomic.Bool
	traits    Traits
	trees     *treeSource
	scanStart int64
	// policyVersion is what the commit records, kept apart from Key
	// because a policy without encryption drops the key for the run
	policyVersion uint32
	// every is the grant's Every
	every    time.Duration
	base     *proto.Ref
	baseScan int64
	started  time.Time
	last     time.Time
	rand     *rand.Rand
	randMtx  sync.Mutex
	// dirSlots bounds the directories walked beside the ones the walk
	// descends into itself
	dirSlots chan struct{}
	result   WalkResult
	sent     *sentSet
	// descriptors is where this run stored each security descriptor
	descriptors *descriptors
	// emptyFile stores the empty file once a run, for placeholders
	emptyFile func() (*proto.Ref, error)
	// local is the root a walk of the local disk lists and opens what it
	// reads in, where the platform has one
	local *os.Root
	// outside is the run's context outside any session, and session the
	// context of the session it writes in now, which a checkpoint renews
	outside, session context.Context
	// clock stands in for time.Now in tests; the Windows monotonic clock
	// ticks too coarsely for a short run to see any time pass
	clock func() time.Time
}

func (w *Walker) now() time.Time {
	if w.clock != nil {
		return w.clock()
	}

	return time.Now()
}

func (w *Walker) fs() FS {
	if w.FS != nil {
		return w.FS
	}

	return LocalFS{}
}

// WalkResult summarises one run.
type WalkResult struct {
	Ref    *proto.Ref
	Commit *proto.Commit
	Base   *proto.Ref
	// SameTree reports a run that found the latest commit's tree and wrote
	// no commit; Ref and Commit are that commit.
	SameTree bool
	Files    int64
	// Bytes is what those files hold on disk, whether or not the walk
	// had to read them; Uploaded is the chunk bytes actually sent.
	Bytes      int64
	Uploaded   int64
	Reused     int64
	Read       int64
	Torn       int64
	Unreadable int64
	Skipped    int64
	// Placeholders counts the files recorded without content because it
	// was only in the cloud; Files and Bytes leave them out.
	Placeholders int64
	// Excluded counts the entries Include rejected, an excluded directory
	// as one; ExcludedBytes is the listed size of the excluded files.
	// Directories taken whole through Unchanged aren't counted.
	Excluded      int64
	ExcludedBytes int64
	Checkpoints   int64
	// Assumed counts parts skipped because a filter or the previous version
	// held them; Repaired counts those the store then turned out to lack.
	Assumed  int64
	Repaired int64
	// Found and FoundBytes count the files, and their bytes, that a walk
	// with no base has listed ahead of reading them; FoundAll reports it
	// has listed the whole tree, so they are what the walk covers.
	Found      int64
	FoundBytes int64
	FoundAll   bool
}

// Dirty reports whether any file was recorded from an inconsistent or failed
// read, which a caller should surface as a non-zero exit.
func (r *WalkResult) Dirty() bool {
	return r.Torn > 0 || r.Unreadable > 0
}

var (
	errUnreadable = errors.New("file could not be read")
	// errVanished is an entry gone since it was listed, which a walk leaves
	// out of its commit
	errVanished = errors.New("the entry is gone")
)

// DefaultProgressInterval is how often a walk reports when the caller
// wants progress but names no interval.
const DefaultProgressInterval = 15 * time.Second

// snapshot is what the counters say right now. The refs a finished run
// reports are left out, because they are only written once it is over.
func (w *Walker) snapshot() WalkResult {
	return WalkResult{
		Files:         atomic.LoadInt64(&w.result.Files),
		Bytes:         atomic.LoadInt64(&w.result.Bytes),
		Uploaded:      atomic.LoadInt64(&w.result.Uploaded),
		Reused:        atomic.LoadInt64(&w.result.Reused),
		Read:          atomic.LoadInt64(&w.result.Read),
		Torn:          atomic.LoadInt64(&w.result.Torn),
		Unreadable:    atomic.LoadInt64(&w.result.Unreadable),
		Skipped:       atomic.LoadInt64(&w.result.Skipped),
		Placeholders:  atomic.LoadInt64(&w.result.Placeholders),
		Excluded:      atomic.LoadInt64(&w.result.Excluded),
		ExcludedBytes: atomic.LoadInt64(&w.result.ExcludedBytes),
		Checkpoints:   atomic.LoadInt64(&w.result.Checkpoints),
		Assumed:       atomic.LoadInt64(&w.result.Assumed),
		Repaired:      atomic.LoadInt64(&w.result.Repaired),
		Found:         atomic.LoadInt64(&w.result.Found),
		FoundBytes:    atomic.LoadInt64(&w.result.FoundBytes),
		FoundAll:      w.foundAll.Load(),
	}
}

// reportProgress calls Progress until the returned stop does, which the
// caller runs before it reads the counters itself. Stopping twice is
// harmless.
func (w *Walker) reportProgress() func() {
	if w.Progress == nil {
		return func() {}
	}

	interval := w.ProgressInterval
	if interval <= 0 {
		interval = DefaultProgressInterval
	}

	var (
		done = make(chan struct{})
		gone = make(chan struct{})
		once sync.Once
	)

	go func() {
		defer close(gone)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				w.Progress(w.snapshot())
				return
			case <-ticker.C:
				w.Progress(w.snapshot())
			}
		}
	}()

	return func() {
		once.Do(func() { close(done) })
		<-gone
	}
}

// PreviousVersions is how many live versions of a changed file the walker
// diffs its chunks against.
const PreviousVersions = 3

func (w *Walker) logger() *slog.Logger {
	if w.Logger != nil {
		return w.Logger
	}

	return slog.Default()
}

// Run walks the root, writes the commit and returns its ref.
func (w *Walker) Run(ctx context.Context) (*WalkResult, error) {
	if len(w.Growing) > 0 && !isLocal(w.FS) {
		return nil, errors.New("a run on a file system other than the local disk takes no growing files")
	}

	if w.InitialRetention != nil {
		if err := w.InitialRetention.Validate(); err != nil {
			return nil, fmt.Errorf("initial retention: %w", err)
		}
	}

	if w.Workers <= 0 {
		w.Workers = 1
	}

	w.gate = syncutil.NewGate(w.Workers)
	if w.dirSlots == nil {
		w.dirSlots = make(chan struct{}, dirWalkers)
	}
	w.rand = rand.New(rand.NewSource(time.Now().UnixNano()))

	w.outside, w.session = ctx, ctx
	if w.Sessions != nil {
		sctx, err := w.beginSession(ctx)
		if err != nil {
			return nil, err
		}

		w.session = sctx
		ctx = sctx

		defer func() { w.endSession(w.session) }()
	}

	// inside the session, so a set the gate creates starts with the
	// session's initial retention
	if gate, ok := w.Index.(CommitGate); ok {
		grant, err := gate.BeginCommit(ctx, w.Set)
		if err != nil {
			return nil, fmt.Errorf("beginning commit: %w", err)
		}

		err = w.adoptPolicy(grant.Policy)
		if err != nil {
			return nil, err
		}

		w.adoptDamage(grant)
		w.every = grant.Every
	}

	w.policyVersion = 0
	if w.Key != nil {
		w.policyVersion = w.Key.Policy.Version
	}

	for retry := 0; ; retry++ {
		result, err := w.walk(w.session)

		var missing *MissingError
		lost := errors.Is(err, ErrSessionLost) || errors.As(err, &missing)

		if !lost || retry >= w.LostRetries || w.Stream != nil {
			return result, err
		}

		w.logger().Warn("the store lost objects of this run, walking again in a new session", "err", err, "retry", retry+1)

		// a store may end the session that lost objects, and it takes the
		// session's uploads with it
		if w.Sessions != nil {
			sctx, err := w.beginSession(w.outside)
			if err != nil {
				return nil, err
			}

			w.endSession(w.session)
			w.session = sctx
		}
	}
}

func (w *Walker) beginSession(ctx context.Context) (context.Context, error) {
	sctx, err := w.Sessions.BeginSession(ctx, &Session{AgentID: w.AgentID, Set: w.Set, InitialRetention: w.InitialRetention})
	if err != nil {
		return nil, fmt.Errorf("beginning session: %w", err)
	}

	return sctx, nil
}

// endSessionWait bounds how long ending a session may take once the run's
// context is done.
const endSessionWait = 10 * time.Second

// endSession ends the session on ctx unless a commit or the store already
// ended it, also when the run was cancelled: a store keeps a session left
// open until its lease runs out.
func (w *Walker) endSession(ctx context.Context) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), endSessionWait)
	defer cancel()

	if err := w.Sessions.EndSession(ctx); err != nil && !errors.Is(err, ErrNoSession) {
		w.logger().Warn("ending the session failed", "err", err)
	}
}

// walk is one attempt at the commit within the run's session.
func (w *Walker) walk(ctx context.Context) (*WalkResult, error) {
	w.started = w.now()
	w.last = w.started
	w.scanStart = w.started.UnixNano()
	w.result = WalkResult{}
	w.foundAll.Store(false)
	w.sent = newSentSet()
	w.descriptors = &descriptors{}
	w.emptyFile = sync.OnceValues(func() (*proto.Ref, error) {
		return w.putContent(w.session, 0, bytes.NewReader(nil))
	})
	w.traits = w.fs().Traits()

	workers := w.ScanWorkers
	if workers == 0 {
		workers = DefaultScanWorkers
	}

	// a root that cannot be opened is listed by path, which reports why
	w.local = nil
	if isLocal(w.fs()) && w.Stream == nil {
		if root, err := openLocalRoot(w.Root); err == nil && root != nil {
			w.local = root
			defer root.Close()
		}
	}

	w.scan = newScanner(ctx, w, max(workers, 0), w.ScanWindow)
	defer w.scan.close()

	stopProgress := w.reportProgress()
	defer stopProgress()

	w.loadPresence(ctx)

	fetcher, _ := w.Objects.(TreeFetcher)
	w.trees = &treeSource{getter: w.Objects, fetcher: fetcher, key: w.Key, depth: w.PrefetchDepth, objects: map[string]*proto.Object{}}

	baseNodes, err := w.loadBase(ctx)
	if err != nil {
		return nil, err
	}

	if baseNodes == nil && w.Stream == nil && w.Subdir == "" {
		w.scan.countAll()
	}

	var (
		nodes   []*proto.TreeNode
		changed = true
	)

	if w.Stream != nil {
		nodes, err = w.putStream(ctx)
		if err != nil {
			return nil, err
		}
	} else if w.Subdir != "" {
		nodes, changed, err = w.walkSubdir(ctx, baseNodes)
		if err != nil {
			return nil, err
		}
	} else {
		nodes, changed, err = w.walkDir(ctx, w.Root, "", nil, nil, baseNodes, true)
		if err != nil {
			return nil, err
		}

		ctx = w.session
	}

	if len(w.Growing) > 0 && w.Subdir == "" {
		if w.Stream != nil {
			return nil, errors.New("a run with a stream takes no growing files")
		}

		var grown bool
		if nodes, grown, err = w.putGrowing(ctx, nodes, baseNodes); err != nil {
			return nil, err
		}

		changed = changed || grown || len(nodes) != len(baseNodes)
	}

	if !changed && w.recent(w.result.Commit) {
		stopProgress()

		w.result.Ref = w.base
		w.result.Base = w.base
		w.result.SameTree = true

		result := w.result
		result.FoundAll = w.foundAll.Load()

		return &result, nil
	}

	var root *proto.Ref
	if !changed && w.base != nil {
		root = w.result.Commit.Tree
	} else {
		root, err = PutTree(ctx, w.Objects, nodes, w.Key, nil)
		if err != nil {
			return nil, fmt.Errorf("storing root tree: %w", err)
		}
	}

	ref, commit, err := w.putCommit(ctx, root, false)
	if err != nil {
		return nil, err
	}

	// the counters are read plainly from here on, so nobody else may be
	// looking at them
	stopProgress()

	w.result.Ref = ref
	w.result.Commit = commit
	w.result.Base = w.base

	result := w.result
	result.FoundAll = w.foundAll.Load()

	return &result, nil
}

// SameTreeFor is how long a run that finds the latest commit's tree takes
// that commit as its own instead of writing another.
const SameTreeFor = 12 * time.Hour

// recent reports whether base, the latest commit, stands for a run that
// found its tree: a complete commit made in the same way, younger than
// SameTreeFor and than half the grant's Every, by a run the store didn't
// ask to read lost content again.
func (w *Walker) recent(base *proto.Commit) bool {
	if w.base == nil || base == nil || base.Partial || w.Stream != nil || w.Subdir != "" || w.rescan || w.damaged != nil {
		return false
	}

	if base.PolicyVersion != w.policyVersion || !maps.Equal(base.Metadata, w.Metadata) {
		return false
	}

	consistent := w.Quiesced && atomic.LoadInt64(&w.result.Torn) == 0 && atomic.LoadInt64(&w.result.Unreadable) == 0
	if consistent && !base.Consistent {
		return false
	}

	within := SameTreeFor
	if w.every > 0 {
		within = min(within, w.every/2)
	}

	made := time.Unix(0, base.ReceivedAtNs)
	if base.ReceivedAtNs == 0 {
		made = time.Unix(base.Timestamp, 0)
	}

	return w.now().Sub(made) < within
}

// adoptPolicy writes this run under the store's policy instead of the key
// file's; a policy the operator never set (version 0) leaves the key file
// in charge. The policy can refuse a keyless run but never unseal a keyed
// one.
func (w *Walker) adoptPolicy(policy *storekey.Policy) error {
	if policy == nil || policy.Version == 0 {
		return nil
	}

	if w.Key == nil {
		if policy.Mode == storekey.ModeNone {
			return nil
		}

		return fmt.Errorf("store policy v%d writes %s but no store key is configured", policy.Version, policy.Mode)
	}

	if w.Key.Policy.Version != policy.Version {
		w.logger().Info("store policy from the server replaces the key file's", "server", policy.Version, "keyfile", w.Key.Policy.Version)
	}

	w.Key.Policy = *policy

	return nil
}

// loadPresence fetches the filters of the run's scope when the store can
// also confirm what they say.
func (w *Walker) loadPresence(ctx context.Context) {
	inner := unwrapStore(w.Objects)

	w.confirmer, _ = inner.(Confirmer)
	w.filters = nil

	budget := w.WindowBytes
	if budget <= 0 {
		budget = DefaultWindowBytes
	}
	w.window = newChunkWindow(budget)

	source, ok := inner.(PresenceSource)
	if !ok || w.confirmer == nil {
		return
	}

	filters, err := source.Presence(ctx, w.Set)
	if err != nil {
		w.logger().Warn("fetching presence filters failed, uploading every new chunk", "err", err)
		return
	}

	w.filters = filters
	if len(filters) > 0 {
		w.logger().Info("presence filters loaded", "filters", len(filters), "refs", filters.Entries(), "bytes", filters.Size())
	}
}

// unwrapStore peels caching wrappers off a store.
func unwrapStore(store ObjectStore) ObjectStore {
	for {
		wrapper, ok := store.(interface{ Unwrap() ObjectStore })
		if !ok {
			return store
		}

		store = wrapper.Unwrap()
	}
}

// loadBase resolves the diff base from the server, never from local state.
func (w *Walker) loadBase(ctx context.Context) ([]*proto.TreeNode, error) {
	ref, err := w.Index.LatestCommit(ctx, w.Set)
	if errors.Is(err, ErrNotFound) {
		w.logger().Info("no previous commit, reading everything", "set", w.Set)
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("finding the latest commit: %w", err)
	}

	obj, err := w.Objects.Get(ctx, ref)
	if err != nil {
		return nil, fmt.Errorf("loading base commit %x: %w", ref.Hash, err)
	}

	commit := obj.GetCommit()
	if commit == nil {
		return nil, fmt.Errorf("base %x is not a commit", ref.Hash)
	}

	tree, err := w.trees.load(ctx, commit.Tree, nil)
	if errors.Is(err, storekey.ErrWrongKey) || commit.KeyId != nil && (len(commit.KeyId) > 0) != (w.Key != nil) {
		if w.Carry != nil {
			return nil, ErrSealChanged
		}

		w.logger().Info("the previous commit was sealed under another key or none, reading everything", "set", w.Set)
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("loading base tree %x: %w", commit.Tree.Hash, err)
	}

	w.base = ref
	w.result.Commit = commit
	w.baseScan = commit.ScanStartNs
	if w.baseScan == 0 {
		w.baseScan = commit.Timestamp * int64(time.Second)
	}

	w.logger().Info("diffing against the previous commit", "ref", fmt.Sprintf("%x", ref.Hash), "from", time.Unix(commit.Timestamp, 0))

	return tree.Nodes, nil
}

func (w *Walker) putCommit(ctx context.Context, tree *proto.Ref, partial bool) (*proto.Ref, *proto.Commit, error) {
	commit := &proto.Commit{
		Timestamp:   time.Now().Unix(),
		Tree:        tree,
		BackupSet:   w.Set,
		Parent:      w.base,
		AgentId:     w.AgentID,
		Metadata:    w.Metadata,
		ScanStartNs: w.scanStart,
		Partial:     partial,
		Consistent:  !partial && w.Quiesced && atomic.LoadInt64(&w.result.Torn) == 0 && atomic.LoadInt64(&w.result.Unreadable) == 0,
	}

	commit.PolicyVersion = w.policyVersion
	commit.KeyId = []byte{}
	if w.Key != nil {
		commit.KeyId = w.Key.ID()
	}

	// a checkpoint carries base nodes this run has not verified, so it
	// keeps the base run's racy window
	if partial && w.base != nil && w.baseScan < commit.ScanStartNs {
		commit.ScanStartNs = w.baseScan
	}

	obj := proto.NewObject(commit)

	err := w.Index.Put(ctx, obj)
	if err != nil {
		return nil, nil, fmt.Errorf("storing commit: %w", err)
	}

	if keeper, ok := w.Objects.(Keeper); ok {
		keeper.Keep(ctx, obj)
	}

	return obj.Ref(), commit, nil
}

func (w *Walker) included(rel string) bool {
	return w.Include == nil || w.Include(rel)
}

// walkDir compares one directory with its base nodes and returns the new
// node list and whether it differs from the base. listed is the stat its
// parent's listing took of it, nil for the root; parent is the token of the
// directory, nil for the root and in a plaintext store.
func (w *Walker) walkDir(ctx context.Context, dir, rel string, listed os.FileInfo, parent []byte, base []*proto.TreeNode, root bool) ([]*proto.TreeNode, bool, error) {
	found := w.scan.take(scanJob{dir: dir, rel: rel})
	if err := found.err; err != nil {
		if !root && vanished(err) {
			return nil, false, fmt.Errorf("%w: listing %s: %v", errVanished, dir, err)
		}

		if base != nil && entryError(err) {
			w.logger().Warn("listing failed, keeping the previous version", "dir", dir, "err", err)
			atomic.AddInt64(&w.result.Unreadable, 1)
			return base, false, nil
		}

		if entryError(err) && !root {
			w.logger().Warn("listing failed, skipping", "dir", dir, "err", err)
			atomic.AddInt64(&w.result.Unreadable, 1)
			return nil, true, nil
		}

		return nil, false, fmt.Errorf("listing %s: %w", dir, err)
	}

	if listed != nil && found.self != nil && !os.SameFile(listed, found.self) {
		return nil, false, fmt.Errorf("%w: %s is no longer the directory listed", errVanished, dir)
	}

	entries := found.entries
	fsys := w.fs()

	baseByName := make(map[string]*proto.TreeNode, len(base))
	for _, node := range base {
		baseByName[string(node.Stat.Name)] = node
	}

	results := make([]*proto.TreeNode, len(entries))
	changed := false
	group, groupCtx, stop := newGroup(ctx)
	// whatever the loop leaves running when it returns early is stopped
	// before walkDir returns
	defer func() { stop() }()
	var mtx sync.Mutex

	markChanged := func() {
		mtx.Lock()
		changed = true
		mtx.Unlock()
	}

	for i, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}

		name := entry.name
		childRel := path.Join(rel, name)
		childPath := fsys.Join(dir, name)
		baseNode := baseByName[name]

		if entry.excluded {
			atomic.AddInt64(&w.result.Excluded, 1)
			atomic.AddInt64(&w.result.ExcludedBytes, entry.excludedSize)
			if baseNode != nil {
				markChanged()
			}
			continue
		}

		if entry.err != nil {
			if vanished(entry.err) {
				if baseNode != nil {
					markChanged()
				}
				continue
			}

			w.logger().Warn("stat failed, keeping the previous version", "path", childPath, "err", entry.err)
			atomic.AddInt64(&w.result.Unreadable, 1)
			results[i] = baseNode
			continue
		}

		info := entry.info

		switch {
		case info.IsDir() && w.reusesUnchanged(childRel, baseNode):
			results[i] = baseNode
		case info.IsDir():
			index := i
			token := NameToken(w.Key, parent, []byte(name))

			walk := func(ctx context.Context) error {
				node, childChanged, err := w.walkChildDir(ctx, childPath, childRel, token, info, baseNode)
				if errors.Is(err, errVanished) {
					w.logger().Debug("the directory is gone, leaving it out", "dir", childPath)
					markChanged()
					return nil
				}

				if err != nil {
					return err
				}

				results[index] = node
				if childChanged {
					markChanged()
				}

				return nil
			}

			// a directory without a free slot is walked here, so a walk
			// never waits on a slot its own ancestors hold
			select {
			case w.dirSlots <- struct{}{}:
				dirCtx := groupCtx
				group.Go(func() error {
					defer func() { <-w.dirSlots }()

					return walk(dirCtx)
				})
			default:
				if err := walk(groupCtx); err != nil {
					// a sibling's failure, which cancelled this walk, goes first
					group.Go(func() error { return err })
					return nil, false, group.Wait()
				}
			}

		case info.Mode()&os.ModeSymlink != 0:
			target, err := fsys.Readlink(ctx, childPath)
			if err != nil {
				if !vanished(err) {
					w.logger().Warn("reading symlink failed, keeping the previous version", "path", childPath, "err", err)
					atomic.AddInt64(&w.result.Unreadable, 1)

					if baseNode.GetStat().GetType() == proto.NodeType_NODE_SYMLINK {
						results[i] = baseNode
						continue
					}
				}

				if baseNode != nil {
					markChanged()
				}
				continue
			}

			node := &proto.TreeNode{Stat: w.stat(info, target)}
			results[i] = node
			if !nodeEqual(node, baseNode) {
				markChanged()
			}

		case info.Mode().IsRegular():
			stat := w.stat(info, "")

			if cloudOnly(stat) && !w.DownloadPlaceholders {
				node, err := w.placeholder(ctx, childPath, childRel, stat, baseNode)
				if err != nil {
					return nil, false, err
				}

				results[i] = node
				if !nodeEqual(node, baseNode) {
					markChanged()
				}

				continue
			}

			atomic.AddInt64(&w.result.Files, 1)
			atomic.AddInt64(&w.result.Bytes, info.Size())
			index := i

			if w.reusable(ctx, childPath, childRel, found.self, info, stat, baseNode) {
				atomic.AddInt64(&w.result.Reused, 1)
				if err := w.metadata(ctx, childPath, childRel, stat, baseNode); err != nil {
					return nil, false, err
				}

				node := &proto.TreeNode{Stat: stat, Ref: baseNode.Ref}
				results[index] = node
				if !nodeEqual(node, baseNode) {
					markChanged()
				}
				continue
			}

			w.gate.Start()
			fileCtx := groupCtx
			group.Go(func() error {
				defer w.gate.Done()

				node, err := w.backupFile(fileCtx, childPath, childRel, found.self, info, baseNode)
				if err != nil {
					return err
				}

				mtx.Lock()
				results[index] = node
				mtx.Unlock()

				if !nodeEqual(node, baseNode) {
					markChanged()
				}

				return nil
			})

		default:
			w.logger().Info("skipping irregular file", "path", childPath, "mode", info.Mode().Type())
			atomic.AddInt64(&w.result.Skipped, 1)
			if baseNode != nil {
				markChanged()
			}
		}

		if root && w.Subdir == "" && w.checkpointDue() {
			// drain the files in flight so every finished entry has a node;
			// Wait cancels the group's context, so start a fresh group after
			err := group.Wait()
			if err != nil {
				return nil, false, err
			}

			err = w.checkpoint(ctx, results[:i+1], base, []byte(name))
			if err != nil {
				return nil, false, err
			}

			ctx = w.session
			group, groupCtx, stop = newGroup(ctx)
		}
	}

	err := group.Wait()
	if err != nil {
		return nil, false, err
	}

	nodes := make([]*proto.TreeNode, 0, len(results))
	for _, node := range results {
		if node != nil {
			nodes = append(nodes, node)
		}
	}

	if root && w.Carry != nil {
		listed := make(map[string]bool, len(entries))
		for _, entry := range entries {
			listed[entry.name] = true
		}

		for _, node := range base {
			if name := string(node.Stat.Name); !listed[name] && w.Carry(name) {
				nodes = append(nodes, node)
			}
		}
	}

	if len(nodes) != len(base) {
		changed = true
	}

	return SortNodes(nodes), changed, nil
}

// newGroup is an errgroup under ctx and a stop that cancels what it runs
// and waits for it.
func newGroup(ctx context.Context) (*errgroup.Group, context.Context, func()) {
	ctx, cancel := context.WithCancel(ctx)
	group, groupCtx := errgroup.WithContext(ctx)

	return group, groupCtx, func() {
		cancel()
		_ = group.Wait()
	}
}

// walkSubdir walks Root as the directory Subdir of the root, whose other
// entries are the previous commit's that Carry keeps.
func (w *Walker) walkSubdir(ctx context.Context, base []*proto.TreeNode) ([]*proto.TreeNode, bool, error) {
	info, err := w.fs().Stat(ctx, w.Root)
	if err != nil {
		return nil, false, err
	}

	var (
		nodes   []*proto.TreeNode
		prev    *proto.TreeNode
		changed bool
	)

	for _, node := range base {
		name := string(node.Stat.Name)

		switch {
		case name == w.Subdir:
			prev = node
		case w.Carry != nil && w.Carry(name):
			nodes = append(nodes, node)
		default:
			changed = true
		}
	}

	token := NameToken(w.Key, nil, []byte(w.Subdir))

	var inner []*proto.TreeNode
	if prev != nil && prev.Stat.IsDir() && prev.Ref != nil {
		tree, err := w.trees.load(ctx, prev.Ref, token)
		if err != nil {
			return nil, false, fmt.Errorf("loading the base tree of %s: %w", w.Subdir, err)
		}

		inner = tree.Nodes
	}

	children, innerChanged, err := w.walkDir(ctx, w.Root, "", nil, token, inner, true)
	if err != nil {
		return nil, false, err
	}

	if len(w.Growing) > 0 {
		var grown bool
		if children, grown, err = w.putGrowing(ctx, children, inner); err != nil {
			return nil, false, err
		}

		innerChanged = innerChanged || grown || len(children) != len(inner)
	}

	stat := w.stat(info, "")
	stat.Name = []byte(w.Subdir)
	if err := w.metadata(ctx, w.Root, "", stat, prev); err != nil {
		return nil, false, err
	}

	node := &proto.TreeNode{Stat: stat}
	if !innerChanged && inner != nil {
		node.Ref = prev.Ref
	} else if node.Ref, err = PutTree(ctx, w.Objects, children, w.Key, token); err != nil {
		return nil, false, fmt.Errorf("storing tree for %s: %w", w.Subdir, err)
	}

	if !nodeEqual(node, prev) {
		changed = true
	}

	return SortNodes(append(nodes, node)), changed, nil
}

func (w *Walker) walkChildDir(ctx context.Context, dir, rel string, token []byte, info os.FileInfo, baseNode *proto.TreeNode) (*proto.TreeNode, bool, error) {
	var baseChildren []*proto.TreeNode
	if baseNode != nil && baseNode.Stat.IsDir() && baseNode.Ref != nil {
		tree, err := w.trees.load(ctx, baseNode.Ref, token)
		if err != nil {
			w.logger().Warn("base tree unavailable, reading the directory in full", "tree", fmt.Sprintf("%x", baseNode.Ref.Hash), "dir", dir, "err", err)
		} else {
			baseChildren = tree.Nodes
		}
	}

	children, childChanged, err := w.walkDir(ctx, dir, rel, info, token, baseChildren, false)
	if err != nil {
		return nil, false, err
	}

	stat := w.stat(info, "")
	if err := w.metadata(ctx, dir, rel, stat, baseNode); err != nil {
		return nil, false, err
	}

	if !childChanged && baseChildren != nil {
		node := &proto.TreeNode{Stat: stat, Ref: baseNode.Ref}
		return node, !nodeEqual(node, baseNode), nil
	}

	ref, err := PutTree(ctx, w.Objects, children, w.Key, token)
	if err != nil {
		return nil, false, fmt.Errorf("storing tree for %s: %w", dir, err)
	}

	return &proto.TreeNode{Stat: stat, Ref: ref}, true, nil
}

// dirWalkers is how many directories a walk reads at once besides the
// ones it descends into itself.
const dirWalkers = 8

// sample reports whether a file is picked for a re-hash.
func (w *Walker) sample() bool {
	w.randMtx.Lock()
	defer w.randMtx.Unlock()

	return w.rand.Intn(100) < w.ForceHashPercent
}

// reusable applies the change test: same metadata as the base node, older
// than the base run, not contradicted by the stat cache, and not picked for
// a sampled re-hash. parent is the stat of the directory listed, nil when
// the FS tells none.
func (w *Walker) reusable(ctx context.Context, path, rel string, parent, info os.FileInfo, stat *proto.FileInfo, baseNode *proto.TreeNode) bool {
	if w.Verify || baseNode == nil || baseNode.Stat.GetType() != proto.NodeType_NODE_FILE || baseNode.Ref == nil {
		return false
	}

	if w.lost(rel) {
		return false
	}

	if !metaEqual(baseNode.Stat, stat) || cloudOnly(baseNode.Stat) != cloudOnly(stat) {
		return false
	}

	// the racy rule: a file touched during or after the base run may have
	// been rewritten within the same timestamp tick, which on a coarse or
	// skewed FS reaches back by its resolution and skew
	if stat.MtimeNs+int64(w.traits.Resolution+w.traits.Skew) >= w.baseScan {
		return false
	}

	if w.Cache != nil && w.traits.Identity {
		if present, matches := w.Cache.Lookup(path, info); present && !matches {
			return false
		}
	}

	if w.ForceHashPercent > 0 && w.traits.Reread && w.sample() {
		ref, err := w.hashListed(ctx, path, rel, parent)
		if err != nil || !ref.Equal(baseNode.Ref) {
			w.logger().Info("sampled re-hash differs from the recorded version, reading the file", "path", path)
			return false
		}
	}

	return true
}

// backupFile reads a file into blobs, retrying when the file changes under
// the read, and falls back to the base node when it cannot be read. parent
// is the stat of the directory listed, nil when the FS tells none.
func (w *Walker) backupFile(ctx context.Context, path, rel string, parent, info os.FileInfo, baseNode *proto.TreeNode) (*proto.TreeNode, error) {
	known := w.knownParts(ctx, rel, baseNode)
	forced := map[string]struct{}{}

	// nothing the store already holds may answer for a path it lost
	if w.lost(rel) {
		known, forced = nil, nil
	}

	var ref *proto.Ref
	var err error

	for attempt := 0; ; attempt++ {
		var read os.FileInfo
		ref, read, err = w.readFile(ctx, path, rel, parent, info, known, forced)

		var changed *fileChangedError
		if errors.As(err, &changed) {
			w.logger().Info("file changed while its parts were confirmed, reading it again", "path", path)

			for _, ref := range changed.refs {
				forced[string(ref.Hash)] = struct{}{}
			}

			if attempt >= w.ReadRetries {
				// upload everything on the next read; it cannot fail this way
				known = nil
				forced = nil
			}

			continue
		}

		if err != nil {
			break
		}

		info = read

		// a file replaced by anything else after the read keeps the read
		after, statErr := w.fs().Lstat(ctx, path)
		if statErr != nil || !after.Mode().IsRegular() || (after.Size() == info.Size() && after.ModTime().Equal(info.ModTime())) {
			break
		}

		info = after
		if attempt >= w.ReadRetries {
			w.logger().Warn("file kept changing while it was read, recording the last read", "path", path)
			atomic.AddInt64(&w.result.Torn, 1)
			break
		}
	}

	if errors.Is(err, errVanished) {
		w.logger().Debug("the file is gone, leaving it out", "path", path)
		return nil, nil
	}

	if errors.Is(err, errUnreadable) {
		atomic.AddInt64(&w.result.Unreadable, 1)

		if baseNode != nil && baseNode.Stat.GetType() == proto.NodeType_NODE_FILE {
			w.logger().Warn("reading failed, keeping the previous version", "path", path)
			return baseNode, nil
		}

		w.logger().Warn("reading failed and no previous version exists, skipping", "path", path)
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	atomic.AddInt64(&w.result.Read, 1)

	if w.Cache != nil && w.traits.Identity {
		if cErr := w.Cache.Store(path, info, ref); cErr != nil {
			w.logger().Warn("updating the stat cache failed", "path", path, "err", cErr)
		}
	}

	stat := w.stat(info, "")
	if err := w.metadata(ctx, path, rel, stat, baseNode); err != nil {
		return nil, err
	}

	return &proto.TreeNode{Stat: stat, Ref: ref}, nil
}

// readFile chunks and stores the file listed as listed, returning its ref
// and the stat of what it read: listed, or the file that has taken its
// place in the directory since. forced refs are uploaded even when known or
// the filters say the store has them; a nil forced map disables the filters
// and known altogether.
func (w *Walker) readFile(ctx context.Context, path, rel string, parent, listed os.FileInfo, known map[string]struct{}, forced map[string]struct{}) (*proto.Ref, os.FileInfo, error) {
	file, info, err := w.open(ctx, path, rel, parent)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	current := listed
	if parent != nil && !os.SameFile(listed, info) {
		current = info
	}

	writer := newFileWriter(ctx, w.Objects, w.Key, info.Size())
	writer.sent = w.sent
	writer.source, _ = file.(io.ReaderAt)
	writer.cache = w.BlobCache
	writer.uploaded = &w.result.Uploaded

	if forced != nil {
		writer.known = known
		writer.forced = forced
		writer.filters = w.filters
		writer.confirmer = w.confirmer
		writer.window = w.window
	}

	read := &readErr{r: contentReader(file, info)}
	_, err = io.Copy(writer, read)
	if err != nil {
		if read.err != nil {
			return nil, nil, entryFailed(fmt.Errorf("reading %s: %w", path, err))
		}

		return nil, nil, fmt.Errorf("storing %s: %w", path, err)
	}

	err = writer.Close()
	if err != nil {
		var changed *fileChangedError
		if errors.As(err, &changed) {
			return nil, nil, err
		}

		return nil, nil, fmt.Errorf("storing %s: %w", path, err)
	}

	atomic.AddInt64(&w.result.Assumed, int64(len(writer.assumed)))
	atomic.AddInt64(&w.result.Repaired, int64(writer.repaired))

	return writer.Ref(), current, nil
}

// open opens the file at path, rel in the walk, to read and returns it
// with its stat. In the walk's local root, given the stat of the directory
// listed as parent, it opens the file in that directory without following
// a symlink or waiting on a FIFO there. Whatever it opens must still be a
// file.
func (w *Walker) open(ctx context.Context, path, rel string, parent os.FileInfo) (File, os.FileInfo, error) {
	var (
		file File
		err  error
	)

	if w.local != nil && parent != nil {
		file, err = openIn(w.local, rel, path, parent)
	} else {
		file, err = w.fs().Open(ctx, path)
	}

	if err != nil {
		return nil, nil, entryFailed(fmt.Errorf("opening %s: %w", path, err))
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, entryFailed(fmt.Errorf("stat %s: %w", path, err))
	}

	if info.Mode()&(os.ModeDir|os.ModeSymlink|os.ModeNamedPipe|os.ModeSocket|os.ModeDevice) != 0 {
		_ = file.Close()
		return nil, nil, fmt.Errorf("%w: %s is no longer a file", errVanished, path)
	}

	return file, info, nil
}

// hashListed is HashFileFS for the file listed at path, opened as open
// does.
func (w *Walker) hashListed(ctx context.Context, path, rel string, parent os.FileInfo) (*proto.Ref, error) {
	file, info, err := w.open(ctx, path, rel, parent)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	return hashOpened(ctx, file, info, w.Key)
}

// entryFailed classes a failure to open or read one file: errVanished when
// it is gone, errUnreadable when the rest of the tree can still be read,
// and err itself otherwise.
func entryFailed(err error) error {
	switch {
	case errors.Is(err, errVanished):
		return err
	case vanished(err):
		return fmt.Errorf("%w: %v", errVanished, err)
	case entryError(err):
		return fmt.Errorf("%w: %v", errUnreadable, err)
	}

	return err
}

// knownParts returns the blob refs of a changed file's base version and of
// the last live versions the index holds at rel, split files included, so
// only new parts are uploaded. A version that cannot be read adds nothing.
func (w *Walker) knownParts(ctx context.Context, rel string, baseNode *proto.TreeNode) map[string]struct{} {
	if baseNode == nil || baseNode.Stat.GetType() != proto.NodeType_NODE_FILE || baseNode.Ref == nil {
		return nil
	}

	var (
		base     *proto.Object
		versions []*proto.TreeNode
		lookups  sync.WaitGroup
	)

	lookups.Go(func() {
		base, _ = w.Objects.Get(ctx, baseNode.Ref)
	})

	lookups.Go(func() {
		versions, _ = w.Index.FileInfo(ctx, w.Set, IndexPath(w.Key, rel), time.Now(), PreviousVersions)
	})

	lookups.Wait()

	seen := map[string]bool{string(baseNode.Ref.Hash): true}
	var refs []*proto.Ref

	for _, version := range versions {
		if version.Ref != nil && !seen[string(version.Ref.Hash)] {
			seen[string(version.Ref.Hash)] = true
			refs = append(refs, version.Ref)
		}
	}

	files := []*proto.Object{base}
	files = append(files, w.readFiles(ctx, refs)...)

	known := map[string]struct{}{}

	for _, obj := range files {
		if obj.GetFile() == nil {
			continue
		}

		parts, err := FileParts(ctx, w.Objects, obj.GetFile())
		if err != nil {
			continue
		}

		for _, part := range parts {
			if part.Ref != nil {
				known[string(part.Ref.GetHash())] = struct{}{}
			}
		}
	}

	if len(known) == 0 {
		return nil
	}

	return known
}

// readFiles returns the file objects of refs, in one request when the
// store reads many files at once; one that cannot be read is nil.
func (w *Walker) readFiles(ctx context.Context, refs []*proto.Ref) []*proto.Object {
	objects := make([]*proto.Object, len(refs))
	if len(refs) == 0 {
		return objects
	}

	if reader, ok := w.Objects.(FilesReader); ok {
		reads := make([]FileRead, len(refs))
		for i, ref := range refs {
			reads[i] = FileRead{Ref: ref}
		}

		err := reader.ReadFiles(ctx, reads, true, func(i int, obj *proto.Object) error {
			if i < len(objects) {
				objects[i] = obj
			}

			return nil
		}, func(int, int, *proto.Object) error { return nil })
		if err == nil {
			return objects
		}
	}

	var gets sync.WaitGroup

	for i, ref := range refs {
		if objects[i] != nil {
			continue
		}

		gets.Go(func() {
			objects[i], _ = w.Objects.Get(ctx, ref)
		})
	}

	gets.Wait()

	return objects
}

func (w *Walker) checkpointDue() bool {
	return w.CheckpointInterval > 0 && w.now().Sub(w.last) >= w.CheckpointInterval
}

// checkpoint writes a partial commit from the finished root entries plus the
// base's nodes for the rest. A commit ends its session, so with Sessions
// the rest of the run goes on in a new one.
func (w *Walker) checkpoint(ctx context.Context, done []*proto.TreeNode, base []*proto.TreeNode, lastName []byte) error {
	nodes := make([]*proto.TreeNode, 0, len(done)+len(base))
	for _, node := range done {
		if node != nil {
			nodes = append(nodes, node)
		}
	}

	for _, node := range base {
		if bytes.Compare(node.Stat.Name, lastName) > 0 {
			nodes = append(nodes, node)
		}
	}

	root, err := PutTree(ctx, w.Objects, SortNodes(nodes), w.Key, nil)
	if err != nil {
		return fmt.Errorf("storing checkpoint tree: %w", err)
	}

	ref, _, err := w.putCommit(ctx, root, true)
	if err != nil {
		return err
	}

	w.logger().Info("wrote checkpoint commit", "ref", fmt.Sprintf("%x", ref.Hash))

	if w.Sessions != nil {
		sctx, err := w.beginSession(w.outside)
		if err != nil {
			return err
		}

		w.endSession(ctx)
		w.session = sctx
	}
	atomic.AddInt64(&w.result.Checkpoints, 1)
	w.last = w.now()

	return nil
}

func (w *Walker) stat(info os.FileInfo, linkTarget string) *proto.FileInfo {
	user, group := w.fs().Owners(info)

	stat := proto.GetFileInfo(proto.WithDetails(info, user, group, linkTarget))
	recordPlatform(stat, info)

	return stat
}

// metaEqual is the change test on stat data alone.
func metaEqual(a, b *proto.FileInfo) bool {
	return a.GetMtimeNs() == b.GetMtimeNs() &&
		a.GetSize() == b.GetSize() &&
		a.GetMode() == b.GetMode() &&
		a.GetType() == b.GetType()
}

// nodeEqual reports whether a new node would encode identically to the base.
func nodeEqual(node, base *proto.TreeNode) bool {
	if node == nil || base == nil {
		return false
	}

	a, b := node.Stat, base.Stat

	return metaEqual(a, b) &&
		bytes.Equal(a.GetName(), b.GetName()) &&
		bytes.Equal(a.GetUser(), b.GetUser()) &&
		bytes.Equal(a.GetGroup(), b.GetGroup()) &&
		bytes.Equal(a.GetLinkTarget(), b.GetLinkTarget()) &&
		a.GetCtimeNs() == b.GetCtimeNs() &&
		a.GetBirthtimeNs() == b.GetBirthtimeNs() &&
		a.GetAttributes() == b.GetAttributes() &&
		a.GetReparseTag() == b.GetReparseTag() &&
		a.GetSecurity().Equal(b.GetSecurity()) &&
		streamsEqual(a.GetStreams(), b.GetStreams()) &&
		node.Ref.Equal(base.Ref)
}

func streamsEqual(a, b []*proto.Stream) bool {
	return slices.EqualFunc(a, b, func(x, y *proto.Stream) bool {
		return bytes.Equal(x.GetName(), y.GetName()) && x.GetRef().Equal(y.GetRef()) && x.GetSize() == y.GetSize()
	})
}

// HashFile computes the File ref a backup of path on the local disk would
// produce without storing anything.
func HashFile(path string, key *storekey.Key) (*proto.Ref, error) {
	return HashFileFS(context.Background(), LocalFS{}, path, key)
}

// HashFileFS computes the File ref a backup of path on fsys would produce
// without storing anything.
func HashFileFS(ctx context.Context, fsys FS, path string, key *storekey.Key) (*proto.Ref, error) {
	file, err := fsys.Open(ctx, path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	return hashOpened(ctx, file, info, key)
}

// hashOpened is HashFileFS for a file already open, whose stat is info.
func hashOpened(ctx context.Context, file File, info os.FileInfo, key *storekey.Key) (*proto.Ref, error) {
	writer := newFileWriter(ctx, discardStore{}, key, info.Size())

	if _, err := io.Copy(writer, contentReader(file, info)); err != nil {
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	return writer.Ref(), nil
}

// discardStore claims to hold everything, so a fileWriter on it only hashes.
type discardStore struct{}

func (discardStore) Put(context.Context, *proto.Object) error { return nil }
func (discardStore) Get(context.Context, *proto.Ref) (*proto.Object, error) {
	return nil, ErrNotFound
}
func (discardStore) Delete(context.Context, *proto.Ref) error { return nil }
func (discardStore) Walk(context.Context, bool, proto.ObjectType, ObjectReceiver) error {
	return nil
}
func (discardStore) Has(context.Context, *proto.Ref) (bool, error) { return true, nil }

// treeSource serves base trees from a prefetch cache, filling it a subtree
// at a time when the store can stream one.
type treeSource struct {
	getter  Getter
	fetcher TreeFetcher
	key     *storekey.Key
	depth   uint32
	mtx     sync.Mutex
	objects map[string]*proto.Object
}

func (t *treeSource) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	key := string(ref.GetHash())

	t.mtx.Lock()
	obj, ok := t.objects[key]
	if ok {
		delete(t.objects, key)
	}
	t.mtx.Unlock()

	if ok {
		return obj, nil
	}

	if t.fetcher != nil {
		objects, err := t.fetcher.GetTree(ctx, ref, t.depth)
		if err == nil {
			t.mtx.Lock()
			for _, o := range objects {
				k := string(o.Ref().Hash)
				if k != key {
					t.objects[k] = o
				} else {
					obj = o
				}
			}
			t.mtx.Unlock()

			if obj != nil {
				return obj, nil
			}
		}
	}

	return t.getter.Get(ctx, ref)
}

func (t *treeSource) load(ctx context.Context, ref *proto.Ref, parent []byte) (*proto.Tree, error) {
	return OpenTree(ctx, t, ref, t.key, parent)
}

// adoptDamage takes what the store asked to be read again.
func (w *Walker) adoptDamage(grant *CommitGrant) {
	w.rescan = grant.Rescan
	w.damaged = nil

	if len(grant.Damaged) > 0 {
		w.damaged = make(map[string]bool, len(grant.Damaged))
		for _, path := range grant.Damaged {
			w.damaged[path] = true
		}
	}

	switch {
	case w.rescan:
		w.logger().Info("the store lost content it could not place, reading every file again", "set", w.Set)
	case w.damaged != nil:
		w.logger().Info("the store lost content at some paths, reading them again", "set", w.Set, "paths", len(w.damaged))
	}
}

// lost reports whether the store asked for this path to be read again.
func (w *Walker) lost(rel string) bool {
	return w.rescan || w.damaged[IndexPath(w.Key, rel)]
}

// unchanged reports whether Unchanged vouches for the directory at rel.
func (w *Walker) unchanged(rel string) bool {
	return w.Unchanged != nil && !w.Verify && w.Unchanged(rel)
}

// reusesUnchanged reports whether the directory at rel is taken whole from
// its base node.
func (w *Walker) reusesUnchanged(rel string, baseNode *proto.TreeNode) bool {
	if !w.unchanged(rel) || baseNode == nil || !baseNode.Stat.IsDir() || baseNode.Ref == nil || w.rescan {
		return false
	}

	dir := IndexPath(w.Key, rel)
	for path := range w.damaged {
		if path == dir || strings.HasPrefix(path, dir+"/") {
			return false
		}
	}

	return true
}
