package pack

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/bits-and-blooms/bitset"
	"github.com/stretchr/testify/require"
)

func batchObjects(objects []*proto.Object, batchSize int) [][]*proto.Object {
	var batches [][]*proto.Object

	for batchSize < len(objects) {
		objects, batches = objects[batchSize:], append(batches, objects[0:batchSize:batchSize])
	}
	batches = append(batches, objects)

	return batches
}

func makeGCFiles(parts []*proto.Object) []*proto.Object {
	var files []*proto.Object
	for _, batch := range batchObjects(parts, 5) {
		var parts []*proto.FilePart
		var offset uint64
		for _, obj := range batch {
			length := uint64(len(obj.GetBlob().GetData())) + 1
			parts = append(parts, &proto.FilePart{
				Ref:    obj.Ref(),
				Offset: offset,
				Length: length,
			})
			offset += length
		}

		files = append(files, proto.NewObject(&proto.File{
			Parts: parts,
		}))
	}

	return files
}

func makeGCTrees(nodes []*proto.Object) []*proto.Object {
	var trees []*proto.Object
	for _, batch := range batchObjects(nodes, 3) {
		trees = append(trees, treeOf(batch))
	}

	return trees
}

// treeOf builds a tree whose nodes point at the objects under generated names.
func treeOf(children []*proto.Object) *proto.Object {
	var nodes []*proto.TreeNode
	for i, obj := range children {
		stat := &proto.FileInfo{Name: []byte(fmt.Sprintf("node-%03d", i)), Mode: 0644, Size: 1}
		if obj.Type() == proto.ObjectType_TREE {
			stat = &proto.FileInfo{Name: []byte(fmt.Sprintf("node-%03d", i)), Mode: 0755, Type: proto.NodeType_NODE_DIRECTORY}
		}

		nodes = append(nodes, &proto.TreeNode{Stat: stat, Ref: obj.Ref()})
	}

	return proto.NewObject(&proto.Tree{Nodes: nodes})
}

func makeGCCommits(trees []*proto.Object) []*proto.Object {
	var commits []*proto.Object
	for i, tree := range trees {
		commits = append(commits, proto.NewObject(&proto.Commit{
			Tree:      tree.Ref(),
			Timestamp: int64(i + 1),
			BackupSet: "world",
		}))
	}

	return commits
}

// makeGCTestData returns two slices of objects. the first one contains objects that
// should be marked reachable after a gc mark phase and the second contains only
// objects that should be unreachable
func makeGCTestData(t *testing.T) ([]*proto.Object, []*proto.Object) {
	var reachable []*proto.Object
	var unreachable []*proto.Object

	// generate a bunch of blobs
	blobs := makeTestData(t, numObjects)
	perm := rand.Perm(len(blobs))

	var reachableBlobs []*proto.Object
	for _, i := range perm[:len(perm)/2] {
		reachableBlobs = append(reachableBlobs, blobs[i])
	}

	var unreachableBlobs []*proto.Object
	for _, i := range perm[len(perm)/2:] {
		unreachableBlobs = append(unreachableBlobs, blobs[i])
	}
	require.EqualValues(t, len(perm), len(reachableBlobs)+len(unreachableBlobs))

	// generate some files
	reachableFiles := makeGCFiles(reachableBlobs)
	unreachableFiles := makeGCFiles(unreachableBlobs)

	// TODO: generate file splits

	// generate some trees
	t.Log("generating reachable trees")
	reachableTrees := makeGCTrees(reachableFiles)
	unreachableTrees := makeGCTrees(unreachableFiles)

	commits := makeGCCommits(reachableTrees)

	reachable = append(reachable, reachableBlobs...)
	reachable = append(reachable, reachableFiles...)
	reachable = append(reachable, reachableTrees...)
	reachable = append(reachable, commits...)

	unreachable = append(unreachable, unreachableBlobs...)
	unreachable = append(unreachable, unreachableFiles...)
	unreachable = append(unreachable, unreachableTrees...)

	return reachable, unreachable
}

func newGCStore(t *testing.T, base string) *PackStorage {
	t.Helper()

	store, err := NewPackStorage(
		WithArchiveStorage(newLocal(base)),
		WithArchiveIndex(NewInMemoryIndex()),
		WithMaxSize(256*1024),
	)
	require.NoError(t, err)
	require.NoError(t, store.Open())

	return store
}

func putAll(t *testing.T, store *PackStorage, objects []*proto.Object) {
	t.Helper()

	ctx := context.Background()
	for _, i := range rand.Perm(len(objects)) {
		require.NoError(t, store.Put(ctx, objects[i]))
	}

	require.NoError(t, store.Flush())
}

func requireStored(t *testing.T, store *PackStorage, objects []*proto.Object, stored bool) {
	t.Helper()

	ctx := context.Background()
	for _, obj := range objects {
		_, err := store.Get(ctx, obj.Ref())
		if stored {
			require.NoErrorf(t, err, "object %x of type %s", obj.Ref().Hash, obj.Type())
		} else {
			require.ErrorIsf(t, err, backup.ErrNotFound, "object %x of type %s", obj.Ref().Hash, obj.Type())
		}
	}
}

func requirePresent(t *testing.T, store *PackStorage, objects []*proto.Object, present bool) {
	t.Helper()

	for _, obj := range objects {
		has, err := store.Has(context.Background(), obj.Ref())
		require.NoError(t, err)
		require.Equalf(t, present, has, "presence of %x of type %s", obj.Ref().Hash, obj.Type())
	}
}

func gcOptions(t *testing.T, ahead time.Duration) CollectOptions {
	return CollectOptions{
		Now:       time.Now().Add(ahead),
		DeadRatio: 1e-9,
		TempDir:   t.TempDir(),
		Readers:   4,
	}
}

func TestCollectMarksThenSweeps(t *testing.T) {
	base := t.TempDir()
	store := newGCStore(t, base)
	ctx := context.Background()

	reachable, unreachable := makeGCTestData(t)
	putAll(t, store, append(append([]*proto.Object{}, reachable...), unreachable...))

	never, err := store.LastCollected()
	require.NoError(t, err)
	require.True(t, never.IsZero())

	opts := gcOptions(t, 0)
	first, err := store.Collect(ctx, opts)
	require.NoError(t, err)
	require.EqualValues(t, 1, first.Generation)

	last, err := store.LastCollected()
	require.NoError(t, err)
	require.True(t, last.Equal(opts.Now), "the newest collection marked as of its now")
	require.Equal(t, "first generation", first.SweepSkipped)
	require.EqualValues(t, len(unreachable), first.DeadObjects)
	require.EqualValues(t, len(reachable), first.Marked)
	require.EqualValues(t, len(unreachable), first.Condemned)
	require.Greater(t, first.Roots, 0)

	results, err := filepath.Glob(filepath.Join(base, "*"+GCExt))
	require.NoError(t, err)
	require.Len(t, results, first.Archives)

	requireStored(t, store, unreachable, true)
	requirePresent(t, store, unreachable, false)
	requirePresent(t, store, reachable, true)

	second, err := store.Collect(ctx, gcOptions(t, 48*time.Hour))
	require.NoError(t, err)
	require.EqualValues(t, 2, second.Generation)
	require.Empty(t, second.SweepSkipped)
	require.Greater(t, second.Swept, 0)
	require.EqualValues(t, len(unreachable), second.ReclaimedObjects)

	requireStored(t, store, unreachable, false)
	requireStored(t, store, reachable, true)
	requirePresent(t, store, reachable, true)

	third, err := store.Collect(ctx, gcOptions(t, 72*time.Hour))
	require.NoError(t, err)
	require.Zero(t, third.Condemned)
	require.EqualValues(t, len(unreachable), third.ReclaimedObjects, "the tombstones hide no copy any more")

	fourth, err := store.Collect(ctx, gcOptions(t, 96*time.Hour))
	require.NoError(t, err)
	require.Zero(t, fourth.DeadObjects)
	requireStored(t, store, reachable, true)

	require.NoError(t, store.Close())
}

func TestCollectResumesInterruptedMark(t *testing.T) {
	base := t.TempDir()
	store := newGCStore(t, base)
	ctx := context.Background()

	reachable, unreachable := makeGCTestData(t)
	putAll(t, store, append(append([]*proto.Object{}, reachable...), unreachable...))

	opts := gcOptions(t, 0)

	defer func(n int) { rootBatch = n; gcAfterBatch = nil }(rootBatch)
	rootBatch = 2

	interrupted := errors.New("interrupted")
	gcAfterBatch = func(batch int) error {
		if batch == 1 {
			return interrupted
		}
		return nil
	}

	_, err := store.Collect(ctx, opts)
	require.ErrorIs(t, err, interrupted)

	runDir := filepath.Join(opts.TempDir, "goback-gc", "gen-1")
	done, err := filepath.Glob(filepath.Join(runDir, "batch-*.done"))
	require.NoError(t, err)
	require.Len(t, done, 2, "the completed batches are checkpointed")

	gcAfterBatch = nil
	report, err := store.Collect(ctx, opts)
	require.NoError(t, err)
	require.Equal(t, 2, report.Resumed)
	require.EqualValues(t, len(unreachable), report.DeadObjects)
	require.EqualValues(t, len(reachable), report.Marked)

	_, err = os.Stat(runDir)
	require.True(t, os.IsNotExist(err), "a finished run leaves no checkpoints")

	requirePresent(t, store, unreachable, false)
	requirePresent(t, store, reachable, true)

	require.NoError(t, store.Close())
}

func TestCollectRestartsAnInterruptedMarkWhenTheRootsChange(t *testing.T) {
	base := t.TempDir()
	store := newGCStore(t, base)
	ctx := context.Background()

	reachable, unreachable := makeGCTestData(t)
	putAll(t, store, append(append([]*proto.Object{}, reachable...), unreachable...))

	opts := gcOptions(t, 0)

	defer func(n int) { rootBatch = n; gcAfterBatch = nil }(rootBatch)
	rootBatch = 2

	interrupted := errors.New("interrupted")
	gcAfterBatch = func(batch int) error {
		if batch == 1 {
			return interrupted
		}
		return nil
	}

	_, err := store.Collect(ctx, opts)
	require.ErrorIs(t, err, interrupted)

	// a restore lease adds a root, which shifts every batch after it
	var target *proto.Ref
	for _, obj := range unreachable {
		if obj.Type() == proto.ObjectType_TREE {
			target = obj.Ref()
			break
		}
	}
	require.NotNil(t, target)

	sctx, err := store.BeginSession(ctx, &backup.Session{Set: "world", Restore: target})
	require.NoError(t, err)

	gcAfterBatch = nil
	report, err := store.Collect(ctx, opts)
	require.NoError(t, err)
	require.Zero(t, report.Resumed, "the checkpoints of the old root list are not reused")
	require.Greater(t, int(report.Marked), len(reachable), "the leased tree and its files stay live")
	requirePresent(t, store, []*proto.Object{unreachable[slices.IndexFunc(unreachable, func(o *proto.Object) bool { return o.Ref().Equal(target) })]}, true)

	requirePresent(t, store, reachable, true)
	require.NoError(t, store.EndSession(sctx))
	require.NoError(t, store.Close())
}

func TestCollectKeepsYoungObjects(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	ctx := context.Background()

	reachable, unreachable := makeGCTestData(t)
	putAll(t, store, append(append([]*proto.Object{}, reachable...), unreachable...))

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	second, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)
	require.Empty(t, second.SweepSkipped)
	require.Zero(t, second.ReclaimedObjects)

	requireStored(t, store, unreachable, true)
	require.NoError(t, store.Close())
}

// makeChain builds files, trees and one commit over the blobs.
func makeChain(blobs []*proto.Object) []*proto.Object {
	files := makeGCFiles(blobs)
	trees := makeGCTrees(files)

	root := treeOf(trees)
	commit := proto.NewObject(&proto.Commit{Tree: root.Ref(), Timestamp: 1, BackupSet: "world"})

	var all []*proto.Object
	all = append(all, blobs...)
	all = append(all, files...)
	all = append(all, trees...)
	all = append(all, root, commit)

	return all
}

func countTombstones(t *testing.T, store *PackStorage) int {
	t.Helper()

	n := 0
	err := store.WalkHeaders(context.Background(), proto.ObjectType_TOMBSTONE, func(*proto.ObjectHeader) error {
		n++
		return nil
	})
	require.NoError(t, err)

	return n
}

func TestCollectReclaimsTombstonedCommit(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	ctx := context.Background()

	blobs := makeTestData(t, 40)
	gone := makeChain(blobs[:20])
	kept := makeChain(blobs[20:])
	putAll(t, store, append(append([]*proto.Object{}, gone...), kept...))

	require.NoError(t, store.Delete(ctx, gone[len(gone)-1].Ref()))
	require.NoError(t, store.Flush())
	require.Equal(t, 1, countTombstones(t, store))

	first, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)
	require.EqualValues(t, len(gone)-1, first.Condemned, "the commit has its tombstone already")
	requirePresent(t, store, gone, false)
	requirePresent(t, store, kept, true)

	second, err := store.Collect(ctx, gcOptions(t, 48*time.Hour))
	require.NoError(t, err)
	require.EqualValues(t, len(gone), second.ReclaimedObjects)
	requireStored(t, store, gone, false)
	requireStored(t, store, kept, true)
	require.Equal(t, len(gone), countTombstones(t, store), "the tombstones outlive the copies they condemned")

	third, err := store.Collect(ctx, gcOptions(t, 72*time.Hour))
	require.NoError(t, err)
	require.EqualValues(t, len(gone), third.ReclaimedObjects, "a tombstone that hides no copy is spent")
	require.Equal(t, 0, countTombstones(t, store))
	requireStored(t, store, kept, true)

	require.NoError(t, store.Close())
}

func TestCollectReclaimsACommitWhoseTombstoneACompactionMovedFirst(t *testing.T) {
	store, err := NewPackStorage(
		WithArchiveStorage(newLocal(t.TempDir())),
		WithArchiveIndex(NewInMemoryIndex()),
		WithMaxSize(256*1024),
		WithCompaction(CompactionConfig{MinimumCandidates: 0}),
	)
	require.NoError(t, err)
	require.NoError(t, store.Open())
	ctx := context.Background()

	blobs := makeTestData(t, 40)
	gone := makeChain(blobs[:20])
	kept := makeChain(blobs[20:])
	putAll(t, store, append(append([]*proto.Object{}, gone...), kept...))

	commit := gone[len(gone)-1].Ref()
	require.NoError(t, store.Delete(ctx, commit))
	require.NoError(t, store.Flush())
	compact(t, context.Background(), store)

	tomb := proto.TombstoneRef(commit)
	found, err := store.index.LocateTombstones([]*proto.Ref{tomb}, Scope{})
	require.NoError(t, err)
	require.Len(t, found[string(tomb.Hash)], 1)
	require.NotZero(t, found[string(tomb.Hash)][0].Record.CarriedTime, "the compaction moved the tombstone")

	_, err = store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	second, err := store.Collect(ctx, gcOptions(t, 48*time.Hour))
	require.NoError(t, err)
	require.EqualValues(t, len(gone), second.ReclaimedObjects)
	requireStored(t, store, gone, false)
	requireStored(t, store, kept, true)

	third, err := store.Collect(ctx, gcOptions(t, 72*time.Hour))
	require.NoError(t, err)
	require.EqualValues(t, len(gone), third.ReclaimedObjects, "the tombstones are spent")
	require.Equal(t, 0, countTombstones(t, store))
	requireStored(t, store, kept, true)

	require.NoError(t, store.Close())
}

func TestCollectSweepsErasedArchivesEarly(t *testing.T) {
	for _, erase := range []bool{false, true} {
		t.Run(fmt.Sprintf("erase=%v", erase), func(t *testing.T) {
			store := newGCStore(t, t.TempDir())
			ctx := context.Background()

			blobs := makeTestData(t, 40)
			gone := makeChain(blobs[:20])
			kept := makeChain(blobs[20:])
			putAll(t, store, append(append([]*proto.Object{}, gone...), kept...))

			target := gone[len(gone)-1].Ref()
			if erase {
				require.NoError(t, store.Erase(ctx, target))
			} else {
				require.NoError(t, store.Delete(ctx, target))
			}
			require.NoError(t, store.Flush())

			// neither the dead ratio nor the erasure bound ever selects
			patient := func(ahead time.Duration) CollectOptions {
				opts := gcOptions(t, ahead)
				opts.DeadRatio = 2
				opts.ErasureBound = 365 * 24 * time.Hour
				return opts
			}

			first, err := store.Collect(ctx, patient(0))
			require.NoError(t, err)
			if erase {
				require.Greater(t, first.ErasedArchives, 0)
			} else {
				require.Zero(t, first.ErasedArchives)
			}

			second, err := store.Collect(ctx, patient(48*time.Hour))
			require.NoError(t, err)
			require.Empty(t, second.SweepSkipped)

			if erase {
				require.EqualValues(t, len(gone), second.ReclaimedObjects, "the erasure flag selects the archives")
				requireStored(t, store, gone, false)
			} else {
				require.Zero(t, second.Swept, "a plain tombstone waits for the ratio or the bound")
				requireStored(t, store, gone, true)
			}
			requireStored(t, store, kept, true)

			require.NoError(t, store.Close())
		})
	}
}

// TestCollectCarriesTheErasureClockThroughARewrite compacts the archives
// holding a tombstoned commit after its first unmarked generation and
// expects the erasure bound to count from that generation, not from the
// rewrite.
func TestCollectCarriesTheErasureClockThroughARewrite(t *testing.T) {
	store, err := NewPackStorage(
		WithArchiveStorage(newLocal(t.TempDir())),
		WithArchiveIndex(NewInMemoryIndex()),
		WithMaxSize(1024*1024),
		WithCompaction(CompactionConfig{MinimumCandidates: 0}),
	)
	require.NoError(t, err)
	require.NoError(t, store.Open())
	ctx := context.Background()

	blobs := makeTestData(t, 40)
	gone := makeChain(blobs[:20])
	kept := makeChain(blobs[20:])
	putAll(t, store, append(append([]*proto.Object{}, gone...), kept...))

	require.NoError(t, store.Delete(ctx, gone[len(gone)-1].Ref()))
	require.NoError(t, store.Flush())

	// only the erasure bound may select, ten days after the first unmarked generation
	patient := func(ahead time.Duration) CollectOptions {
		opts := gcOptions(t, ahead)
		opts.DeadRatio = 2
		opts.ErasureBound = 10 * 24 * time.Hour
		return opts
	}

	_, err = store.Collect(ctx, patient(0))
	require.NoError(t, err)

	before, _, err := store.archiveNames()
	require.NoError(t, err)
	compact(t, context.Background(), store)
	after, _, err := store.archiveNames()
	require.NoError(t, err)
	require.NotEqual(t, before, after, "the rewrite replaced the archives")

	waiting, err := store.Collect(ctx, patient(48*time.Hour))
	require.NoError(t, err)
	requireStored(t, store, gone, true)
	require.False(t, waiting.OldestDead.IsZero(), "the dead objects wait for the bound")

	report, err := store.Collect(ctx, patient(11*24*time.Hour))
	require.NoError(t, err)
	require.EqualValues(t, len(gone), report.ReclaimedObjects, "the bound counts from the generation before the rewrite")
	require.True(t, report.OldestDead.IsZero(), "no archive holds dead objects past the rewrite")
	requireStored(t, store, gone, false)
	requireStored(t, store, kept, true)

	require.NoError(t, store.Close())
}

func TestCollectKeepsRestoreSessionTargets(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	ctx := context.Background()

	blobs := makeTestData(t, 40)
	gone := makeChain(blobs[:20])
	kept := makeChain(blobs[20:])
	putAll(t, store, append(append([]*proto.Object{}, gone...), kept...))

	target := gone[len(gone)-1].Ref()
	require.NoError(t, store.Delete(ctx, target))
	require.NoError(t, store.Flush())

	sctx, err := store.BeginSession(ctx, &backup.Session{Set: "world", Restore: target})
	require.NoError(t, err)

	leases, err := store.RestoreLeases(ctx)
	require.NoError(t, err)
	require.Len(t, leases, 1)

	_, err = store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)
	requireStored(t, store, gone, true)

	require.NoError(t, store.EndSession(sctx))

	leases, err = store.RestoreLeases(ctx)
	require.NoError(t, err)
	require.Empty(t, leases)

	_, err = store.Collect(ctx, gcOptions(t, 48*time.Hour))
	require.NoError(t, err)
	requirePresent(t, store, gone, false)
	requirePresent(t, store, kept, true)

	require.NoError(t, store.Close())
}

func TestCollectResultsSurviveReopen(t *testing.T) {
	base := t.TempDir()
	store := newGCStore(t, base)
	ctx := context.Background()

	reachable, unreachable := makeGCTestData(t)
	putAll(t, store, append(append([]*proto.Object{}, reachable...), unreachable...))

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)
	require.NoError(t, store.Close())

	store = newGCStore(t, base)
	requirePresent(t, store, unreachable, false)
	requirePresent(t, store, reachable, true)

	second, err := store.Collect(ctx, gcOptions(t, 48*time.Hour))
	require.NoError(t, err)
	require.EqualValues(t, 2, second.Generation)
	require.EqualValues(t, len(unreachable), second.ReclaimedObjects)
	requireStored(t, store, reachable, true)

	require.NoError(t, store.Close())
}

func TestCollectRefusesSessionContext(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	sctx, _ := beginSession(t, store, "a1")

	_, err := store.Collect(sctx, CollectOptions{})
	require.Error(t, err)
	require.NoError(t, store.Close())
}

func TestGCFileRoundTrip(t *testing.T) {
	storage := newLocal(t.TempDir())

	current := bitset.New(10)
	current.Set(1)
	current.Set(9)
	previous := bitset.New(10)
	previous.Set(3)

	in := &gcFile{
		Generation:  7,
		Snapshot:    time.Unix(1700000000, 0).UTC(),
		Current:     current,
		Previous:    previous,
		DeadObjects: 8,
		DeadBytes:   4096,
		DeadSince:   time.Unix(1600000000, 0).UTC(),
		Dead:        []uint64{1, 2, 1 << 60},
		Erase:       true,
	}
	require.NoError(t, writeGCFile(storage, "root/a", in))

	out, err := readGCFile(storage, "root/a")
	require.NoError(t, err)
	require.Equal(t, in, out)

	require.True(t, out.dead(0))
	require.False(t, out.dead(1))
	require.False(t, out.dead(3))
	require.True(t, out.candidate([]byte{0, 0, 0, 0, 0, 0, 0, 2, 0xff}))
	require.False(t, out.candidate([]byte{0, 0, 0, 0, 0, 0, 0, 3, 0xff}))

	missing, err := readGCFile(storage, "root/none")
	require.NoError(t, err)
	require.Nil(t, missing)

	in.Previous = nil
	require.NoError(t, writeGCFile(storage, "root/b", in))
	out, err = readGCFile(storage, "root/b")
	require.NoError(t, err)
	require.Nil(t, out.Previous)
	require.False(t, out.dead(0))
}

func TestLiveRunsMergeSortedAndDistinct(t *testing.T) {
	runs := newLiveRuns(t.TempDir(), 100)
	defer runs.close()

	var refs []liveRef
	for i := 0; i < 1000; i++ {
		n := uint64(rand.Intn(300))

		var k refKey
		binary.BigEndian.PutUint64(k[:], n)
		refs = append(refs, liveRef{key: k, size: n * 10})
	}

	for start := 0; start < len(refs); start += 37 {
		require.NoError(t, runs.add(refs[start:min(start+37, len(refs))]))
	}
	require.NotEmpty(t, runs.files)

	distinct := make(map[refKey]bool)
	for _, ref := range refs {
		distinct[ref.key] = true
	}

	it, err := runs.iterator()
	require.NoError(t, err)
	defer it.close()

	// asked in order, the runs answer for every key they hold and no other
	found := 0
	for i := 0; i < 400; i++ {
		var k refKey
		binary.BigEndian.PutUint64(k[:], uint64(i))

		owners, size, ok := it.at(k)
		require.Equal(t, distinct[k], ok, "key %d", i)

		if ok {
			found++
			require.Equal(t, []Attribution{{}}, owners, "one unattributed run holds them all")
			require.EqualValues(t, i*10, size, "key %d", i)
		}
	}

	require.Equal(t, len(distinct), found)
}

// storedBytes is what the archives spend on these objects.
func storedBytes(t *testing.T, store *PackStorage, objects ...*proto.Object) uint64 {
	t.Helper()

	var total uint64
	for _, obj := range objects {
		loc, err := store.index.LocateObject(obj.Ref(), Scope{})
		require.NoError(t, err)

		total += uint64(loc.Record.Length)
	}

	return total
}

func TestCollectAttributesObjectsToTheSetThatReachesThemFirst(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	ctx := context.Background()

	shared := proto.NewObject(&proto.Blob{Data: bytes.Repeat([]byte("s"), 512)})
	mine := proto.NewObject(&proto.Blob{Data: bytes.Repeat([]byte("m"), 1024)})
	yours := proto.NewObject(&proto.Blob{Data: bytes.Repeat([]byte("y"), 2048)})

	myFile := proto.NewObject(&proto.File{Parts: []*proto.FilePart{
		{Ref: mine.Ref(), Length: 1024},
		{Ref: shared.Ref(), Offset: 1024, Length: 512},
	}})
	yourFile := proto.NewObject(&proto.File{Parts: []*proto.FilePart{
		{Ref: shared.Ref(), Length: 512},
		{Ref: yours.Ref(), Offset: 512, Length: 2048},
	}})

	myTree := treeOf([]*proto.Object{myFile})
	yourTree := treeOf([]*proto.Object{yourFile})

	myCommit := proto.NewObject(&proto.Commit{Tree: myTree.Ref(), Timestamp: 1, BackupSet: "mine"})
	yourCommit := proto.NewObject(&proto.Commit{Tree: yourTree.Ref(), Timestamp: 2, BackupSet: "yours"})

	putAll(t, store, []*proto.Object{shared, mine, yours, myFile, yourFile, myTree, yourTree, myCommit, yourCommit})

	opts := gcOptions(t, 0)
	opts.Owner = func(root []byte) Attribution {
		switch {
		case bytes.Equal(root, myCommit.Ref().Hash):
			return Attribution{Set: 7}
		case bytes.Equal(root, yourCommit.Ref().Hash):
			return Attribution{Set: 9}
		}

		return Attribution{}
	}

	report, err := store.Collect(ctx, opts)
	require.NoError(t, err)

	require.EqualValues(t, storedBytes(t, store, myCommit, myTree, myFile, mine, shared), report.SetBytes[7])
	require.EqualValues(t, storedBytes(t, store, yourCommit, yourTree, yourFile, yours), report.SetBytes[9],
		"the shared blob belongs to the set that reached it first")
	require.EqualValues(t, 1024+512, report.SetDeduplicated[7])
	require.EqualValues(t, 2048, report.SetDeduplicated[9])
	require.EqualValues(t, 1024+512, report.SetDeduplicatedAlone[7])
	require.EqualValues(t, 512+2048, report.SetDeduplicatedAlone[9], "alone, each set counts the content both hold")

	var attributed uint64
	for _, bytes := range report.SetBytes {
		attributed += bytes
	}
	require.EqualValues(t, report.Marked, 9, "every object is live")
	require.EqualValues(t, storedBytes(t, store, shared, mine, yours, myFile, yourFile, myTree, yourTree, myCommit, yourCommit), attributed,
		"what the sets hold adds up to what the store holds")
	require.Zero(t, report.Unattributed)

	require.NoError(t, store.Close())
}

func TestCollectReportsWhatOnlyCommitsWithoutAnOwnerReach(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	ctx := context.Background()

	shared := proto.NewObject(&proto.Blob{Data: bytes.Repeat([]byte("s"), 512)})
	lone := proto.NewObject(&proto.Blob{Data: bytes.Repeat([]byte("l"), 1024)})

	ownedFile := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Ref: shared.Ref(), Length: 512}}})
	loneFile := proto.NewObject(&proto.File{Parts: []*proto.FilePart{
		{Ref: shared.Ref(), Length: 512},
		{Ref: lone.Ref(), Offset: 512, Length: 1024},
	}})

	ownedTree := treeOf([]*proto.Object{ownedFile})
	loneTree := treeOf([]*proto.Object{loneFile})

	owned := proto.NewObject(&proto.Commit{Tree: ownedTree.Ref(), Timestamp: 1, BackupSet: "mine"})
	orphan := proto.NewObject(&proto.Commit{Tree: loneTree.Ref(), Timestamp: 2, BackupSet: "mine"})

	putAll(t, store, []*proto.Object{shared, lone, ownedFile, loneFile, ownedTree, loneTree, owned, orphan})

	opts := gcOptions(t, 0)
	opts.Owner = func(root []byte) Attribution {
		if bytes.Equal(root, owned.Ref().Hash) {
			return Attribution{Set: 7}
		}

		return Attribution{}
	}

	report, err := store.Collect(ctx, opts)
	require.NoError(t, err)
	require.EqualValues(t, storedBytes(t, store, orphan, loneTree, loneFile, lone), report.Unattributed)

	require.NoError(t, store.Close())
}

func TestCollectCountsTheContentOfEachSetOnceBeforeCompression(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	ctx := context.Background()

	shared := proto.NewObject(&proto.Blob{Data: bytes.Repeat([]byte("s"), 8192)})
	mine := proto.NewObject(&proto.Blob{Data: bytes.Repeat([]byte("m"), 4096)})

	myFile := proto.NewObject(&proto.File{Parts: []*proto.FilePart{
		{Ref: mine.Ref(), Length: 4096},
		{Ref: shared.Ref(), Offset: 4096, Length: 8192},
	}})
	sameContent := proto.NewObject(&proto.File{Parts: []*proto.FilePart{
		{Ref: shared.Ref(), Length: 8192},
		{Ref: mine.Ref(), Offset: 8192, Length: 4096},
	}})
	small := proto.NewObject(&proto.File{Inline: []byte("tiny")})
	myCommit := proto.NewObject(&proto.Commit{Tree: treeOf([]*proto.Object{myFile, sameContent, small}).Ref(), Timestamp: 1})

	putAll(t, store, []*proto.Object{shared, mine})
	putAll(t, store, []*proto.Object{shared, mine, myFile, sameContent, small, treeOf([]*proto.Object{myFile, sameContent, small}), myCommit})

	opts := gcOptions(t, 0)
	opts.Owner = func(root []byte) Attribution {
		if bytes.Equal(root, myCommit.Ref().Hash) {
			return Attribution{Set: 7}
		}

		return Attribution{}
	}

	report, err := store.Collect(ctx, opts)
	require.NoError(t, err)

	require.EqualValues(t, 4096+8192+len("tiny"), report.SetDeduplicated[7], "each chunk counts once, at its length before compression")
	require.Greater(t, report.SetBytes[7], storedBytes(t, store, shared, mine), "both copies take up room")

	require.NoError(t, store.Close())
}

func TestCollectCountsASharedObjectInEveryGroup(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	ctx := context.Background()

	shared := proto.NewObject(&proto.Blob{Data: bytes.Repeat([]byte("s"), 512)})

	myFile := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Ref: shared.Ref(), Length: 512}}})
	myTree := treeOf([]*proto.Object{myFile})
	yourTree := treeOf([]*proto.Object{myFile, myFile})

	myCommit := proto.NewObject(&proto.Commit{Tree: myTree.Ref(), Timestamp: 1, BackupSet: "mine"})
	yourCommit := proto.NewObject(&proto.Commit{Tree: yourTree.Ref(), Timestamp: 2, BackupSet: "yours"})

	putAll(t, store, []*proto.Object{shared, myFile, myTree, yourTree, myCommit, yourCommit})

	opts := gcOptions(t, 0)
	opts.Owner = func(root []byte) Attribution {
		switch {
		case bytes.Equal(root, myCommit.Ref().Hash):
			return Attribution{Group: 1, Set: 7}
		case bytes.Equal(root, yourCommit.Ref().Hash):
			return Attribution{Group: 2, Set: 9}
		}

		return Attribution{}
	}

	report, err := store.Collect(ctx, opts)
	require.NoError(t, err)

	// the file and its chunk belong to both groups, so both carry them
	require.EqualValues(t, storedBytes(t, store, myCommit, myTree, myFile, shared), report.SetBytes[7])
	require.EqualValues(t, storedBytes(t, store, yourCommit, yourTree, myFile, shared), report.SetBytes[9])
	require.EqualValues(t, 512, report.SetDeduplicated[7])
	require.EqualValues(t, 512, report.SetDeduplicated[9])

	require.NoError(t, store.Close())
}

func TestCollectCountsWhatEachSetHoldsAloneAndWhatOnlyItHolds(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	ctx := context.Background()

	shared := proto.NewObject(&proto.Blob{Data: bytes.Repeat([]byte("s"), 512)})
	mine := proto.NewObject(&proto.Blob{Data: bytes.Repeat([]byte("m"), 1024)})

	sharedFile := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Ref: shared.Ref(), Length: 512}}})
	myFile := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Ref: mine.Ref(), Length: 1024}}})
	sharedTree := treeOf([]*proto.Object{sharedFile})
	myTree := treeOf([]*proto.Object{myFile, sharedTree})

	myCommit := proto.NewObject(&proto.Commit{Tree: myTree.Ref(), Timestamp: 1, BackupSet: "mine"})
	yourCommit := proto.NewObject(&proto.Commit{Tree: sharedTree.Ref(), Timestamp: 2, BackupSet: "yours"})

	putAll(t, store, []*proto.Object{shared, mine, sharedFile, myFile, sharedTree, myTree, myCommit, yourCommit})

	opts := gcOptions(t, 0)
	opts.Owner = func(root []byte) Attribution {
		switch {
		case bytes.Equal(root, myCommit.Ref().Hash):
			return Attribution{Group: 1, Set: 7}
		case bytes.Equal(root, yourCommit.Ref().Hash):
			return Attribution{Group: 1, Set: 9}
		}

		return Attribution{}
	}

	report, err := store.Collect(ctx, opts)
	require.NoError(t, err)

	sharedBytes := storedBytes(t, store, sharedTree, sharedFile, shared)
	require.EqualValues(t, storedBytes(t, store, myCommit, myTree, myFile, mine)+sharedBytes, report.SetAlone[7])
	require.EqualValues(t, storedBytes(t, store, yourCommit)+sharedBytes, report.SetAlone[9], "the second set walks into the subtree the first reached")
	require.EqualValues(t, storedBytes(t, store, myCommit, myTree, myFile, mine), report.SetExclusive[7])
	require.EqualValues(t, storedBytes(t, store, yourCommit), report.SetExclusive[9])
	require.EqualValues(t, report.SetAlone[7], report.SetBytes[7], "the first set still carries what both hold")
	require.EqualValues(t, storedBytes(t, store, yourCommit), report.SetBytes[9])
	require.EqualValues(t, 1024+512, report.SetDeduplicatedAlone[7])
	require.EqualValues(t, 512, report.SetDeduplicatedAlone[9])
	require.EqualValues(t, 1024+512, report.SetDeduplicated[7])
	require.Zero(t, report.SetDeduplicated[9], "attributed, the shared content counts once")

	require.NoError(t, store.Close())
}

func TestCollectAttributesNothingWithoutAnOwner(t *testing.T) {
	store := newGCStore(t, t.TempDir())

	reachable, unreachable := makeGCTestData(t)
	putAll(t, store, append(append([]*proto.Object{}, reachable...), unreachable...))

	report, err := store.Collect(context.Background(), gcOptions(t, 0))
	require.NoError(t, err)
	require.Empty(t, report.SetBytes)

	require.NoError(t, store.Close())
}

func TestCompactionBetweenCollectionsKeepsTheUnreachableCounting(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	ctx := context.Background()

	reachable, unreachable := makeGCTestData(t)
	putAll(t, store, append(append([]*proto.Object{}, reachable...), unreachable...))

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	compact(t, context.Background(), store)
	requirePresent(t, store, unreachable, false)
	requirePresent(t, store, reachable, true)

	second, err := store.Collect(ctx, gcOptions(t, 48*time.Hour))
	require.NoError(t, err)
	require.EqualValues(t, len(unreachable), second.ReclaimedObjects, "the generation before the compaction still counts")

	requireStored(t, store, unreachable, false)
	requireStored(t, store, reachable, true)

	require.NoError(t, store.Close())
}

// requireOneCopy expects the index to place every object exactly once.
func requireOneCopy(t *testing.T, store *PackStorage, objects []*proto.Object) {
	t.Helper()

	for _, obj := range objects {
		copies, err := store.index.LocateCopies([]*proto.Ref{obj.Ref()}, Scope{})
		require.NoError(t, err)
		require.Lenf(t, copies[string(obj.Ref().Hash)], 1, "copies of %x of type %s", obj.Ref().Hash, obj.Type())
	}
}

func TestASweepStoppedBetweenChunksIsFinishedByTheNextCollection(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	ctx := context.Background()

	reachable, unreachable := makeGCTestData(t)
	putAll(t, store, append(append([]*proto.Object{}, reachable...), unreachable...))

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	defer func(n int) { chunkRecords = n; rewriteAfterChunk = nil }(chunkRecords)
	chunkRecords = 200

	var chunks int
	stopped := errors.New("stopped")
	rewriteAfterChunk = func(chunk int) error {
		chunks++
		if chunk == 1 {
			return stopped
		}

		return nil
	}

	before, _, err := store.archiveNames()
	require.NoError(t, err)

	_, err = store.Collect(ctx, gcOptions(t, 48*time.Hour))
	require.ErrorIs(t, err, stopped)
	require.Equal(t, 2, chunks, "the sweep stopped after its second chunk")

	after, _, err := store.archiveNames()
	require.NoError(t, err)
	require.NotEqual(t, before, after, "the finished chunks retired their inputs")

	requireStored(t, store, reachable, true)
	requireOneCopy(t, store, reachable)

	rewriteAfterChunk = nil

	finished, err := store.Collect(ctx, gcOptions(t, 72*time.Hour))
	require.NoError(t, err)
	require.Empty(t, finished.SweepSkipped)
	require.Greater(t, finished.Swept, 0)

	requireStored(t, store, unreachable, false)
	requireStored(t, store, reachable, true)
	requireOneCopy(t, store, reachable)

	// the tombstones go once spent
	_, err = store.Collect(ctx, gcOptions(t, 96*time.Hour))
	require.NoError(t, err)

	settled, err := store.Collect(ctx, gcOptions(t, 120*time.Hour))
	require.NoError(t, err)
	require.Zero(t, settled.DeadObjects)
	require.EqualValues(t, len(reachable), settled.Marked, "no copy of the stopped sweep is left over")

	require.NoError(t, store.Close())
}

func TestASweepStreamsArchivesTooLargeToReadAhead(t *testing.T) {
	const archiveSize = 2 * prefetchLimit

	store := newTestStore(t, t.TempDir(), WithMaxSize(archiveSize))
	ctx := context.Background()

	var kept []*proto.Object
	for range 2 {
		for i := range archiveSize / (64 << 10) {
			data := make([]byte, 64<<10)
			_, _ = rand.Read(data)

			obj := proto.NewObject(&proto.Blob{Data: data})
			require.NoError(t, store.Put(ctx, obj))

			if i%10 == 0 {
				kept = append(kept, obj)
			}
		}

		require.NoError(t, store.Flush())
	}

	putAll(t, store, makeChain(kept)[len(kept):])

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	defer debug.SetGCPercent(debug.SetGCPercent(10))
	runtime.GC()

	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	base := stats.HeapAlloc

	peak := base
	done := make(chan struct{})
	sampled := make(chan struct{})

	go func() {
		defer close(sampled)

		var stats runtime.MemStats
		for {
			runtime.ReadMemStats(&stats)
			peak = max(peak, stats.HeapAlloc)

			select {
			case <-done:
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()

	report, err := store.Collect(ctx, gcOptions(t, 48*time.Hour))
	close(done)
	<-sampled

	require.NoError(t, err)
	require.Greater(t, report.Swept, 1)
	require.Less(t, peak-base, uint64(archiveSize/2), "the sweep held %d MiB", (peak-base)>>20)

	requireStored(t, store, kept, true)
	require.NoError(t, store.Close())
}

// skewedIndex is an index whose shared clock runs an hour behind this
// machine's.
type skewedIndex struct {
	*InMemoryIndex
}

func (skewedIndex) SharedNow(context.Context) (time.Time, error) {
	return time.Now().Add(-time.Hour), nil
}

func TestSessionsAreStampedByTheSharedClock(t *testing.T) {
	index := skewedIndex{NewInMemoryIndex()}
	store, err := NewPackStorage(WithArchiveStorage(newLocal(t.TempDir())), WithArchiveIndex(index))
	require.NoError(t, err)
	require.NoError(t, store.Open())

	ctx := context.Background()

	session := &backup.Session{AgentID: "a", Set: "world"}
	_, err = store.BeginSession(ctx, session)
	require.NoError(t, err)

	begun, err := index.GetSession(session.ID)
	require.NoError(t, err)
	require.WithinDuration(t, time.Now().Add(-time.Hour), begun.Started, time.Minute)

	require.NoError(t, store.Close())
}

func TestACollectionRootsTheCommitsAnotherProcessStored(t *testing.T) {
	base := t.TempDir()
	index := NewInMemoryIndex()
	ctx := context.Background()

	open := func() *PackStorage {
		store, err := NewPackStorage(WithArchiveStorage(newLocal(base)), WithArchiveIndex(index), WithMaxSize(256*1024))
		require.NoError(t, err)
		require.NoError(t, store.Open())

		return store
	}

	collector := open()
	writer := open()

	chunk := proto.NewObject(&proto.Blob{Data: bytes.Repeat([]byte("c"), 4096)})
	putAll(t, collector, []*proto.Object{chunk})

	file := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Ref: chunk.Ref(), Length: 4096}}})
	tree := treeOf([]*proto.Object{file})
	commit := proto.NewObject(&proto.Commit{Tree: tree.Ref(), Timestamp: 1})
	putAll(t, writer, []*proto.Object{file, tree, commit})

	for _, ahead := range []time.Duration{0, 48 * time.Hour, 96 * time.Hour} {
		_, err := collector.Collect(ctx, gcOptions(t, ahead))
		require.NoError(t, err)
	}

	requireStored(t, collector, []*proto.Object{chunk, file, tree, commit}, true)

	require.NoError(t, writer.Close())
	require.NoError(t, collector.Close())
}

func TestACollectionCoversWhatAnotherProcessCommittedAfterItLoadedIt(t *testing.T) {
	base := t.TempDir()
	index := NewInMemoryIndex()
	ctx := context.Background()

	open := func() *PackStorage {
		store, err := NewPackStorage(WithArchiveStorage(newLocal(base)), WithArchiveIndex(index))
		require.NoError(t, err)
		require.NoError(t, store.Open())

		return store
	}

	collector := open()
	writer := open()

	sctx, err := writer.BeginSession(ctx, &backup.Session{AgentID: "a", Set: "world"})
	require.NoError(t, err)

	objects := makeTestData(t, 10)
	for _, obj := range objects {
		require.NoError(t, writer.Put(sctx, obj))
	}

	// the half-written archive is the writer's, so a collection leaves it be
	_, err = collector.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	collector.mtx.RLock()
	require.Empty(t, collector.archives)
	collector.mtx.RUnlock()

	require.NoError(t, writer.Flush())

	// now finalized and pending, a collection loads it but does not cover it
	report, err := collector.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)
	require.Zero(t, report.Archives)

	require.NoError(t, writer.Put(sctx, commitOver(objects[0].Ref())))
	require.NoError(t, writer.EndSession(sctx))

	report, err = collector.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)
	require.Equal(t, 2, report.Archives, "the archive it had pending and the one holding the commit")

	require.NoError(t, writer.Close())
	require.NoError(t, collector.Close())
}

func TestCollectKeepsPoliciesWithoutWhatTheyName(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	ctx := context.Background()

	reachable, unreachable := makeGCTestData(t)
	named := unreachable[len(unreachable)-1]
	policy := proto.NewObject(&proto.Policy{Sequence: 1, Scope: &proto.Policy_Commit{Commit: &proto.CommitScope{Commit: named.Ref(), DeletedAtNs: 1}}})
	putAll(t, store, append(append([]*proto.Object{policy}, reachable...), unreachable...))

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)
	_, err = store.Collect(ctx, gcOptions(t, 48*time.Hour))
	require.NoError(t, err)

	requireStored(t, store, []*proto.Object{policy}, true)
	requireStored(t, store, []*proto.Object{named}, false)
	require.NoError(t, store.Close())
}

func TestACollectionTakesNoRootFromTheUntombstoneOfACommitThatIsGone(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	blobs := makeTestData(t, 10)
	putAll(t, store, blobs)

	chain := makeChain(blobs)
	own := chain[len(blobs):]
	commit := own[len(own)-1]

	sctx, _ := beginSession(t, store, "agent-a")
	for _, obj := range own {
		require.NoError(t, store.Put(sctx, obj))
	}

	require.NoError(t, store.Delete(ctx, commit.Ref()))
	require.NoError(t, store.Flush())

	for _, ahead := range []time.Duration{0, 48 * time.Hour} {
		_, err := store.Collect(ctx, gcOptions(t, ahead))
		require.NoError(t, err)
	}

	requireStored(t, store, []*proto.Object{commit}, false)
	requireUntombed(t, store, []*proto.Object{commit}, true)

	report, err := store.Collect(ctx, gcOptions(t, 96*time.Hour))
	require.NoError(t, err)
	require.Zero(t, report.Roots)
}

func TestACollectionReportsARevivedCommitThatIsGone(t *testing.T) {
	var logged bytes.Buffer
	store, err := NewPackStorage(
		WithArchiveStorage(newLocal(t.TempDir())),
		WithArchiveIndex(NewInMemoryIndex()),
		WithMaxSize(256*1024),
		WithLogger(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelError}))),
	)
	require.NoError(t, err)
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	chain := makeChain(makeTestData(t, 10))
	commit := chain[len(chain)-1]
	putAll(t, store, chain)

	require.NoError(t, store.Delete(ctx, commit.Ref()))
	require.NoError(t, store.Flush())

	for _, ahead := range []time.Duration{0, 48 * time.Hour} {
		_, err := store.Collect(ctx, gcOptions(t, ahead))
		require.NoError(t, err)
	}

	requireStored(t, store, []*proto.Object{commit}, false)
	require.Empty(t, logged.String())

	require.NoError(t, store.Delete(ctx, proto.TombstoneRef(commit.Ref())))
	require.NoError(t, store.Flush())

	report, err := store.Collect(ctx, gcOptions(t, 96*time.Hour))
	require.NoError(t, err)
	require.Zero(t, report.Roots)
	require.Contains(t, logged.String(), fmt.Sprintf("ref=%x", commit.Ref().Hash))
}

func TestAnErasureFlagsOnlyWhatNoGroupReaches(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	sharedBlobs := makeTestData(t, 5)
	shared := makeGCFiles(sharedBlobs)
	sharedTree := treeOf(shared)
	sharedCommit := proto.NewObject(&proto.Commit{Tree: sharedTree.Ref(), Timestamp: 1, BackupSet: "first"})
	first := append(append(append([]*proto.Object{}, sharedBlobs...), shared...), sharedTree, sharedCommit)
	putAll(t, store, first)

	other := makeChain(makeTestData(t, 5))
	putAll(t, store, other)

	ownBlobs := makeTestData(t, 5)
	own := makeGCFiles(ownBlobs)
	erasedTree := treeOf(append(append([]*proto.Object{}, shared...), own...))
	erased := proto.NewObject(&proto.Commit{Tree: erasedTree.Ref(), Timestamp: 2, BackupSet: "erased"})
	putAll(t, store, append(append(append([]*proto.Object{}, ownBlobs...), own...), erasedTree, erased))

	require.NoError(t, store.Erase(ctx, erased.Ref()))
	require.NoError(t, store.Flush())

	opts := gcOptions(t, 0)
	opts.Owner = func(root []byte) Attribution {
		switch {
		case bytes.Equal(root, sharedCommit.Ref().Hash):
			return Attribution{Group: 1, Set: 1}
		case bytes.Equal(root, other[len(other)-1].Ref().Hash):
			return Attribution{Group: 2, Set: 2}
		}

		return Attribution{}
	}

	_, err := store.Collect(ctx, opts)
	require.NoError(t, err)

	for _, obj := range first {
		a, _, err := store.indexLocation(ctx, obj.Ref())
		require.NoError(t, err)
		require.Falsef(t, a.gcResult().Erase, "archive %s holds only what the first group reaches", a.name)
	}

	a, _, err := store.indexLocation(ctx, erased.Ref())
	require.NoError(t, err)
	require.True(t, a.gcResult().Erase)
}
