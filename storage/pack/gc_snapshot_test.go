package pack

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// newScenarioStore holds every kind of tombstone a collection reads: one a
// compaction carried, those a collection condemned with, and the
// un-tombstones of a session that committed over a condemned chain.
func newScenarioStore(t *testing.T) (*PackStorage, []*proto.Object) {
	t.Helper()

	store, err := NewPackStorage(
		WithArchiveStorage(newLocal(t.TempDir())),
		WithArchiveIndex(NewInMemoryIndex()),
		WithMaxSize(256*1024),
		WithCompaction(CompactionConfig{MinimumCandidates: 0}),
	)
	require.NoError(t, err)
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()

	gone, _ := retiredChain(t, store)
	compact(t, context.Background(), store)

	chain := unreferencedChain(t, store)
	sctx, _ := beginSession(t, store, "agent-a")
	requirePresent(t, store, chain[:len(chain)-1], true)

	_, err = store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	require.NoError(t, store.Put(sctx, chain[len(chain)-1]))
	require.NoError(t, store.Delete(ctx, gone[0].Ref()))
	require.NoError(t, store.Flush())

	return store, chain
}

// snapshotRun takes the snapshot a collection would and collects its roots.
func snapshotRun(t *testing.T, store *PackStorage, prepare func(r *gcRun)) *gcRun {
	t.Helper()

	ctx := context.Background()

	prev, err := loadGCState(store.storage)
	require.NoError(t, err)

	opts := gcOptions(t, 48*time.Hour).withDefaults()
	r := newGCRun(store, opts, prev, newGCReads())

	require.NoError(t, r.takeSnapshot(ctx))
	if prepare != nil {
		prepare(r)
	}
	require.NoError(t, r.collectRoots(ctx))

	return r
}

// readTombstonesOneByOne fills the run's tombstone headers with a read of
// each tombstone on its own.
func readTombstonesOneByOne(t *testing.T) func(r *gcRun) {
	return func(r *gcRun) {
		for _, ga := range r.order {
			tombs := make(map[int]tombHeader)
			ga.each(func(pos int, rec *IndexRecord) {
				if proto.ObjectType(rec.Type) != proto.ObjectType_TOMBSTONE {
					return
				}

				hdr, err := ga.a.readHeader(rec)
				require.NoError(t, err)

				tombs[pos] = tombHeaderOf(hdr)
			})

			r.reads.tombs[ga.a.name] = tombs
		}
	}
}

// markOf marks the run and returns what it found reachable in each
// archive and what it would condemn.
func markOf(t *testing.T, r *gcRun) (map[string]string, map[refKey]Version) {
	t.Helper()

	live, err := r.mark(context.Background())
	require.NoError(t, err)
	defer live.close()

	_, err = r.merge(live)
	require.NoError(t, err)

	marked := make(map[string]string)
	condemned := make(map[refKey]Version)

	for _, ga := range r.order {
		marked[ga.a.name] = ga.cur.String()

		ga.each(func(pos int, rec *IndexRecord) {
			if !ga.cur.Test(uint(pos)) && r.uncondemned(ga, rec) {
				newestAt(condemned, keyOf(rec.Sum[:]), ga.a.version(*rec))
			}
		})
	}

	return marked, condemned
}

func TestBatchedTombstoneReadsRootWhatReadingEachDoes(t *testing.T) {
	store, _ := newScenarioStore(t)

	batched := snapshotRun(t, store, nil)
	single := snapshotRun(t, store, readTombstonesOneByOne(t))

	require.NotEmpty(t, batched.tombstones)
	require.NotEmpty(t, batched.untombed, "the session's un-tombstones take objects back")
	require.NotEmpty(t, batched.condemning)

	sortRoots(batched.roots)
	sortRoots(single.roots)
	require.Equal(t, single.roots, batched.roots)
	require.Equal(t, single.targets, batched.targets)
	require.Equal(t, single.newestTomb, batched.newestTomb)
	require.Equal(t, single.condemning, batched.condemning)
	require.Equal(t, single.untombed, batched.untombed)
	require.Equal(t, single.oldestCopy, batched.oldestCopy)
	require.Equal(t, single.spent, batched.spent)
	require.Equal(t, single.erased, batched.erased)
	require.Equal(t, single.tombTimes, batched.tombTimes)
	require.Len(t, batched.tombstones, len(single.tombstones))

	batchedMarked, batchedCondemned := markOf(t, batched)
	singleMarked, singleCondemned := markOf(t, single)
	require.Equal(t, singleMarked, batchedMarked)
	require.Equal(t, singleCondemned, batchedCondemned)
}

func TestBatchedHeaderReadsOfAPendingSessionMatchReadingEach(t *testing.T) {
	store, _ := newScenarioStore(t)
	ctx := context.Background()

	chain := unreferencedChain(t, store)
	sctx, _ := beginSession(t, store, "agent-b")
	requirePresent(t, store, chain[:len(chain)-1], true)

	_, err := store.Collect(ctx, gcOptions(t, 72*time.Hour))
	require.NoError(t, err)

	var checked int

	// the un-tombstones are stored, the session not yet committed
	commitAfterResurrect = func() {
		commitAfterResurrect = nil

		r := snapshotRun(t, store, nil)
		require.NotEmpty(t, r.pending)

		var tombs []placed
		for _, a := range r.pending {
			require.NoError(t, scanArchive(a, func(_ int, rec *IndexRecord) error {
				if proto.ObjectType(rec.Type) == proto.ObjectType_TOMBSTONE {
					held := *rec
					tombs = append(tombs, placed{a: a, rec: &held})
				}

				return nil
			}, nil, nil))
		}
		require.NotEmpty(t, tombs)

		headers, err := r.readHeaders(ctx, tombs, false)
		require.NoError(t, err)

		for i, tomb := range tombs {
			hdr, err := tomb.a.readHeader(tomb.rec)
			require.NoError(t, err)
			require.Equal(t, hdr.TombstoneFor.Hash, headers[i].TombstoneFor.Hash)
			checked++
		}

		require.True(t, r.untombed[keyOf(chain[len(chain)-1].Ref().Hash)], "the pending un-tombstone roots the commit")
	}
	t.Cleanup(func() { commitAfterResurrect = nil })

	require.NoError(t, store.Put(sctx, chain[len(chain)-1]))
	require.NotZero(t, checked)
}

// locatingIndex counts the objects looked up in the index.
type locatingIndex struct {
	ArchiveIndex
	calls atomic.Int64
}

func (l *locatingIndex) LocateObject(ref *proto.Ref, scope Scope, exclude ...string) (IndexLocation, error) {
	l.calls.Add(1)

	return l.ArchiveIndex.LocateObject(ref, scope, exclude...)
}

func TestMarkingFromTheSnapshotMarksWhatMarkingThroughTheIndexDoes(t *testing.T) {
	store, _ := newScenarioStore(t)

	snapshot := snapshotRun(t, store, nil)
	indexed := snapshotRun(t, store, func(r *gcRun) { r.located = newPlaces(nil) })

	require.NotZero(t, snapshot.located.count())

	locating := &locatingIndex{ArchiveIndex: store.index}
	store.index = locating
	t.Cleanup(func() { store.index = locating.ArchiveIndex })

	snapshotMarked, snapshotCondemned := markOf(t, snapshot)
	require.Zero(t, locating.calls.Load(), "the snapshot places everything the mark reads")

	indexedMarked, indexedCondemned := markOf(t, indexed)
	require.NotZero(t, locating.calls.Load())
	require.Equal(t, indexedMarked, snapshotMarked)
	require.Equal(t, indexedCondemned, snapshotCondemned)
}

func TestTheCheckBeforeASweepHaltsOnAPinStoredAfterTheSnapshot(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	putAll(t, store, makeChain(makeTestData(t, 20)[10:]))
	chain := unreferencedChain(t, store)
	root := chain[len(chain)-2]

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	gcAfterBatch = func(int) error {
		gcAfterBatch = nil
		if err := store.Put(ctx, proto.NewObject(&proto.Pin{Target: root.Ref(), ReceivedAtNs: 1})); err != nil {
			return err
		}

		return store.Flush()
	}
	t.Cleanup(func() { gcAfterBatch = nil })

	_, err = store.Collect(ctx, gcOptions(t, 48*time.Hour))
	require.ErrorIs(t, err, ErrHalted)
	requireStored(t, store, chain[:len(chain)-1], true)
}

func owned(r *gcRun) {
	r.opts.Owner = func(root []byte) Attribution {
		return Attribution{Group: 1 + int64(root[0]%2), Set: 1 + int64(root[0]%3)}
	}
}

// requireSameMark marks both runs and requires them to mark, condemn and
// attribute the same.
func requireSameMark(t *testing.T, want, got *gcRun) {
	t.Helper()

	gotMarked, gotCondemned := markOf(t, got)
	wantMarked, wantCondemned := markOf(t, want)
	require.Equal(t, wantMarked, gotMarked)
	require.Equal(t, wantCondemned, gotCondemned)
	require.Equal(t, want.setBytes, got.setBytes)
	require.Equal(t, want.setDeduplicated, got.setDeduplicated)
	require.Equal(t, want.setAlone, got.setAlone)
	require.Equal(t, want.setExclusive, got.setExclusive)
	require.Equal(t, want.setDeduplicatedAlone, got.setDeduplicatedAlone)
	require.Equal(t, want.unattributed, got.unattributed)
}

// markBlobRootsBothWays marks the store's snapshot adding the blobs among
// the roots unread, and again reading them, and requires the same outcome;
// it returns how many roots were blobs.
func markBlobRootsBothWays(t *testing.T, store *PackStorage) int {
	t.Helper()

	unread := snapshotRun(t, store, owned)
	read := snapshotRun(t, store, owned)
	read.blobs = make(map[refKey]bool)

	blobRoots := 0
	for _, root := range unread.roots {
		if unread.blobs[root.key] {
			blobRoots++
		}
	}

	requireSameMark(t, read, unread)

	return blobRoots
}

func TestUnreadBlobRootsMarkWhatReadingThemDoes(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	putAll(t, store, makeChain(makeTestData(t, 20)))

	// condemned, so the session that commits over them takes them back
	blobs := makeTestData(t, 10)
	putAll(t, store, blobs)

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	chain := makeChain(blobs)
	own := chain[len(blobs):]
	sctx, _ := beginSession(t, store, "agent-a")
	for _, obj := range own[:len(own)-1] {
		require.NoError(t, store.Put(sctx, obj))
	}

	pending := 0
	commitAfterResurrect = func() {
		commitAfterResurrect = nil
		pending = markBlobRootsBothWays(t, store)
	}
	t.Cleanup(func() { commitAfterResurrect = nil })

	require.NoError(t, store.Put(sctx, own[len(own)-1]))
	require.Equal(t, len(blobs), pending, "a live session's un-tombstones root the blobs")

	require.Equal(t, len(blobs), markBlobRootsBothWays(t, store), "a committed session's un-tombstones root the blobs")
}

func TestReadingSpansInParallelMarksWhatReadingThemInTurnDoes(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	for range 4 {
		putAll(t, store, makeChain(makeTestData(t, 30)))
	}
	retiredChain(t, store)

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	inTurn := snapshotRun(t, store, func(r *gcRun) {
		owned(r)
		r.opts.Readers = 1
	})
	parallel := snapshotRun(t, store, func(r *gcRun) {
		owned(r)
		r.opts.Readers = 32
	})
	require.Greater(t, len(parallel.order), 4)

	requireSameMark(t, inTurn, parallel)
}
