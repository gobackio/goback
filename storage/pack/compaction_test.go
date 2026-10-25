package pack

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

// askingIndex counts the refs LocateCopies is asked for.
type askingIndex struct {
	*InMemoryIndex
	asked atomic.Int64
}

func (a *askingIndex) LocateCopies(refs []*proto.Ref, scope Scope) (map[string][]IndexLocation, error) {
	a.asked.Add(int64(len(refs)))

	return a.InMemoryIndex.LocateCopies(refs, scope)
}

func compact(t testing.TB, ctx context.Context, store *PackStorage) *CompactReport {
	t.Helper()

	report, err := store.Compact(ctx)
	require.NoError(t, err)

	return report
}

func TestARewriteLooksEachObjectUpOncePerChunk(t *testing.T) {
	objects := makeTestData(t, numObjects)

	for _, chunk := range []int{0, 1} {
		index := &askingIndex{InMemoryIndex: NewInMemoryIndex()}
		store := newTestStore(t, t.TempDir(), WithArchiveIndex(index), WithMaxSize(64<<20),
			WithCompaction(CompactionConfig{Chunk: chunk}))

		for range 3 {
			for _, object := range objects {
				require.NoError(t, store.Put(context.Background(), object))
			}

			require.NoError(t, store.Flush())
		}

		index.asked.Store(0)
		compact(t, context.Background(), store)

		if chunk == 0 {
			require.EqualValues(t, numObjects, index.asked.Load(), "one chunk asks for each object once")
		}

		for i, object := range objects {
			copies, err := index.LocateCopies([]*proto.Ref{object.Ref()}, Scope{})
			require.NoError(t, err)
			require.Len(t, copies[string(object.Ref().Hash)], 1, "chunk %d, object %d", chunk, i)

			got, err := store.Get(context.Background(), object.Ref())
			require.NoError(t, err)
			require.Equal(t, object.Bytes(), got.Bytes())
		}

		require.NoError(t, store.Close())
	}
}

func TestCompactionMergesOnlySmallArchivesAndNeverItsOwnOutput(t *testing.T) {
	const small = 64 << 10

	store := newTestStore(t, t.TempDir(), WithMaxSize(4<<20),
		WithCompaction(CompactionConfig{Small: small, Batch: 1 << 20, MinimumCandidates: 1000, Workers: 16}))
	defer store.Close()

	objects := makeTestData(t, 300)

	for i := 0; i < 200; i += 5 {
		for _, object := range objects[i : i+5] {
			require.NoError(t, store.Put(context.Background(), object))
		}
		require.NoError(t, store.Flush())
	}

	for _, object := range objects[200:] {
		require.NoError(t, store.Put(context.Background(), object))
	}
	require.NoError(t, store.Flush())

	sizes := func() map[string]uint64 {
		store.mtx.RLock()
		defer store.mtx.RUnlock()

		out := make(map[string]uint64)
		for _, a := range store.archives {
			out[a.name] = a.size
		}

		return out
	}

	var objectsIn int
	store.mtx.RLock()
	for _, a := range store.archives {
		if a.size < small {
			idx, err := a.getIndex()
			require.NoError(t, err)
			objectsIn += len(idx)
		}
	}
	store.mtx.RUnlock()

	before := sizes()

	var large string
	var candidates int
	var candidateBytes uint64
	for name, size := range before {
		if size >= small {
			large = name
		} else {
			candidates++
			candidateBytes += size
		}
	}
	require.NotEmpty(t, large)

	report := compact(t, context.Background(), store)

	merged := sizes()
	require.Contains(t, merged, large, "an archive that is not small stays")

	var written int
	var writtenBytes uint64
	for name, size := range merged {
		require.GreaterOrEqual(t, size, uint64(small), "%s came out small", name)

		if _, ok := before[name]; !ok {
			written++
			writtenBytes += size
		}
	}

	require.Equal(t, candidates, report.Candidates)
	require.Equal(t, candidateBytes, report.CandidateBytes)
	require.Equal(t, candidates, report.Rewritten)
	require.Equal(t, candidateBytes, report.ReadBytes)
	require.Equal(t, written, report.Written)
	require.Equal(t, writtenBytes, report.WrittenBytes)
	require.EqualValues(t, objectsIn, report.Moved+report.Superseded)
	require.EqualValues(t, max(int64(candidateBytes)-int64(writtenBytes), 0), report.ReclaimedBytes)

	report = compact(t, context.Background(), store)
	require.Equal(t, merged, sizes(), "what compaction wrote is not merged again")
	require.Zero(t, report.Rewritten)
	require.Zero(t, report.Written)

	for _, object := range objects {
		got, err := store.Get(context.Background(), object.Ref())
		require.NoError(t, err)
		require.Equal(t, object.Bytes(), got.Bytes())
	}
}

func TestAClassedRewriteChunksWithinItsRecordAndOutputBounds(t *testing.T) {
	defer func(records, outputs int) { chunkRecords, chunkOutputs = records, outputs }(chunkRecords, chunkOutputs)
	chunkRecords, chunkOutputs = 10, 2

	const small = 100

	c := newClasses(nil)
	class := func(set int64) int32 { return c.id(outputClass{Owner: Attribution{Group: 1, Set: set}}) }

	held := make(map[string]*classed)
	candidate := func(name string, records int, sizes map[int32]uint64) *archive {
		held[name] = &classed{Class: make([]int32, records), Bytes: sizes}
		return &archive{name: name}
	}

	group := &compactionGroup{classes: c, classed: func(a *archive) *classed { return held[a.name] }}
	group.candidates = []*archive{
		candidate("a", 4, map[int32]uint64{class(1): small}),
		candidate("b", 4, map[int32]uint64{class(1): small}),
		candidate("c", 4, map[int32]uint64{class(1): small}),
		candidate("d", 2, map[int32]uint64{class(2): small}),
		candidate("e", 2, map[int32]uint64{class(3): small}),
		candidate("f", 30, map[int32]uint64{class(4): small}),
		{name: "unclassed"},
		candidate("g", 1, map[int32]uint64{class(5): small, class(6): small, class(7): small}),
	}

	names := func(chunks [][]*archive) [][]string {
		var out [][]string
		for _, chunk := range chunks {
			var names []string
			for _, a := range chunk {
				names = append(names, a.name)
			}

			out = append(out, names)
		}

		return out
	}

	require.Equal(t, [][]string{{"a", "b"}, {"c", "d"}, {"e"}, {"f", "unclassed"}, {"g"}}, names(group.chunks(1000, small)),
		"a chunk ends before it would exceed either bound, and an archive beyond one takes a chunk of its own")
	require.Equal(t, [][]string{{"a"}, {"b"}, {"c"}, {"d"}, {"e"}, {"f"}, {"unclassed"}, {"g"}}, names(group.chunks(1, small)))

	group.classed = nil
	require.Equal(t, [][]string{{"a", "b", "c"}, {"d", "e", "f"}, {"unclassed", "g"}}, names(group.chunks(3, small)),
		"unclassed candidates are chunked by count alone")
}

func TestARewriteTrustsOnlyAReachableCopyOutsideItsGroup(t *testing.T) {
	rw := &rewrite{
		inGroup: map[string]bool{"candidate": true},
		group: &compactionGroup{marked: func(loc *IndexLocation) bool {
			return loc.Archive != "unreachable"
		}},
	}

	at := func(archives ...string) []IndexLocation {
		var copies []IndexLocation
		for _, archive := range archives {
			copies = append(copies, IndexLocation{Archive: archive})
		}

		return copies
	}

	require.Empty(t, rw.standIns(nil))
	require.Empty(t, rw.standIns(at("candidate")), "a copy the rewrite drops is no copy")
	require.Empty(t, rw.standIns(at("candidate", "unreachable")), "an unreachable copy may be swept next")
	require.Equal(t, at("reachable"), rw.standIns(at("unreachable", "reachable")))
}

func TestACompactionKeepsTheNewestTombstoneOfAnObject(t *testing.T) {
	store := newTestStore(t, t.TempDir(), WithCompaction(CompactionConfig{Small: 32 << 10, MinimumCandidates: 0}))
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	obj := makeTestData(t, 1)
	ballast := make([]byte, 64<<10)
	_, err := rand.Read(ballast)
	require.NoError(t, err)

	for i, present := range []bool{true, false, true, false} {
		if present {
			require.NoError(t, store.Put(ctx, obj[0]))
		} else {
			require.NoError(t, store.Delete(ctx, obj[0].Ref()))
		}

		// the first tombstone and the copy after it sit in archives too
		// large to compact
		if i == 1 || i == 2 {
			ballast[0] = byte(i)
			require.NoError(t, store.Put(ctx, proto.NewObject(&proto.Blob{Data: ballast})))
		}

		require.NoError(t, store.Flush())
		requirePresent(t, store, obj, present)
	}

	compact(t, ctx, store)
	requirePresent(t, store, obj, false)

	compact(t, ctx, store)
	requirePresent(t, store, obj, false)
}

// refusingRetirement refuses to store retirements while refusing is set.
type refusingRetirement struct {
	*localArchiveStorage
	refusing bool
}

func (r *refusingRetirement) CreateNew(name string, data []byte) error {
	if r.refusing && strings.HasSuffix(name, RetirementExt) {
		return errors.New("refused")
	}

	return r.localArchiveStorage.CreateNew(name, data)
}

// deletingIndex counts the calls to DeleteArchives, and fails them while
// refusing is set.
type deletingIndex struct {
	*InMemoryIndex
	calls    atomic.Int64
	refusing atomic.Bool
}

func (d *deletingIndex) DeleteArchives(names []string) error {
	d.calls.Add(1)

	if d.refusing.Load() {
		return errors.New("database is locked")
	}

	return d.InMemoryIndex.DeleteArchives(names)
}

func TestTheRowsOfRetiredArchivesGoOnceTheIndexTakesTheDeletion(t *testing.T) {
	base := t.TempDir()
	index := &deletingIndex{InMemoryIndex: NewInMemoryIndex()}
	ctx := context.Background()

	open := func() *PackStorage {
		store, err := NewPackStorage(WithArchiveStorage(newLocal(base)), WithArchiveIndex(index), WithCompaction(CompactionConfig{MinimumCandidates: 0}))
		require.NoError(t, err)
		require.NoError(t, store.Open())

		return store
	}

	requireIndexed := func(names []string, indexed bool, msg string) {
		t.Helper()

		for _, name := range names {
			_, found, err := index.LookupArchive(name)
			require.NoError(t, err)
			require.Equal(t, indexed, found, "%s: %s", msg, name)
		}
	}

	store := open()

	for restart, again := range []func(){
		func() { require.NoError(t, store.refreshArchives()) },
		func() {
			require.NoError(t, store.Close())
			store = open()
		},
	} {
		objects := makeTestData(t, 3)
		for _, obj := range objects {
			require.NoError(t, store.Put(ctx, obj))
			require.NoError(t, store.Flush())
		}

		var inputs []string
		for _, a := range store.archives {
			inputs = append(inputs, a.name)
		}

		index.refusing.Store(true)
		compact(t, ctx, store)
		requireIndexed(inputs, true, "the rows stay while the index refuses")

		index.refusing.Store(false)
		again()
		requireIndexed(inputs, false, fmt.Sprintf("the rows go (restart=%d)", restart))

		for _, obj := range objects {
			requirePresent(t, store, []*proto.Object{obj}, true)
		}
	}

	require.NoError(t, store.Close())
}

// batchObserved collects the batches an ArchiveBatchObserver hears.
type batchObserved struct {
	observed
	batches [][]string
}

func (b *batchObserved) ArchivesDeleted(names []string) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	b.batches = append(b.batches, names)
}

func TestARewriteRetiresItsInputsTogetherOrNotAtAll(t *testing.T) {
	for _, refusing := range []bool{false, true} {
		t.Run(fmt.Sprintf("refusing=%v", refusing), func(t *testing.T) {
			storage := &refusingRetirement{localArchiveStorage: newLocal(t.TempDir())}
			index := &deletingIndex{InMemoryIndex: NewInMemoryIndex()}
			seen := &batchObserved{observed: *newObserved()}

			store, err := NewPackStorage(WithArchiveStorage(storage), WithArchiveIndex(index), WithMaxSize(64<<20),
				WithArchiveObserver(seen), WithCompaction(CompactionConfig{MinimumCandidates: 1}))
			require.NoError(t, err)
			require.NoError(t, store.Open())
			defer store.Close()

			objects := makeTestData(t, 40)
			for i := 0; i < len(objects); i += 10 {
				for _, object := range objects[i : i+10] {
					require.NoError(t, store.Put(context.Background(), object))
				}

				require.NoError(t, store.Flush())
			}

			var inputs []string
			for _, a := range store.archives {
				inputs = append(inputs, a.name)
			}
			require.Len(t, inputs, 4)

			storage.refusing = refusing
			compact(t, context.Background(), store)

			retired, err := store.retiredArchives()
			require.NoError(t, err)

			for _, name := range inputs {
				_, found, err := index.LookupArchive(name)
				require.NoError(t, err)
				require.Equal(t, refusing, found, "%s keeps its rows only when its retirement failed", name)
				require.Equal(t, !refusing, retired[name], "%s is quarantined", name)

				if !refusing {
					_, err = storage.Open(name + CommittedExt)
					require.True(t, notExist(err), "%s lost its committed marker", name)
				}
			}

			require.Empty(t, seen.deleted, "a batch observer hears no single deletions")

			if refusing {
				require.Zero(t, index.calls.Load())
				require.Empty(t, seen.batches)
			} else {
				require.EqualValues(t, 1, index.calls.Load(), "the index forgets the chunk's inputs at once")
				require.Len(t, seen.batches, 1)
				require.ElementsMatch(t, inputs, seen.batches[0])
			}

			for _, object := range objects {
				got, err := store.Get(context.Background(), object.Ref())
				require.NoError(t, err)
				require.Equal(t, object.Bytes(), got.Bytes())
			}
		})
	}
}

func TestForgettingDeletedArchivesKeepsWhatAnotherArchiveHolds(t *testing.T) {
	store, cache := newCachedStore(t)
	defer store.Close()

	ctx := context.Background()
	gone, kept := treeOf(makeGCFiles(makeTestData(t, 1))), treeOf(makeGCFiles(makeTestData(t, 2)))
	record := func(obj *proto.Object) IndexRecord {
		rec := IndexRecord{Type: uint32(obj.Type())}
		copy(rec.Sum[:], obj.Ref().Hash)

		return rec
	}

	require.NoError(t, store.index.IndexArchive(ArchiveInfo{Name: "a"}, IndexFile{record(gone), record(kept)}))
	require.NoError(t, store.index.IndexArchive(ArchiveInfo{Name: "b"}, IndexFile{record(kept)}))
	require.NoError(t, cache.Put(ctx, gone))
	require.NoError(t, cache.Put(ctx, kept))

	require.NoError(t, store.index.DeleteArchives([]string{"a"}))
	store.forgetCached(ctx, []IndexFile{{record(gone), record(kept)}})

	requireCached(t, cache, []*proto.Object{gone}, false)
	requireCached(t, cache, []*proto.Object{kept}, true)
}

// failingOpen fails to open the archive file of one archive.
type failingOpen struct {
	*localArchiveStorage
	fail  string
	opens atomic.Int64
}

func (f *failingOpen) Open(name string) (File, error) {
	if name == f.fail+ArchiveSuffix {
		return nil, errors.New("unreadable")
	}

	if strings.HasSuffix(name, ArchiveSuffix) {
		f.opens.Add(1)
	}

	return f.localArchiveStorage.Open(name)
}

func TestPrefetchHandsTheInputsOverInOrderWithinItsBudget(t *testing.T) {
	storage := &failingOpen{localArchiveStorage: newLocal(t.TempDir())}
	store, err := NewPackStorage(WithArchiveStorage(storage), WithArchiveIndex(NewInMemoryIndex()), WithMaxSize(64<<20))
	require.NoError(t, err)
	require.NoError(t, store.Open())
	defer store.Close()

	objects := makeTestData(t, 50)
	for i := 0; i < len(objects); i += 10 {
		for _, object := range objects[i : i+10] {
			require.NoError(t, store.Put(context.Background(), object))
		}

		require.NoError(t, store.Flush())
	}

	inputs := append([]*archive(nil), store.archives...)
	require.Len(t, inputs, 5)

	var largest uint64
	want := make([][]byte, len(inputs))
	for i, a := range inputs {
		largest = max(largest, a.size)
		want[i], err = a.readAll()
		require.NoError(t, err)
	}

	storage.fail = inputs[2].name
	storage.opens.Store(0)

	// room for one input at a time: the next is read only once the one
	// before is released
	reads := prefetch(context.Background(), inputs, semaphore.NewWeighted(int64(largest)))

	for i, read := range reads {
		input := <-read

		if i == 2 {
			require.Error(t, input.err, "the input that cannot be read")
		} else {
			require.NoError(t, input.err)
			require.Equal(t, want[i], input.data, "input %d", i)
		}

		opened := storage.opens.Load()
		require.LessOrEqual(t, opened, int64(i+1), "nothing is read beyond the budget")

		input.release()
	}

	large := &archive{name: "large", size: prefetchLimit + 1, storage: storage}
	input := <-prefetch(context.Background(), []*archive{large}, semaphore.NewWeighted(prefetchBytes))[0]
	require.NoError(t, input.err)
	require.Nil(t, input.data, "an input too large to read ahead is left to stream")
}

func TestTwoProcessesCompactingOneStoreKeepEveryObjectAndRetireEachArchiveOnce(t *testing.T) {
	config := WithCompaction(CompactionConfig{Small: 64 << 10, Batch: 64 << 20, MinimumCandidates: 10})

	for _, c := range []struct{ shared, inTurn bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
		shared := c.shared

		t.Run(fmt.Sprintf("shared=%v,inTurn=%v", c.shared, c.inTurn), func(t *testing.T) {
			base := t.TempDir()
			index := NewInMemoryIndex()

			indexOf := func() ArchiveIndex {
				if shared {
					return index
				}

				return NewInMemoryIndex()
			}

			writer := newTestStore(t, base, WithArchiveIndex(index), config)
			objects := makeTestData(t, 300)

			for i := 0; i < len(objects); i += 3 {
				for _, object := range objects[i : i+3] {
					require.NoError(t, writer.Put(context.Background(), object))
				}

				require.NoError(t, writer.Flush())
			}

			require.NoError(t, writer.Close())

			processes := []*PackStorage{newTestStore(t, base, WithArchiveIndex(indexOf()), config),
				newTestStore(t, base, WithArchiveIndex(indexOf()), config)}

			reports := make([]*CompactReport, len(processes))
			errs := make([]error, len(processes))

			if c.inTurn {
				for i, process := range processes {
					reports[i], errs[i] = process.Compact(context.Background())
				}
			} else {
				start := make(chan struct{})

				var wg sync.WaitGroup
				for i, process := range processes {
					wg.Go(func() {
						<-start
						reports[i], errs[i] = process.Compact(context.Background())
					})
				}

				close(start)
				wg.Wait()
			}

			for i, process := range processes {
				require.NoError(t, errs[i])
				require.NoError(t, process.Close())
			}

			require.Positive(t, reports[0].Rewritten+reports[1].Rewritten)

			fresh := newTestStore(t, base, config)
			defer fresh.Close()

			retirements, err := fresh.retirements()
			require.NoError(t, err)

			times := make(map[string]int)
			for _, r := range retirements {
				for _, name := range r.Archives {
					times[name]++
				}
			}

			for name, n := range times {
				require.Equal(t, 1, n, "%s is retired once", name)
			}

			for i, object := range objects {
				got, err := fresh.Get(context.Background(), object.Ref())
				require.NoError(t, err, "object %d", i)
				require.Equal(t, object.Bytes(), got.Bytes(), "object %d", i)
			}
		})
	}
}
