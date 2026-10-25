package pack

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// recordOf finds where the store keeps the first record of the type.
func recordOf(t *testing.T, store *PackStorage, typ proto.ObjectType) (string, IndexRecord) {
	t.Helper()

	store.mtx.RLock()
	archives := append([]*archive(nil), store.archives...)
	store.mtx.RUnlock()

	for _, a := range archives {
		idx, err := a.getIndex()
		require.NoError(t, err)

		for _, rec := range idx {
			if proto.ObjectType(rec.Type) == typ {
				return a.name, rec
			}
		}
	}

	require.FailNow(t, "no record of the type", "%s", typ)

	return "", IndexRecord{}
}

func TestACollectionReportsATombstoneWhoseHeaderIsDamaged(t *testing.T) {
	base := t.TempDir()
	store := newGCStore(t, base)
	ctx := context.Background()

	chain := makeChain(makeTestData(t, 5))
	putAll(t, store, chain)
	require.NoError(t, store.Delete(ctx, chain[len(chain)-1].Ref()))
	require.NoError(t, store.Flush())

	name, rec := recordOf(t, store, proto.ObjectType_TOMBSTONE)
	require.NoError(t, store.Close())

	file, err := os.OpenFile(filepath.Join(base, name+ArchiveSuffix), os.O_RDWR, 0o644)
	require.NoError(t, err)
	_, err = file.WriteAt(bytes.Repeat([]byte{0xff}, 10), int64(rec.Offset))
	require.NoError(t, err)
	require.NoError(t, file.Close())

	store = newGCStore(t, base)
	t.Cleanup(func() { _ = store.Close() })

	_, err = store.Collect(ctx, gcOptions(t, 0))
	require.Error(t, err)
}

// failingGCWrite refuses one write of a mark result once armed.
type failingGCWrite struct {
	*localArchiveStorage

	armed atomic.Bool
}

func (f *failingGCWrite) Create(name string) (File, error) {
	if strings.HasSuffix(name, GCExt) && f.armed.CompareAndSwap(true, false) {
		return nil, errors.New("503 service unavailable")
	}

	return f.localArchiveStorage.Create(name)
}

func TestACollectionThatFailsAfterItsMarkMarksAgainWhenRetried(t *testing.T) {
	storage := &failingGCWrite{localArchiveStorage: newLocal(t.TempDir())}
	store, err := NewPackStorage(WithArchiveStorage(storage), WithArchiveIndex(NewInMemoryIndex()), WithMaxSize(256*1024))
	require.NoError(t, err)
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	reachable, unreachable := makeGCTestData(t)
	putAll(t, store, append(append([]*proto.Object{}, reachable...), unreachable...))

	_, err = store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	opts := gcOptions(t, 48*time.Hour)
	storage.armed.Store(true)
	_, err = store.Collect(ctx, opts)
	require.Error(t, err)

	retried, err := store.Collect(ctx, opts)
	require.NoError(t, err)
	require.Zero(t, retried.Resumed)
	require.Zero(t, retried.Condemned)
	requirePresent(t, store, reachable, true)

	_, err = store.Collect(ctx, gcOptions(t, 96*time.Hour))
	require.NoError(t, err)
	requireStored(t, store, reachable, true)
}

func TestAResumedMarkMarksAgainABatchWhoseRunsDidNotSurvive(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
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

	runs, err := filepath.Glob(filepath.Join(opts.TempDir, "goback-gc", "gen-1", "batch-0-*.run"))
	require.NoError(t, err)
	require.NotEmpty(t, runs)
	for _, run := range runs {
		require.NoError(t, os.Truncate(run, 0))
	}

	gcAfterBatch = nil
	report, err := store.Collect(ctx, opts)
	require.NoError(t, err)
	require.Equal(t, 1, report.Resumed)
	require.EqualValues(t, len(reachable), report.Marked)
	requirePresent(t, store, reachable, true)
}

func TestAResumedMarkMarksAgainWhenTheRootsFallIntoOtherBatches(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	reachable, unreachable := makeGCTestData(t)
	putAll(t, store, append(append([]*proto.Object{}, reachable...), unreachable...))

	var roots [][]byte
	for _, obj := range reachable {
		if obj.Type() == proto.ObjectType_COMMIT {
			roots = append(roots, obj.Ref().Hash)
		}
	}
	slices.SortFunc(roots, bytes.Compare)

	// a set per root, numbered in key order, keeps the roots' order but
	// gives each a batch of its own
	opts := gcOptions(t, 0)
	opts.Owner = func(root []byte) Attribution {
		return Attribution{Set: int64(slices.IndexFunc(roots, func(r []byte) bool { return bytes.Equal(r, root) }) + 1)}
	}

	defer func() { gcAfterBatch = nil }()

	interrupted := errors.New("interrupted")
	gcAfterBatch = func(int) error { return interrupted }

	_, err := store.Collect(ctx, opts)
	require.ErrorIs(t, err, interrupted)

	gcAfterBatch = nil
	opts.Owner = nil
	report, err := store.Collect(ctx, opts)
	require.NoError(t, err)
	require.Zero(t, report.Resumed)
	require.EqualValues(t, len(reachable), report.Marked)
	requirePresent(t, store, reachable, true)
}

func TestLiveRunsReportARunThatEndsWithinAnEntry(t *testing.T) {
	runs := newLiveRuns(t.TempDir(), 2)
	defer runs.close()

	key := func(n byte) refKey { return refKey{n} }
	require.NoError(t, runs.add([]liveRef{{key: key(1)}, {key: key(2)}}))
	require.Len(t, runs.files, 1)
	require.NoError(t, os.Truncate(runs.files[0], int64(liveRefSize+5)))

	it, err := runs.iterator()
	require.NoError(t, err)
	defer it.close()

	_, ok := it.at(key(1))
	require.True(t, ok)
	_, ok = it.at(key(2))
	require.False(t, ok)
	require.Error(t, it.err())
}

func TestAMergeRefusesALiveSetThatLacksARoot(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })

	putAll(t, store, makeChain(makeTestData(t, 5)))

	r := snapshotRun(t, store, nil)
	live, err := r.mark(context.Background())
	require.NoError(t, err)

	live.close()
	_, err = r.merge(live)
	require.Error(t, err)
}
