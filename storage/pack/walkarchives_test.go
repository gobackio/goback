package pack

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

func TestWalkArchivesReadsOnlyTheArchivesNotSkipped(t *testing.T) {
	store := newTestStore(t, t.TempDir(), WithMaxSize(64*1024*1024), WithCompaction(CompactionConfig{MinimumCandidates: 0, Workers: 1}))
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()
	target := makeTestData(t, 1)[0].Ref()
	first := proto.NewObject(&proto.Pin{Target: target, ReceivedAtNs: 1})
	second := proto.NewObject(&proto.Pin{Target: target, ReceivedAtNs: 2})

	walk := func(known []string) ([]string, []string) {
		t.Helper()

		skip := make(map[string]bool, len(known))
		for _, name := range known {
			skip[name] = true
		}

		var pins []string
		names, err := store.WalkArchives(ctx, proto.ObjectType_PIN, func(name string) bool { return skip[name] }, func(obj *proto.Object) error {
			pins = append(pins, obj.Ref().String())
			return nil
		})
		require.NoError(t, err)

		return names, pins
	}

	require.NoError(t, store.Put(ctx, first))
	require.NoError(t, store.Flush())

	known, pins := walk(nil)
	require.Len(t, known, 1)
	require.Equal(t, []string{first.Ref().String()}, pins)

	require.NoError(t, store.Put(ctx, second))
	require.NoError(t, store.Flush())

	names, pins := walk(known)
	require.Len(t, names, 2)
	require.Subset(t, names, known)
	require.Equal(t, []string{second.Ref().String()}, pins, "the skipped archive is not read")

	known = names
	compact(t, context.Background(), store)

	names, pins = walk(known)
	require.NotEmpty(t, names)
	require.NotSubset(t, known, names, "a rewrite is a new archive")
	require.ElementsMatch(t, []string{first.Ref().String(), second.Ref().String()}, pins)
}

func TestAWalkWhoseCallbackAsksTheStoreLetsAWriterIn(t *testing.T) {
	store := newTestStore(t, t.TempDir())

	ctx := context.Background()
	stored := makeTestData(t, 3)
	putAll(t, store, stored)

	later := makeTestData(t, 1)[0]
	wrote := make(chan error, 1)
	walked := make(chan error, 1)

	go func() {
		first := true

		walked <- store.Walk(ctx, false, proto.ObjectType_INVALID, func(*proto.Object) error {
			if !first {
				return nil
			}

			first = false

			// a writer that opens an archive waits for the store's lock
			go func() { wrote <- store.Put(ctx, later) }()
			time.Sleep(100 * time.Millisecond)

			_, err := store.Has(ctx, stored[0].Ref())

			return err
		})
	}()

	select {
	case err := <-walked:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the walk and the writer wait for each other")
	}

	require.NoError(t, <-wrote)
	require.NoError(t, store.Close())
}

func TestAWalkReachesWhatARewriteMovesWhileItRuns(t *testing.T) {
	store := newTestStore(t, t.TempDir(), WithCompaction(CompactionConfig{MinimumCandidates: 0}))
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()
	objects := makeTestData(t, 4)

	for _, obj := range objects {
		require.NoError(t, store.Delete(ctx, obj.Ref()))
		require.NoError(t, store.Flush())
	}

	seen := make(map[string]bool)
	compacted := false

	require.NoError(t, store.WalkHeaders(ctx, proto.ObjectType_TOMBSTONE, func(hdr *proto.ObjectHeader) error {
		seen[string(hdr.TombstoneFor.Hash)] = true

		if !compacted {
			compacted = true
			compact(t, ctx, store)
		}

		return nil
	}))

	for _, obj := range objects {
		require.True(t, seen[string(obj.Ref().Hash)], "tombstone of %x", obj.Ref().Hash)
	}
}

func TestARefreshUnloadsAnArchiveThisProcessWroteOnceAnotherDeletedIt(t *testing.T) {
	store, err := NewPackStorage(WithArchiveStorage(newMemBucket().view()), WithArchiveIndex(NewInMemoryIndex()))
	require.NoError(t, err)
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()
	putAll(t, store, makeTestData(t, 3))
	require.Len(t, store.archives, 1)

	gone := store.archives[0].name
	require.NoError(t, store.index.DeleteArchives([]string{gone}))
	for _, ext := range []string{ArchiveSuffix, IndexExt} {
		require.NoError(t, store.storage.Delete(gone+ext))
	}

	require.NoError(t, store.refreshArchives())
	require.Empty(t, store.archives)
	require.NoError(t, store.Walk(ctx, false, proto.ObjectType_INVALID, func(*proto.Object) error { return nil }))
}

func TestAWalkReturnsWhatItsCallbackReturns(t *testing.T) {
	store := newTestStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })

	putAll(t, store, makeTestData(t, 3))

	stop := errors.New("stop")
	err := store.Walk(context.Background(), true, proto.ObjectType_INVALID, func(*proto.Object) error { return stop })
	require.Equal(t, stop, err)
}
