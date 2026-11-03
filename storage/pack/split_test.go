package pack

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gobackio/goback/progress"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/pebble"

	"github.com/stretchr/testify/require"
)

// putMixed stores blobs and a tree naming them, flushed into one archive.
func putMixed(t *testing.T, store *PackStorage, n int) []*proto.Object {
	t.Helper()

	objects := makeTestData(t, n)

	tree := &proto.Tree{}
	for i, blob := range objects {
		tree.Nodes = append(tree.Nodes, &proto.TreeNode{Ref: blob.Ref(),
			Stat: &proto.FileInfo{Name: fmt.Appendf(nil, "%04d", i), Type: proto.NodeType_NODE_FILE}})
	}
	objects = append(objects, proto.NewObject(tree))

	for _, object := range objects {
		require.NoError(t, store.Put(context.Background(), object))
	}
	require.NoError(t, store.Flush())

	return objects
}

func mixedNames(t *testing.T, store *PackStorage) []string {
	t.Helper()

	store.mtx.RLock()
	defer store.mtx.RUnlock()

	var names []string
	for _, a := range store.archives {
		mixed, err := a.mixed()
		require.NoError(t, err)

		if mixed {
			names = append(names, a.name)
		}
	}

	return names
}

func TestSplittingRewritesTheMixedArchivesOldestFirstWithinTheBudget(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t, t.TempDir(), WithMaxSize(64<<20))
	defer store.Close()

	first := putMixed(t, store, 20)
	oldest := mixedNames(t, store)
	require.Len(t, oldest, 1)

	time.Sleep(10 * time.Millisecond)
	second := putMixed(t, store, 20)
	require.Len(t, mixedNames(t, store), 2)

	report, err := store.Split(ctx, SplitOptions{Budget: 1})
	require.NoError(t, err)
	require.Equal(t, 1, report.Rewritten, "the budget takes the oldest archive however small it is")

	left := mixedNames(t, store)
	require.Len(t, left, 1)
	require.NotEqual(t, oldest[0], left[0])

	report, err = store.Split(ctx, SplitOptions{})
	require.NoError(t, err)
	require.Equal(t, 1, report.Rewritten)
	require.Empty(t, mixedNames(t, store))

	report, err = store.Split(ctx, SplitOptions{})
	require.NoError(t, err)
	require.Zero(t, report.Rewritten, "what a split wrote is split already")

	for _, object := range append(first, second...) {
		got, err := store.Get(ctx, object.Ref())
		require.NoError(t, err)
		require.Equal(t, plain(object), plain(got))
	}
}

func TestSplittingCachesTheMetadataItCopies(t *testing.T) {
	ctx := context.Background()
	storage := &countingStorage{localArchiveStorage: newLocal(t.TempDir())}

	store := newTestStore(t, "", WithArchiveStorage(storage), WithMaxSize(64<<20))
	putMixed(t, store, 20)
	require.NoError(t, store.Close())

	cache, err := pebble.New(t.TempDir())
	require.NoError(t, err)

	store = newTestStore(t, "", WithArchiveStorage(storage), WithMaxSize(64<<20), WithMetadataCache(cache))
	defer store.Close()

	report, err := store.Split(ctx, SplitOptions{})
	require.NoError(t, err)
	require.Equal(t, 1, report.Rewritten)

	before := storage.count()
	require.NoError(t, store.CacheMetadata(ctx, progress.OpCollect))
	require.Equal(t, before, storage.count(), "the fill after a split reads nothing")
}
