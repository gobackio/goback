package pack

import (
	"context"
	"testing"
	"time"

	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/pebble"

	"github.com/stretchr/testify/require"
)

func newCachedStore(t *testing.T) (*PackStorage, *pebble.Store) {
	t.Helper()

	cache, err := pebble.New(t.TempDir())
	require.NoError(t, err)

	store, err := NewPackStorage(
		WithArchiveStorage(newLocal(t.TempDir())),
		WithArchiveIndex(NewInMemoryIndex()),
		WithMaxSize(256*1024),
		WithMetadataCache(cache),
	)
	require.NoError(t, err)
	require.NoError(t, store.Open())

	return store, cache
}

func requireCached(t *testing.T, cache *pebble.Store, objects []*proto.Object, cached bool) {
	t.Helper()

	for _, obj := range objects {
		switch obj.Type() {
		case proto.ObjectType_COMMIT, proto.ObjectType_TREE, proto.ObjectType_FILE:
		default:
			continue
		}

		has, err := cache.Has(context.Background(), obj.Ref())
		require.NoError(t, err)
		require.Equalf(t, cached, has, "cache entry of %x of type %s", obj.Ref().Hash, obj.Type())
	}
}

func TestTheMetadataCacheServesOnlyWhatTheIndexServes(t *testing.T) {
	store, cache := newCachedStore(t)
	ctx, _ := beginSession(t, store, "agent")

	tree := treeOf(makeGCFiles(makeTestData(t, 2)))
	require.NoError(t, store.Put(ctx, tree))
	requireCached(t, cache, []*proto.Object{tree}, true)

	requireVisible(t, store, ctx, tree, true)
	requireVisible(t, store, context.Background(), tree, false)

	require.NoError(t, store.Close())
}

func TestReclaimingAnObjectDropsItFromTheMetadataCache(t *testing.T) {
	store, cache := newCachedStore(t)
	ctx := context.Background()

	blobs := makeTestData(t, 40)
	gone := makeChain(blobs[:20])
	kept := makeChain(blobs[20:])
	putAll(t, store, append(append([]*proto.Object{}, gone...), kept...))
	requireCached(t, cache, gone, true)

	require.NoError(t, store.Delete(ctx, gone[len(gone)-1].Ref()))
	require.NoError(t, store.Flush())

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	second, err := store.Collect(ctx, gcOptions(t, 48*time.Hour))
	require.NoError(t, err)
	require.EqualValues(t, len(gone), second.ReclaimedObjects)

	requireCached(t, cache, gone, false)
	requireCached(t, cache, kept, true)
	requireStored(t, store, gone, false)
	requireStored(t, store, kept, true)

	require.NoError(t, store.Close())
}
