package pack

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/gobackio/goback/progress"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/pebble"

	"github.com/stretchr/testify/require"
)

func TestCachingMetadataReadsNeighboursTogetherAndOnlyOnce(t *testing.T) {
	ctx := context.Background()
	storage := &countingStorage{localArchiveStorage: newLocal(t.TempDir())}

	store, err := NewPackStorage(WithArchiveStorage(storage), WithArchiveIndex(NewInMemoryIndex()))
	require.NoError(t, err)
	require.NoError(t, store.Open())

	const files = 60

	var metadata []*proto.Object
	for i := range files {
		blob := proto.NewObject(&proto.Blob{Data: fmt.Appendf(nil, "blob %d", i)})
		require.NoError(t, store.Put(ctx, blob))

		file := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Length: uint64(len(blob.GetBlob().GetData())), Ref: blob.Ref()}}})
		require.NoError(t, store.Put(ctx, file))
		metadata = append(metadata, file)
	}

	tree := treeOf(metadata)
	require.NoError(t, store.Put(ctx, tree))

	commit := proto.NewObject(&proto.Commit{Tree: tree.Ref(), Timestamp: 1, BackupSet: "world"})
	require.NoError(t, store.Put(ctx, commit))
	require.NoError(t, store.Close())

	metadata = append(metadata, tree, commit)

	cache, err := pebble.New(t.TempDir())
	require.NoError(t, err)

	store, err = NewPackStorage(WithArchiveStorage(storage), WithArchiveIndex(NewInMemoryIndex()), WithMetadataCache(cache))
	require.NoError(t, err)
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })

	var (
		mtx  sync.Mutex
		last progress.Update
	)

	reported := progress.NewContext(ctx, func(u progress.Update) {
		mtx.Lock()
		last = u
		mtx.Unlock()
	})

	before := storage.count()
	require.NoError(t, store.CacheMetadata(reported, progress.OpReIndex))
	require.Less(t, storage.count()-before, files/2, "neighbouring records come back in one read, not one each")
	requireCached(t, cache, metadata, true)

	require.Equal(t, progress.OpReIndex, last.Op)
	require.Equal(t, progress.PhaseMetadata, last.Phase)
	require.Equal(t, last.Total, last.Done)
	require.Equal(t, last.BytesTotal, last.Bytes)
	require.Positive(t, last.Bytes)

	before = storage.count()
	require.NoError(t, store.CacheMetadata(ctx, progress.OpReIndex))

	for _, obj := range metadata {
		_, err := store.Get(ctx, obj.Ref())
		require.NoError(t, err)
	}

	require.Equal(t, before, storage.count(), "what the cache holds is neither read again nor read to serve it")
}

func TestCachingMetadataLeavesOutARecordThatDoesNotDecode(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()

	store := newTestStore(t, base)

	var files []*proto.Object
	for i := range 3 {
		file := proto.NewObject(&proto.File{Inline: fmt.Appendf(nil, "file %d", i)})
		require.NoError(t, store.Put(ctx, file))
		files = append(files, file)
	}
	require.NoError(t, store.Close())

	damageRecord(t, base, files[1].Ref())

	cache, err := pebble.New(t.TempDir())
	require.NoError(t, err)

	store = newTestStore(t, base, WithMetadataCache(cache))
	t.Cleanup(func() { _ = store.Close() })

	require.NoError(t, store.CacheMetadata(ctx, progress.OpReIndex))
	requireCached(t, cache, []*proto.Object{files[0], files[2]}, true)
	requireCached(t, cache, []*proto.Object{files[1]}, false)
}
