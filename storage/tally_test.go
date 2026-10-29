package storage

import (
	"context"
	"testing"

	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/badger"
	"github.com/gobackio/goback/storage/pack"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"
)

func TestAReadCountsTowardEveryTallyItsContextCarries(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })

	store := NewBucketStore(bucket)
	file, err := store.Create("a.archive")
	require.NoError(t, err)
	_, err = file.Write([]byte("0123456789"))
	require.NoError(t, err)
	require.NoError(t, file.Close())

	opened, err := store.Open("a.archive")
	require.NoError(t, err)
	t.Cleanup(func() { _ = opened.Close() })

	var outer, inner Tally
	ctx := WithTally(context.Background(), &outer)
	read := opened.(pack.ReaderAtContext)

	_, err = read.ReadAtContext(WithTally(ctx, &inner), make([]byte, 4), 6)
	require.NoError(t, err)
	_, err = read.ReadAtContext(ctx, make([]byte, 2), 0)
	require.NoError(t, err)
	_, err = read.ReadAtContext(context.Background(), make([]byte, 8), 0)
	require.NoError(t, err)

	require.Equal(t, Count{Requests: 1, Bytes: 4}, inner.Of(OpGet))
	require.Equal(t, Count{Requests: 2, Bytes: 6}, outer.Of(OpGet))

	require.Equal(t, Count{Requests: 2, Bytes: 6}, outer.Take(OpGet))
	require.Zero(t, outer.Of(OpGet))
}

func TestWhatTheMetadataCacheAnswersAsksTheBucketNothing(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })

	cache, err := badger.New(t.TempDir())
	require.NoError(t, err)

	store, err := pack.NewPackStorage(
		pack.WithArchiveStorage(NewBucketStore(bucket)),
		pack.WithArchiveIndex(pack.NewInMemoryIndex()),
		pack.WithMetadataCache(cache),
	)
	require.NoError(t, err)
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })

	blob := proto.NewObject(&proto.Blob{Data: []byte("a save file")})
	tree := proto.NewObject(&proto.Tree{})
	require.NoError(t, store.Put(context.Background(), blob))
	require.NoError(t, store.Put(context.Background(), tree))
	require.NoError(t, store.Flush())

	var tally Tally
	ctx := WithTally(context.Background(), &tally)

	_, err = store.Get(ctx, tree.Ref())
	require.NoError(t, err)
	require.Zero(t, tally.Of(OpGet))

	_, err = store.Get(ctx, blob.Ref())
	require.NoError(t, err)
	require.Equal(t, int64(1), tally.Of(OpGet).Requests)
	require.Positive(t, tally.Of(OpGet).Bytes)
}
