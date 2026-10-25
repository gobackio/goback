package storage

import (
	"context"
	"testing"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/pack"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/fileblob"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// testSealedStore checks that a store keeps a sealed blob byte for byte
// and hands it back under its ref; read is how the store serves a blob.
func testSealedStore(t *testing.T, store backup.ObjectStore, read func(context.Context, *proto.Ref) (*proto.Object, error)) {
	t.Helper()

	key, err := storekey.Generate("s1")
	require.NoError(t, err)

	sealed := backup.SealBlob(key, []byte("secret save data"))
	obj := proto.NewObject(sealed)
	ctx := context.Background()

	require.NoError(t, store.Put(ctx, obj))

	got, err := read(ctx, sealed.Ref())
	require.NoError(t, err)
	require.NotNil(t, got.GetSealed())
	require.Equal(t, sealed.Data, got.GetSealed().Data)
	require.Equal(t, sealed.Encryption, got.GetSealed().Encryption)
	require.Equal(t, sealed.KeyId, got.GetSealed().KeyId)
	require.Equal(t, sealed.Compression, got.GetSealed().Compression)
	require.True(t, got.Ref().Equal(sealed.Ref()))

	opened, err := backup.OpenBlob(key, got.GetSealed())
	require.NoError(t, err)
	require.Equal(t, []byte("secret save data"), opened)
}

func TestPackStoreSealed(t *testing.T) {
	bucket, err := fileblob.OpenBucket(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bucket.Close() })

	store, err := pack.NewPackStorage(
		pack.WithArchiveStorage(NewBucketStore(bucket)),
		pack.WithArchiveIndex(pack.NewInMemoryIndex()),
	)
	require.NoError(t, err)
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })

	// a bucket-backed archive is readable once it is flushed
	read := func(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
		if err := store.Flush(); err != nil {
			return nil, err
		}

		return store.Get(ctx, ref)
	}

	testSealedStore(t, store, read)
}

func TestRemoteSealed(t *testing.T) {
	_, dial := startServer(t)
	client := dial("node-1")

	// the server serves blobs only as parts of a referenced file
	readPart := func(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
		file := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Length: 1, Ref: ref}}})
		if err := client.Put(ctx, file); err != nil {
			return nil, err
		}

		var got *proto.Object
		err := client.ReadParts(ctx, file.Ref(), nil, func(_ int, obj *proto.Object) error {
			got = obj
			return nil
		})

		return got, err
	}

	testSealedStore(t, client, readPart)
}

func TestASealedBlobFiledUnderAnotherRefIsRefused(t *testing.T) {
	_, dial := startServer(t)
	client := dial("node-1")
	ctx := context.Background()

	key, err := storekey.Generate("s1")
	require.NoError(t, err)

	honest := proto.NewObject(&proto.Blob{Data: []byte("a popular server jar")})
	junk := proto.NewObject(backup.SealBlob(key, []byte("junk")))

	_, err = client.store.Put(client.outgoing(ctx), &proto.PutRequest{Object: junk, Ref: honest.Ref()})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	missing, err := client.Missing(ctx, []*proto.Ref{honest.Ref()})
	require.NoError(t, err)
	require.Len(t, missing, 1, "nothing answers for the ref the forgery claimed")
}
