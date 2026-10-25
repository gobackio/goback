package pack

import (
	"context"
	"testing"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

func TestPackSealedRoundTrip(t *testing.T) {
	base := t.TempDir()
	store := newTestStore(t, base)

	key, err := storekey.Generate("s1")
	require.NoError(t, err)

	sealed := backup.SealBlob(key, []byte("shared level data"))
	ctx := context.Background()
	require.NoError(t, store.Put(ctx, proto.NewObject(sealed)))

	tree := proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{{Stat: &proto.FileInfo{Name: []byte{0xff, 0x00}}, Ref: sealed.SealedRef()}}})
	tree.KeyId = key.ID()
	require.NoError(t, store.Put(ctx, tree))
	require.NoError(t, store.Close())

	// reopen so the objects are read back from the archive
	store = newTestStore(t, base)
	t.Cleanup(func() { _ = store.Close() })

	got, err := store.Get(ctx, sealed.SealedRef())
	require.NoError(t, err)
	require.NotNil(t, got.Sealed())
	require.Equal(t, sealed.Data, got.Sealed().Data)
	require.Equal(t, proto.Encryption_SEALED, got.Sealed().Encryption)

	opened, err := backup.OpenBlob(key, got.Sealed())
	require.NoError(t, err)
	require.Equal(t, []byte("shared level data"), opened)

	gotTree, err := store.Get(ctx, tree.Ref())
	require.NoError(t, err)
	require.Equal(t, key.ID(), gotTree.KeyId)
	require.Equal(t, []byte{0xff, 0x00}, gotTree.GetTree().Nodes[0].Stat.Name)

	var blobs, trees int
	require.NoError(t, store.Walk(ctx, true, proto.ObjectType_BLOB, func(obj *proto.Object) error {
		require.NotNil(t, obj.Sealed())
		blobs++
		return nil
	}))
	require.NoError(t, store.Walk(ctx, true, proto.ObjectType_TREE, func(obj *proto.Object) error {
		trees++
		return nil
	}))
	require.Equal(t, 1, blobs)
	require.Equal(t, 1, trees)
}

func TestASealedBlobIsFiledUnderTheRefItsBytesHashTo(t *testing.T) {
	base := t.TempDir()
	store := newTestStore(t, base)
	ctx := context.Background()

	honest := proto.NewObject(&proto.Blob{Data: []byte("a popular server jar")})
	junk := proto.NewObject(&proto.Stored{Type: proto.ObjectType_BLOB, Data: honest.GetBlob().Data, Encryption: proto.Encryption_SEALED, KeyId: []byte{0xba, 0xd0}})
	require.NoError(t, store.Put(ctx, junk))
	require.NoError(t, store.Close())

	store = newTestStore(t, base)
	t.Cleanup(func() { _ = store.Close() })

	has, err := store.HasAll(ctx, []*proto.Ref{honest.Ref(), junk.Ref()})
	require.NoError(t, err)
	require.Equal(t, []bool{false, true}, has, "the sealed bytes answer only for their own ref")

	got, err := store.Get(ctx, junk.Ref())
	require.NoError(t, err)
	require.True(t, got.Ref().Equal(junk.Ref()))
}
