package pack

import (
	"context"
	"crypto/rand"
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

	tree := proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{{Stat: &proto.FileInfo{Name: []byte{0xff, 0x00}}, Ref: sealed.Ref}}})
	tree.KeyId = key.ID()
	require.NoError(t, store.Put(ctx, tree))
	require.NoError(t, store.Close())

	// reopen so the objects are read back from the archive
	store = newTestStore(t, base)
	t.Cleanup(func() { _ = store.Close() })

	got, err := store.Get(ctx, sealed.Ref)
	require.NoError(t, err)
	require.NotNil(t, got.GetSealed())
	require.Equal(t, sealed.Data, got.GetSealed().Data)
	require.Equal(t, proto.Encryption_SEALED, got.GetSealed().Encryption)

	opened, err := backup.OpenBlob(key, got.GetSealed())
	require.NoError(t, err)
	require.Equal(t, []byte("shared level data"), opened)

	gotTree, err := store.Get(ctx, tree.Ref())
	require.NoError(t, err)
	require.Equal(t, key.ID(), gotTree.KeyId)
	require.Equal(t, []byte{0xff, 0x00}, gotTree.GetTree().Nodes[0].Stat.Name)

	var blobs, trees int
	require.NoError(t, store.Walk(ctx, true, proto.ObjectType_BLOB, func(obj *proto.Object) error {
		require.NotNil(t, obj.GetSealed())
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

// forgery is a sealed blob of junk filed under ref, which anyone who
// knows content can compute the plaintext ref of.
func forgery(ref *proto.Ref, junk string) *proto.Object {
	return proto.NewObject(&proto.Sealed{Ref: ref, Type: proto.ObjectType_BLOB, Data: []byte(junk), Encryption: proto.Encryption_SEALED, KeyId: []byte{0xba, 0xd0}})
}

func requireServesPlaintext(t *testing.T, store *PackStorage, honest []*proto.Object) {
	t.Helper()
	ctx := context.Background()

	for _, obj := range honest {
		got, err := store.Get(ctx, obj.Ref())
		require.NoError(t, err)
		require.Nil(t, got.GetSealed(), "the forgery is served")
		require.Equal(t, obj.GetBlob().GetData(), got.GetBlob().GetData())

		verified, err := store.HasVerified(ctx, []*proto.Ref{obj.Ref()})
		require.NoError(t, err)
		require.True(t, verified[0])
	}
}

func TestASealedForgeryPlantedBeforeAnHonestUploadNeverStandsInForIt(t *testing.T) {
	base := t.TempDir()
	compacting := WithCompaction(CompactionConfig{Small: 32 << 10, MinimumCandidates: 0})
	store := newTestStore(t, base, compacting)
	first := store
	t.Cleanup(func() { _ = first.Close() })
	ctx := context.Background()

	var honest []*proto.Object
	for i := range 4 {
		honest = append(honest, proto.NewObject(&proto.Blob{Data: []byte{'j', 'a', 'r', byte(i)}}))
		require.NoError(t, store.Put(ctx, forgery(honest[i].Ref(), "junk")))
	}

	// the forgeries sit in an archive too large to compact, where a
	// rewrite of the honest copies could take them for stand-ins
	ballast := make([]byte, 64<<10)
	_, err := rand.Read(ballast)
	require.NoError(t, err)
	require.NoError(t, store.Put(ctx, proto.NewObject(&proto.Blob{Data: ballast})))
	require.NoError(t, store.Flush())

	refs := make([]*proto.Ref, len(honest))
	for i, obj := range honest {
		refs[i] = obj.Ref()
	}

	has, err := store.HasAll(ctx, refs)
	require.NoError(t, err)
	require.NotContains(t, has, false, "a sealed writer still dedups against the sealed copy")

	verified, err := store.HasVerified(ctx, refs)
	require.NoError(t, err)
	require.NotContains(t, verified, true, "a plaintext writer uploads its own copy")

	for _, obj := range honest {
		require.NoError(t, store.Put(ctx, obj))
	}
	require.NoError(t, store.Flush())
	requireServesPlaintext(t, store, honest)
	require.ErrorIs(t, store.Put(ctx, forgery(honest[0].Ref(), "more junk")), proto.ErrInvalidObject)
	require.NoError(t, store.Close())

	// read back from the archives, whose headers say which copy is sealed
	store = newTestStore(t, base, compacting)
	t.Cleanup(func() { _ = store.Close() })
	requireServesPlaintext(t, store, honest)

	require.NotZero(t, compact(t, ctx, store).Rewritten)
	requireServesPlaintext(t, store, honest)
}

func TestARewriteOfAForgeryAndTheHonestCopyKeepsTheHonestOne(t *testing.T) {
	store := newTestStore(t, t.TempDir(), WithCompaction(CompactionConfig{Small: 32 << 10, MinimumCandidates: 0}))
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	// a rewrite meets the archives in no fixed order, so several pairs
	// are planted, each copy in an archive of its own
	var honest []*proto.Object
	for i := range 6 {
		honest = append(honest, proto.NewObject(&proto.Blob{Data: []byte{'j', 'a', 'r', byte(i)}}))
		require.NoError(t, store.Put(ctx, forgery(honest[i].Ref(), "junk")))
		require.NoError(t, store.Flush())
	}

	refs := make([]*proto.Ref, len(honest))
	for i, obj := range honest {
		refs[i] = obj.Ref()
		require.NoError(t, store.Put(ctx, obj))
		require.NoError(t, store.Flush())
	}

	require.NotZero(t, compact(t, ctx, store).Rewritten)
	requireServesPlaintext(t, store, honest)

	copies, err := store.index.LocateCopies(refs, Scope{})
	require.NoError(t, err)

	for _, ref := range refs {
		require.Len(t, copies[string(ref.Hash)], 1, "a rewrite leaves the forgery behind")
	}
}

func TestASealedForgeryOfContentStoredInTheClearIsRefused(t *testing.T) {
	store := newTestStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	pending := proto.NewObject(&proto.Blob{Data: []byte("written in this session")})
	require.NoError(t, store.Put(ctx, pending))
	require.ErrorIs(t, store.Put(ctx, forgery(pending.Ref(), "junk")), proto.ErrInvalidObject)

	require.NoError(t, store.Flush())
	require.ErrorIs(t, store.Put(ctx, forgery(pending.Ref(), "junk")), proto.ErrInvalidObject)
	requireServesPlaintext(t, store, []*proto.Object{pending})
}
