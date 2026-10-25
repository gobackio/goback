package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/gobackio/goback/admin"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/storekey"
	adminpb "github.com/gobackio/goback/proto/admin"
	"github.com/gobackio/goback/storage/pack"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/fileblob"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func bucketPack(t *testing.T, dir string) *pack.PackStorage {
	t.Helper()

	bucket, err := fileblob.OpenBucket(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bucket.Close() })

	store, err := pack.NewPackStorage(
		pack.WithArchiveStorage(NewBucketStore(bucket)),
		pack.WithArchiveIndex(pack.NewInMemoryIndex()),
	)
	require.NoError(t, err)
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })

	return store
}

func escrowed(t *testing.T, key *storekey.Key, passphrase string) backup.EscrowedKey {
	t.Helper()

	data, err := key.Escrow(passphrase)
	require.NoError(t, err)

	return backup.EscrowedKey{KeyID: key.IDString(), Escrowed: data}
}

func TestTheEscrowedKeyLivesInTheBucket(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := bucketPack(t, dir)

	key, err := storekey.Generate("s1")
	require.NoError(t, err)
	first := escrowed(t, key, "correct horse")

	require.NoError(t, store.PutEscrowedKey(ctx, first))
	require.NoError(t, store.PutEscrowedKey(ctx, first), "keeping the same copy again changes nothing")

	second := escrowed(t, key, "battery staple")
	require.NoError(t, store.PutEscrowedKey(ctx, second), "another copy of the same key is kept beside the first")

	other, err := storekey.Generate("s1")
	require.NoError(t, err)
	require.ErrorIs(t, store.PutEscrowedKey(ctx, escrowed(t, other, "correct horse")), backup.ErrOtherKeyEscrowed)

	require.ErrorIs(t, store.PutEscrowedKey(ctx, backup.EscrowedKey{KeyID: key.IDString(), Escrowed: []byte("{}")}), backup.ErrInvalidEscrow)
	require.ErrorIs(t, store.PutEscrowedKey(ctx, backup.EscrowedKey{KeyID: "../x", Escrowed: first.Escrowed}), backup.ErrInvalidEscrow)

	// a store opened over the same bucket with a fresh index still has both
	kept, err := bucketPack(t, dir).EscrowedKeys(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []backup.EscrowedKey{first, second}, kept)
}

func TestAFirstEscrowedCopyIsKeptOnlyWhileTheStoreKeepsNoneOfItsKey(t *testing.T) {
	ctx := context.Background()
	store := bucketPack(t, t.TempDir())

	key, err := storekey.Generate("s1")
	require.NoError(t, err)

	copies := make([]backup.EscrowedKey, 4)
	for i := range copies {
		copies[i] = escrowed(t, key, fmt.Sprintf("passphrase number %d", i))
	}

	errs := make(chan error, len(copies))
	for _, c := range copies {
		go func() { errs <- store.PutFirstEscrowedKey(ctx, c) }()
	}

	firsts := 0
	for range copies {
		err := <-errs
		if err == nil {
			firsts++
			continue
		}

		require.ErrorIs(t, err, backup.ErrKeyEscrowed)
	}

	require.Equal(t, 1, firsts, "of copies put at once, one is first")

	kept, err := store.EscrowedKeys(ctx)
	require.NoError(t, err)
	require.Len(t, kept, 1)
	require.NoError(t, store.PutFirstEscrowedKey(ctx, kept[0]), "the copy kept is kept already")

	other, err := storekey.Generate("s1")
	require.NoError(t, err)
	require.ErrorIs(t, store.PutFirstEscrowedKey(ctx, escrowed(t, other, "correct horse")), backup.ErrOtherKeyEscrowed)
}

type escrowIndex struct {
	*memIndex
	backup.KeyEscrow
}

func TestAnAgentFetchesTheEscrowedKey(t *testing.T) {
	ctx := context.Background()
	store := bucketPack(t, t.TempDir())

	key, err := storekey.Generate("s1")
	require.NoError(t, err)
	held := escrowed(t, key, "correct horse")
	require.NoError(t, store.PutEscrowedKey(ctx, held))

	client := startServerWith(t, escrowIndex{memIndex: newMemIndex(), KeyEscrow: store}, nil)("node-1")

	kept, err := client.EscrowedKeys(ctx)
	require.NoError(t, err)
	require.Equal(t, []backup.EscrowedKey{held}, kept)

	opened, err := storekey.Recover(kept[0].Escrowed, "correct horse")
	require.NoError(t, err)
	require.Equal(t, key.ID(), opened.ID())
}

func TestAnOperatorKeepsTheEscrowedKeyThroughTheStoresOwnListener(t *testing.T) {
	ctx := context.Background()
	store := bucketPack(t, t.TempDir())

	key, err := storekey.Generate("s1")
	require.NoError(t, err)
	held := escrowed(t, key, "correct horse")

	operations := &admin.Server{Escrow: store}
	dial := serveAs(t, NewStore(escrowIndex{memIndex: newMemIndex(), KeyEscrow: store}, nil), func(srv *grpc.Server) {
		adminpb.RegisterAdminServer(srv, operations)
	})

	require.Equal(t, codes.Unauthenticated, status.Code(dial("wrong", "operator").PutEscrowedKey(ctx, held)), "the store's secret guards it")
	require.NoError(t, dial(testSecret, "operator").PutEscrowedKey(ctx, held))

	kept, err := store.EscrowedKeys(ctx)
	require.NoError(t, err)
	require.Equal(t, []backup.EscrowedKey{held}, kept)
}
