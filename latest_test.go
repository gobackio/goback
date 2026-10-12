package goback

import (
	"context"
	"testing"

	coresql "github.com/gobackio/goback/index/sql"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage"
	"github.com/gobackio/goback/storage/pack"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"
)

func TestLatestByIDReadsThatSetsHead(t *testing.T) {
	ctx := context.Background()

	index := coresql.NewMemory(t.Name(), nil)
	packs, err := pack.NewPackStorage(pack.WithArchiveStorage(storage.NewBucketStore(memblob.OpenBucket(nil))), pack.WithArchiveIndex(index))
	require.NoError(t, err)
	index.ObjectStore = packs
	require.NoError(t, index.Open())
	require.NoError(t, packs.Open())
	t.Cleanup(func() { _ = packs.Close() })

	repo := &Repo{Store: packs, Index: index}

	tree := proto.NewObject(&proto.Tree{})
	require.NoError(t, packs.Put(ctx, tree))

	commit := func(setID uint64, received int64) *proto.Ref {
		obj := proto.NewObject(&proto.Commit{Tree: tree.Ref(), BackupSet: "control-base", SetId: setID, ReceivedAtNs: received})
		require.NoError(t, packs.Put(ctx, obj))
		return obj.Ref()
	}

	ours := commit(3, 10)
	commit(4, 20)

	ref, err := repo.LatestCommit(ctx, "3")
	require.NoError(t, err)
	require.Equal(t, ours, ref)

	_, err = repo.LatestCommit(ctx, "5")
	require.Error(t, err)
}
