package sql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/pack"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/fileblob"
)

// losingStore lists an object it can no longer read, as a store whose
// archive of it is gone does.
type losingStore struct {
	*pack.PackStorage
	lost *proto.Ref
}

func (s losingStore) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	if ref.Equal(s.lost) {
		return nil, backup.ErrNotFound
	}

	return s.PackStorage.Get(ctx, ref)
}

func (s losingStore) ReadRecords(context.Context, []*proto.Ref) ([]*proto.Object, error) {
	return nil, errors.New("reading records is not supported")
}

func TestACommitTheStoreTookButTheIndexCouldNotRecordIsNoOrphan(t *testing.T) {
	ctx := context.Background()
	bucket, err := fileblob.OpenBucket(t.TempDir(), nil)
	require.NoError(t, err)
	packs, index := openPacks(t, bucket)

	tree := proto.NewObject(&proto.Tree{})
	require.NoError(t, packs.Put(ctx, tree))
	index.ObjectStore = losingStore{PackStorage: packs, lost: tree.Ref()}

	commit := proto.NewObject(&proto.Commit{Tree: tree.Ref(), BackupSet: "world", Timestamp: 1})
	require.ErrorIs(t, index.Put(ctx, commit), backup.ErrDanglingRef)

	commits := []*proto.Object{
		proto.NewObject(&proto.Commit{Tree: tree.Ref(), BackupSet: "world", Timestamp: 2}),
		proto.NewObject(&proto.Commit{Tree: tree.Ref(), BackupSet: "other", Timestamp: 3}),
	}
	require.ErrorIs(t, index.PutCommits(ctx, commits), backup.ErrDanglingRef)

	require.NoError(t, packs.Flush())

	orphans, err := index.OrphanCommits(ctx)
	require.NoError(t, err)
	require.Empty(t, orphans, "a commit the index failed to record stays a commit no row names")
}

func TestACommitTheIndexFailedToRecordCanBeStoredAgain(t *testing.T) {
	ctx := context.Background()
	bucket, err := fileblob.OpenBucket(t.TempDir(), nil)
	require.NoError(t, err)
	packs, index := openPacks(t, bucket)

	tree := proto.NewObject(&proto.Tree{})
	require.NoError(t, packs.Put(ctx, tree))
	index.ObjectStore = losingStore{PackStorage: packs, lost: tree.Ref()}

	received := time.Now().Add(-time.Minute).UnixNano()
	carrying := func() *proto.Object {
		return proto.NewObject(&proto.Commit{Tree: tree.Ref(), BackupSet: "world", Timestamp: 1, ReceivedAtNs: received})
	}

	failed := carrying()
	require.ErrorIs(t, index.Put(ctx, failed), backup.ErrDanglingRef)

	has, err := packs.Has(ctx, failed.Ref())
	require.NoError(t, err)
	require.False(t, has, "the failed commit is tombstoned")

	index.ObjectStore = packs

	retried := carrying()
	require.NoError(t, index.Put(ctx, retried))
	require.Equal(t, failed.Ref().Hash, retried.Ref().Hash, "the retry carries the same receipt")
	require.NoError(t, packs.Flush())

	rebuilt, _ := openPacks(t, bucket)
	has, err = rebuilt.Has(ctx, retried.Ref())
	require.NoError(t, err)
	require.True(t, has, "the commit stored again is live in the store")
}
