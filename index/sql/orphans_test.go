package sql

import (
	"context"
	"testing"
	"time"

	coreindex "github.com/gobackio/goback/index"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/fileblob"
)

func TestOrphanCommitsAreTheStoredCommitsWithoutARowOrTombstone(t *testing.T) {
	ctx := context.Background()
	bucket, err := fileblob.OpenBucket(t.TempDir(), nil)
	require.NoError(t, err)
	packs, index := openPacks(t, bucket)

	tree := proto.NewObject(&proto.Tree{})
	require.NoError(t, packs.Put(ctx, tree))

	commit := func(n int64) *proto.Object {
		return proto.NewObject(&proto.Commit{Tree: tree.Ref(), BackupSet: "world", Timestamp: n})
	}

	indexed, orphan, retired := commit(1), commit(2), commit(3)
	require.NoError(t, index.Put(ctx, indexed))
	require.NoError(t, packs.Put(ctx, orphan))
	require.NoError(t, packs.Put(ctx, retired))
	require.NoError(t, packs.Flush())
	require.NoError(t, packs.Delete(ctx, retired.Ref()))
	require.NoError(t, packs.Flush())

	orphans, err := index.OrphanCommits(ctx)
	require.NoError(t, err)
	require.Len(t, orphans, 1)
	require.Equal(t, orphan.Ref().Hash, orphans[0].Ref.Hash)
	require.Equal(t, 1, orphans[0].Copies)
	require.Positive(t, orphans[0].Bytes)
}

func TestDroppingOrphansTombstonesTheOrphansReceivedBeforeTheCutoff(t *testing.T) {
	ctx := context.Background()
	bucket, err := fileblob.OpenBucket(t.TempDir(), nil)
	require.NoError(t, err)
	packs, index := openPacks(t, bucket)

	tree := proto.NewObject(&proto.Tree{})
	require.NoError(t, packs.Put(ctx, tree))

	cutoff := time.Now().Add(-time.Hour)
	received := func(at time.Time) *proto.Object {
		return proto.NewObject(&proto.Commit{Tree: tree.Ref(), BackupSet: "world", ReceivedAtNs: at.UnixNano()})
	}

	indexed, old, recent := received(cutoff.Add(-2*time.Hour)), received(cutoff.Add(-time.Hour)), received(time.Now())
	require.NoError(t, index.Put(ctx, indexed))
	require.NoError(t, packs.Put(ctx, old))
	require.NoError(t, packs.Put(ctx, recent))
	require.NoError(t, packs.Flush())

	refs := func(orphans []coreindex.OrphanCommit) [][]byte {
		var hashes [][]byte
		for _, o := range orphans {
			hashes = append(hashes, o.Ref.Hash)
		}

		return hashes
	}

	planned, err := index.DropOrphans(ctx, cutoff, true)
	require.NoError(t, err)
	require.Equal(t, [][]byte{old.Ref().Hash}, refs(planned))

	orphans, err := index.OrphanCommits(ctx)
	require.NoError(t, err)
	require.Len(t, orphans, 2, "a dry run tombstones nothing")

	dropped, err := index.DropOrphans(ctx, cutoff, false)
	require.NoError(t, err)
	require.Equal(t, [][]byte{old.Ref().Hash}, refs(dropped))

	orphans, err = index.OrphanCommits(ctx)
	require.NoError(t, err)
	require.Equal(t, [][]byte{recent.Ref().Hash}, refs(orphans))

	has, err := packs.Has(ctx, indexed.Ref())
	require.NoError(t, err)
	require.True(t, has, "an indexed commit stays")
}
