package commit

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/index/sql"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage"
	"github.com/gobackio/goback/storage/pack"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"
)

func TestAnUnretiredCommitSurvivesCollectionsAndRestores(t *testing.T) {
	ctx := context.Background()

	index := sql.NewMemory(t.Name(), nil)
	packs, err := pack.NewPackStorage(pack.WithArchiveStorage(storage.NewBucketStore(memblob.OpenBucket(nil))), pack.WithArchiveIndex(index))
	require.NoError(t, err)
	index.ObjectStore = packs
	require.NoError(t, index.Open())
	require.NoError(t, packs.Open())
	t.Cleanup(func() { _ = packs.Close() })

	src := t.TempDir()
	backupOf := func(content string) *proto.Ref {
		require.NoError(t, os.WriteFile(filepath.Join(src, "level.dat"), []byte(content), 0o644))

		walker := &backup.Walker{Index: index, Objects: index, Set: "world", AgentID: "a", Root: src, Workers: 1}
		result, err := walker.Run(ctx)
		require.NoError(t, err)

		return result.Ref
	}

	old := backupOf("the old world")
	backupOf("the new world")

	require.NoError(t, index.SetPolicy(ctx, "world", &retention.Policy{KeepLast: 1}))
	n, err := index.Retire(ctx, time.Now())
	require.NoError(t, err)
	require.Equal(t, 1, n)

	collect := func(ahead time.Duration) {
		t.Helper()

		report, err := packs.Collect(ctx, pack.CollectOptions{Now: time.Now().Add(ahead), TempDir: t.TempDir(), DeadRatio: 1e-9})
		require.NoError(t, err)
		require.Contains(t, []string{"", "first generation"}, report.SweepSkipped)
	}

	collect(0)

	require.NoError(t, index.SetPolicy(ctx, "world", &retention.Policy{KeepLast: 10}))
	done, err := index.UnretireCommits(ctx, []*proto.Ref{old}, false)
	require.NoError(t, err)
	require.Len(t, done, 1)
	require.True(t, done[0].Whole())
	require.NotEmpty(t, done[0].RetainedBy)

	for _, ahead := range []time.Duration{48 * time.Hour, 96 * time.Hour, 144 * time.Hour} {
		collect(ahead)
	}

	dst := t.TempDir()
	c := &commit{
		ctx:              ctx,
		index:            index,
		store:            index,
		set:              "world",
		ref:              old,
		restorer:         &backup.Restorer{Store: index, Workers: 2, Overwrite: backup.OverwriteIfChanged},
		progressInterval: time.Hour,
		base:             dst,
	}
	require.NoError(t, c.restore())

	restored, err := os.ReadFile(filepath.Join(dst, "level.dat"))
	require.NoError(t, err)
	require.Equal(t, "the old world", string(restored))
}
