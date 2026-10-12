package backup_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/index/sql"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage"
	"github.com/gobackio/goback/storage/pack"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"
)

func names(nodes []*proto.TreeNode) []string {
	out := make([]string, len(nodes))
	for i, node := range nodes {
		out[i] = string(node.GetStat().GetName())
	}

	return out
}

func TestFindFileAndListDirReadASealedSetByItsPlainPaths(t *testing.T) {
	ctx := context.Background()

	index := sql.NewMemory(t.Name(), nil)
	packs, err := pack.NewPackStorage(pack.WithArchiveStorage(storage.NewBucketStore(memblob.OpenBucket(nil))), pack.WithArchiveIndex(index))
	require.NoError(t, err)
	index.ObjectStore = packs
	require.NoError(t, index.Open())
	require.NoError(t, packs.Open())
	t.Cleanup(func() { _ = packs.Close() })

	key, err := storekey.Generate("test")
	require.NoError(t, err)

	src := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(src, "world", "region"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "world", "level.dat"), []byte("level"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(src, "world", "region", "r.0.0.mca"), []byte("region"), 0o644))

	walker := &backup.Walker{Index: index, Objects: index, Key: key, Set: "world", AgentID: "a", Root: src, Workers: 1}
	_, err = walker.Run(ctx)
	require.NoError(t, err)

	now := time.Now().Add(time.Hour)

	plain, err := index.FileInfo(ctx, "world", "world/region/r.0.0.mca", now, 10)
	require.NoError(t, err)
	require.Empty(t, plain, "the index holds the sealed path")

	found, err := backup.FindFile(ctx, index, key, "world", "world/region/r.0.0.mca", now, 10)
	require.NoError(t, err)
	require.Equal(t, []string{"r.0.0.mca"}, names(found))

	listed, err := backup.ListDir(ctx, index, key, "world", "world", now)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"level.dat", "region"}, names(listed))

	missing, err := backup.FindFile(ctx, index, key, "world", "world/gone.dat", now, 10)
	require.NoError(t, err)
	require.Empty(t, missing)
}
