package goback

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"testing"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/testing/tests3"

	"github.com/stretchr/testify/require"
)

func TestAnS3StoreKeepsItsArchivesInTheIndexAndCachesUnderTheCacheDir(t *testing.T) {
	ctx := context.Background()
	location := tests3.Start(t, "goback")
	cacheDir := t.TempDir()

	repo, err := Open(ctx, Settings{Index: t.TempDir(), Storage: location, CacheDir: cacheDir})
	require.NoError(t, err)
	require.NotNil(t, repo.Index)
	t.Cleanup(func() { _ = repo.Close() })

	content := bytes.Repeat([]byte("goback"), 1<<16)

	ref, err := backup.PutFile(ctx, repo.Store, nil, int64(len(content)), bytes.NewReader(content))
	require.NoError(t, err)

	reader, err := backup.NewBackupReader(repo.Store).ReadFile(ctx, ref)
	require.NoError(t, err)

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, content, got)

	metadata, err := filepath.Glob(filepath.Join(cacheDir, "stores", "*", "metadata"))
	require.NoError(t, err)
	require.Len(t, metadata, 1, "the bucket's metadata cache sits under the cache directory")
}

func TestABucketURLTakesNoCache(t *testing.T) {
	_, err := Open(context.Background(), Settings{Storage: "s3://bucket?cache=" + t.TempDir(), Index: t.TempDir()})
	require.ErrorContains(t, err, "takes no cache")
}
