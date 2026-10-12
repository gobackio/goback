package common

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
	location := tests3.Start(t, "goback")
	u := parse(t, location)
	cacheDir := t.TempDir()

	store, err := initS3(u, StoreSettings{Index: t.TempDir(), Storage: location, CacheDir: cacheDir, MetadataCacheSize: "1GiB"})
	require.NoError(t, err)
	require.Contains(t, storeIndexes, store)
	t.Cleanup(func() { CloseStore(store) })

	ctx := context.Background()
	content := bytes.Repeat([]byte("goback"), 1<<16)

	ref, err := backup.PutFile(ctx, store, nil, int64(len(content)), bytes.NewReader(content))
	require.NoError(t, err)

	reader, err := backup.NewBackupReader(store).ReadFile(ctx, ref)
	require.NoError(t, err)

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, content, got)

	metadata, err := filepath.Glob(filepath.Join(cacheDir, "stores", "*", "metadata"))
	require.NoError(t, err)
	require.Len(t, metadata, 1, "the bucket's metadata cache sits under --cache-dir")
}

func TestABucketURLTakesNoCache(t *testing.T) {
	_, err := initS3(parse(t, "s3://bucket?cache="+t.TempDir()), StoreSettings{Index: t.TempDir()})
	require.ErrorContains(t, err, "--cache-dir")
}
