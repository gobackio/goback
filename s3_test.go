package goback

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/testing/tests3"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob"
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

func TestStoresAtTwoPathsOfOneBucketKeepApart(t *testing.T) {
	ctx := context.Background()
	location := tests3.Start(t, "goback")

	at := func(path string) string {
		return strings.Replace(location, "s3://goback?", "s3://goback/"+path+"?", 1)
	}

	content := bytes.Repeat([]byte("web1"), 1<<14)

	web1, err := Open(ctx, Settings{Index: t.TempDir(), Storage: at("web1")})
	require.NoError(t, err)

	ref, err := backup.PutFile(ctx, web1.Store, nil, int64(len(content)), bytes.NewReader(content))
	require.NoError(t, err)
	require.NoError(t, web1.Close())

	web2, err := Open(ctx, Settings{Index: t.TempDir(), Storage: at("web2")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = web2.Close() })

	_, err = web2.Store.Get(ctx, ref)
	require.ErrorIs(t, err, backup.ErrNotFound, "the other path's store is another store")

	root, err := blob.OpenBucket(ctx, location)
	require.NoError(t, err)
	t.Cleanup(func() { _ = root.Close() })

	listed := root.List(nil)
	for {
		object, err := listed.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(object.Key, "web1/") || strings.HasPrefix(object.Key, "web2/"), object.Key)
	}
}

func TestABucketURLTakesNoCache(t *testing.T) {
	_, err := Open(context.Background(), Settings{Storage: "s3://bucket?cache=" + t.TempDir(), Index: t.TempDir()})
	require.ErrorContains(t, err, "takes no cache")
}
