package storage

import (
	"io"
	"testing"

	"github.com/gobackio/goback/storage/pack"
	"github.com/gobackio/goback/storage/pack/packtest"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/fileblob"
)

func TestBucketStore(t *testing.T) {
	dir := t.TempDir()

	bucket, err := fileblob.OpenBucket(dir, nil) //memblob.OpenBucket(nil)
	if err != nil {
		t.Fatal(err)
	}

	packtest.TestArchiveStorage(t, NewBucketStore(bucket))

	t.Log(dir)
}

func TestABucketStoreWhoseMetadataCacheIsOpenElsewhereRunsWithoutIt(t *testing.T) {
	bucket, err := fileblob.OpenBucket(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}

	cache := t.TempDir()

	first, err := NewBucketObjectStore(bucket, pack.NewInMemoryIndex(), cache, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	second, err := NewBucketObjectStore(bucket, pack.NewInMemoryIndex(), cache, 0)
	if err != nil {
		t.Fatalf("the second store failed instead of running without the cache: %v", err)
	}
	defer second.Close()
}

func TestClosingAFileOpenedWhileItUploadsLeavesTheUploadAlone(t *testing.T) {
	bucket, err := fileblob.OpenBucket(t.TempDir(), nil)
	require.NoError(t, err)

	store := NewBucketStore(bucket)

	writer, err := store.Create("7.head")
	require.NoError(t, err)

	reader, err := store.Open("7.head")
	require.NoError(t, err)

	_, err = reader.Read(make([]byte, 1))
	require.Error(t, err, "the file is not stored yet")
	require.NoError(t, reader.Close())

	_, err = writer.Write([]byte("head"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	stored, err := store.Open("7.head")
	require.NoError(t, err)
	defer stored.Close()

	data, err := io.ReadAll(stored)
	require.NoError(t, err)
	require.Equal(t, "head", string(data))
}
