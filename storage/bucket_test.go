package storage

import (
	"testing"

	"github.com/gobackio/goback/storage/pack"
	"github.com/gobackio/goback/storage/pack/packtest"

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
