package common

import (
	"fmt"
	"log"
	"path/filepath"

	"github.com/gobackio/goback/backup/blobcache"
	"github.com/gobackio/goback/backup/storekey"

	"github.com/urfave/cli"
)

// BlobCache opens the blob cache under --cache-dir for the store the
// command works against, or exits. It returns nil when --cache-dir is
// unset.
func BlobCache(c *cli.Context, key *storekey.Key) *blobcache.Cache {
	cache, err := Settings(c).BlobCache(key)
	if err != nil {
		Fatal(err)
	}

	return cache
}

// BlobCache opens the blob cache under CacheDir for the store, keyed by
// key when the store is sealed. It returns nil when CacheDir is unset.
func (s StoreSettings) BlobCache(key *storekey.Key) (*blobcache.Cache, error) {
	if s.CacheDir == "" {
		return nil, nil
	}

	limit, err := s.BlobCacheLimit()
	if err != nil {
		return nil, err
	}

	storeID := s.storageID()
	if key != nil {
		storeID = key.IDString()
	}

	cache, err := blobcache.Open(filepath.Join(s.CacheDir, "blobs"), storeID, limit)
	if err != nil {
		return nil, fmt.Errorf("opening blob cache: %w", err)
	}

	return cache, nil
}

// SweepBlobCache trims the cache to its size limit and logs the result.
func SweepBlobCache(cache *blobcache.Cache) {
	if cache == nil {
		return
	}

	removed, err := cache.Sweep()
	if err != nil {
		log.Printf("sweeping blob cache: %v", err)
	}

	if removed > 0 {
		log.Printf("Evicted %d blobs from the cache", removed)
	}
}
