package common

import (
	"log"

	"github.com/gobackio/goback"
	"github.com/gobackio/goback/backup/blobcache"

	"github.com/urfave/cli"
)

// BlobCache opens the repo's blob cache under --cache-dir, or exits. It
// returns nil when --cache-dir is unset.
func BlobCache(c *cli.Context, repo *goback.Repo) *blobcache.Cache {
	cache, err := repo.BlobCache(Context(c))
	if err != nil {
		Fatal(err)
	}

	return cache
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
