package common

import (
	"crypto/sha256"
	"fmt"
	"path/filepath"

	"github.com/dustin/go-humanize"
	"github.com/urfave/cli"
)

// StoreCache is the directory under --cache-dir that holds the caches of
// the store --storage names, or "" when --cache-dir is unset and nothing
// is cached.
func StoreCache(c *cli.Context, name string) string {
	return Settings(c).StoreCache(name)
}

// StoreCache is the directory under CacheDir that holds the caches of the
// Storage location, or "" when CacheDir is unset and nothing is cached.
func (s StoreSettings) StoreCache(name string) string {
	if s.CacheDir == "" {
		return ""
	}

	return filepath.Join(s.CacheDir, "stores", s.storageID(), name)
}

// MetadataCacheSize is the --metadata-cache-size in bytes, or exits.
func MetadataCacheSize(c *cli.Context) int64 {
	size, err := Settings(c).MetadataCacheLimit()
	if err != nil {
		Fatal(err)
	}

	return size
}

// MetadataCacheLimit parses MetadataCacheSize into bytes.
func (s StoreSettings) MetadataCacheLimit() (int64, error) {
	size, err := humanize.ParseBytes(s.MetadataCacheSize)
	if err != nil {
		return 0, fmt.Errorf("invalid --metadata-cache-size: %w", err)
	}

	return int64(size), nil
}

// BlobCacheLimit parses BlobCacheSize into bytes.
func (s StoreSettings) BlobCacheLimit() (int64, error) {
	size, err := humanize.ParseBytes(s.BlobCacheSize)
	if err != nil {
		return 0, fmt.Errorf("invalid --blob-cache-size: %w", err)
	}

	return int64(size), nil
}

func (s StoreSettings) storageID() string {
	sum := sha256.Sum256([]byte(s.Storage))

	return fmt.Sprintf("%x", sum[:8])
}
