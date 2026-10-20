// Package goback opens a store from the location the goback CLI takes:
// its objects, its index, its key and the caches under a cache directory.
// The API is not stable yet and may change between minor versions.
package goback

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/blobcache"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/storage/pack"
	"github.com/gobackio/goback/storage/wrapped"
)

// The cache sizes a zero Settings field stands for.
const (
	DefaultMetadataCacheSize = 1 << 30
	DefaultBlobCacheSize     = 4_000_000_000
)

// Settings say where a store lives and how to open it. Storage is
// required, and Index for a directory or bucket store; the rest may be
// left zero.
type Settings struct {
	// Storage is the store's location: a directory, gs:// or s3:// with
	// the bucket's query parameters, or goback:// and goback+insecure://
	// for a store server.
	Storage string
	// Index is where a directory or bucket store keeps its index: a
	// directory for SQLite, or a postgres:// URL. A store server brings
	// its own.
	Index string
	// AgentID is who commits and who a store server is told is calling;
	// empty means the hostname.
	AgentID string
	// StoreKey is the store key's file. Without it, Passphrase opens the
	// copy the store keeps escrowed; without either, the store is written
	// in the clear.
	StoreKey   string
	Passphrase string
	// AtRestKey is the file of the key a directory or bucket store
	// encrypts its archives with.
	AtRestKey string
	// Behind is a store that a directory or bucket store stands in front
	// of and publishes to, which checks references when what it holds
	// gets there: its index leaves a Put's references unchecked, and
	// reads the trees a commit names from Behind where it holds nothing
	// itself, so its commits may refer to what only Behind holds. A Put
	// doesn't wait for its index row either, so nothing else may serve the
	// store.
	Behind backup.ObjectStore
	// CacheDir holds every cache, apart per store; empty caches nothing.
	CacheDir string
	// MetadataCacheSize bounds a bucket's metadata cache in bytes; zero
	// means DefaultMetadataCacheSize and a negative size no bound.
	MetadataCacheSize int64
	// BlobCacheSize is what BlobCache is trimmed to in bytes; zero means
	// DefaultBlobCacheSize and a negative size no bound.
	BlobCacheSize int64
	// Logger is where opening reports; nil means slog.Default.
	Logger *slog.Logger
}

func (s Settings) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}

	return slog.Default()
}

// Repo is an open store. Close it when done.
type Repo struct {
	Store backup.ObjectStore
	Index backup.Index

	settings Settings

	keyOnce sync.Once
	key     *storekey.Key
	keyErr  error
}

// Open opens the store s names and its index.
func Open(ctx context.Context, s Settings) (*Repo, error) {
	if s.Storage == "" {
		return nil, errors.New("no storage location: where the objects live")
	}

	if s.AgentID == "" {
		s.AgentID, _ = os.Hostname()
	}

	u, err := parseLocation(s.Storage)
	if err != nil {
		return nil, fmt.Errorf("invalid storage location %s: %w", s.Storage, err)
	}

	driver, ok := drivers[u.Scheme]
	if !ok {
		return nil, fmt.Errorf("no driver for storage location %s", u.String())
	}

	r := &Repo{settings: s}

	r.Store, r.Index, err = driver(ctx, u, s)
	if err != nil {
		return nil, err
	}

	if op, ok := r.Store.(interface{ Open() error }); ok {
		if err := op.Open(); err != nil {
			return nil, fmt.Errorf("opening the store: %w", err)
		}
	}

	return r, nil
}

// Key is the store key: loaded from Settings.StoreKey, else recovered with
// Settings.Passphrase from the copy the store keeps escrowed, else nil for
// a store written in the clear. It is read once, on the first call.
func (r *Repo) Key(ctx context.Context) (*storekey.Key, error) {
	r.keyOnce.Do(func() {
		switch {
		case r.settings.StoreKey != "":
			r.key, r.keyErr = storekey.Load(r.settings.StoreKey)
			if r.keyErr != nil {
				r.keyErr = fmt.Errorf("loading the store key: %w", r.keyErr)
			}
		case r.settings.Passphrase != "":
			escrow, ok := wrapped.Innermost(r.Store).(backup.EscrowLister)
			if !ok {
				r.keyErr = fmt.Errorf("store %T keeps no escrowed key", wrapped.Innermost(r.Store))
				return
			}

			r.key, r.keyErr = backup.RecoverKey(ctx, escrow, r.settings.Passphrase)
			if r.keyErr != nil {
				r.keyErr = fmt.Errorf("opening the escrowed store key: %w", r.keyErr)
			}
		}
	})

	return r.key, r.keyErr
}

// StoreCache is the directory under Settings.CacheDir that holds this
// store's caches of that name, or "" when nothing is cached.
func (r *Repo) StoreCache(name string) string {
	return r.settings.storeCache(name)
}

func (s Settings) storeCache(name string) string {
	if s.CacheDir == "" {
		return ""
	}

	return filepath.Join(s.CacheDir, "stores", s.storageID(), name)
}

func (s Settings) storageID() string {
	sum := sha256.Sum256([]byte(s.Storage))

	return fmt.Sprintf("%x", sum[:8])
}

// BlobCache opens the cache of the blobs backups upload and restores read,
// under Settings.CacheDir and apart per store key, or returns nil when
// nothing is cached.
func (r *Repo) BlobCache(ctx context.Context) (*blobcache.Cache, error) {
	if r.settings.CacheDir == "" {
		return nil, nil
	}

	key, err := r.Key(ctx)
	if err != nil {
		return nil, err
	}

	storeID := r.settings.storageID()
	if key != nil {
		storeID = key.IDString()
	}

	limit := r.settings.BlobCacheSize
	if limit == 0 {
		limit = DefaultBlobCacheSize
	}

	cache, err := blobcache.Open(filepath.Join(r.settings.CacheDir, "blobs"), storeID, limit)
	if err != nil {
		return nil, fmt.Errorf("opening the blob cache: %w", err)
	}

	return cache, nil
}

// Compact rewrites a directory or bucket store's sparse archives; it
// does nothing for a store server, which compacts its own.
func (r *Repo) Compact(ctx context.Context) (*pack.CompactReport, error) {
	c, ok := r.Store.(interface {
		Compact(context.Context) (*pack.CompactReport, error)
	})
	if !ok {
		return &pack.CompactReport{}, nil
	}

	return c.Compact(ctx)
}

// Close closes the store, then its index, which a directory or bucket
// store's last writes still go to.
func (r *Repo) Close() error {
	var errs []error

	if c, ok := r.Store.(interface{ Close() error }); ok {
		errs = append(errs, c.Close())
	}

	if r.Index != nil && any(r.Index) != any(r.Store) {
		errs = append(errs, r.Index.Close())
	}

	return errors.Join(errs...)
}
