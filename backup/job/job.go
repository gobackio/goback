// Package job runs each kind of backup once against a store that is
// already open: a directory, a Postgres base backup or WAL, a MySQL dump or
// binlogs. A one-shot command and a long-running process call the same
// functions.
package job

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/blobcache"
	"github.com/gobackio/goback/backup/statcache"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/storage/badger"
)

// Env is what runs share: the open store and index, the key, the agent
// they commit as, where they log, and the caches. The caller opens and
// closes all of it; a nil cache runs without it.
type Env struct {
	Store backup.ObjectStore
	Index backup.Index
	// Key seals what runs upload; nil backs up in the clear.
	Key     *storekey.Key
	AgentID string
	// Logger is where runs report; nil means slog.Default.
	Logger *slog.Logger
	// Stderr, when set, takes the stderr of the programs and hooks runs
	// start as it comes; nil logs it through Logger line by line.
	Stderr io.Writer

	// StatCache lets Path skip re-reading files whose stat data match.
	StatCache *statcache.Cache
	// ObjectCache keeps the commits and trees Path diffs against.
	ObjectCache *badger.Store
	// TreeCache keeps the commits and trees PostgresReceive and MySQLBinlog
	// read every run.
	TreeCache *blobcache.Cache
	// BlobCache receives every blob Path uploads.
	BlobCache *blobcache.Cache
}

func (e *Env) logger() *slog.Logger {
	if e.Logger != nil {
		return e.Logger
	}

	return slog.Default()
}

// stderr is where program's stderr goes. flush logs what is left of a
// line once program has exited.
func (e *Env) stderr(program string) (w io.Writer, flush func()) {
	if e.Stderr != nil {
		return e.Stderr, func() {}
	}

	l := &lines{logger: e.logger(), program: program}

	return l, l.flush
}

func (e *Env) sessions() backup.SessionStore {
	sessions, _ := e.Store.(backup.SessionStore)

	return sessions
}

// OpenStatCache opens the stat cache under dir, a store's backup cache
// directory. It returns nil, logging a warning, when another process has
// it open.
func OpenStatCache(dir string, logger *slog.Logger) (*statcache.Cache, error) {
	stats, err := statcache.Open(filepath.Join(dir, "stat"))
	switch {
	case errors.Is(err, statcache.ErrLocked):
		logger.Warn("running without the stat cache, which another goback process has open", "dir", dir)
		return nil, nil
	case err != nil:
		return nil, errors.Join(errors.New("opening stat cache"), err)
	}

	return stats, nil
}

// OpenObjectCache opens the object cache under dir, a store's backup cache
// directory. It returns nil, logging a warning, when another process has
// it open.
func OpenObjectCache(dir string, logger *slog.Logger) (*badger.Store, error) {
	objects, err := badger.New(filepath.Join(dir, "objects"))
	switch {
	case errors.Is(err, badger.ErrLocked):
		logger.Warn("running without the object cache, which another goback process has open", "dir", dir)
		return nil, nil
	case err != nil:
		return nil, errors.Join(errors.New("opening object cache"), err)
	}

	return objects, nil
}

// treeCacheSize bounds the commits and trees the tree cache keeps; the
// latest commit's are read every run, so they are never the ones let go.
const treeCacheSize = 256 << 20

// OpenTreeCache opens the tree cache under dir, a store's backup cache
// directory.
func OpenTreeCache(dir string) (*blobcache.Cache, error) {
	trees, err := blobcache.Open(dir, "trees", treeCacheSize)
	if err != nil {
		return nil, fmt.Errorf("opening the tree cache: %w", err)
	}

	return trees, nil
}
