package pack

import (
	"io"
	"log/slog"
	"time"

	"github.com/gobackio/goback/backup"
)

type packOptions struct {
	owned           []io.Closer
	compaction      CompactionConfig
	maxParallel     uint
	maxSize         uint64
	closeBeforeRead bool
	storage         ArchiveStorage
	index           ArchiveIndex
	cache           backup.ObjectStore
	idleFinalize    time.Duration
	sessionLease    time.Duration
	claimOpen       time.Duration
	claimGrace      time.Duration
	localSessions   bool
	foreignRefs     bool
	atRestKey       *AtRestKey
	observer        ArchiveObserver
	logger          *slog.Logger
	indexCache      string
}

// ArchiveObserver is told about every archive the store puts into its
// storage and every one it deletes from there, so a deployment can
// account for what the storage holds. It is called on the path that
// stored or deleted the archive, so it must be quick.
type ArchiveObserver interface {
	// ArchiveStored is told an archive and its index are in the storage,
	// the bytes both take up and the session that wrote them; the session
	// is empty for an archive the store rewrote itself.
	ArchiveStored(name string, bytes int64, session string)
	// ArchiveDeleted is told an archive's files are gone from the storage.
	ArchiveDeleted(name string)
}

// ArchiveBatchObserver is an ArchiveObserver told about archives deleted
// together in one call, in place of a call to ArchiveDeleted for each.
type ArchiveBatchObserver interface {
	ArchiveObserver
	// ArchivesDeleted is told the archives' files are gone from the storage.
	ArchivesDeleted(names []string)
}

// WithArchiveObserver tells observer about every archive stored and
// deleted.
func WithArchiveObserver(observer ArchiveObserver) PackOption {
	return func(p *packOptions) {
		p.observer = observer
	}
}

// WithAtRestKey seals every payload written from now on under the key
// and opens the ones sealed under any key of its keyset; records written
// without a key stay readable. A rewrite re-seals what it opens under the
// primary key.
func WithAtRestKey(key *AtRestKey) PackOption {
	return func(p *packOptions) {
		p.atRestKey = key
	}
}

// WithLogger is where the store reports; the default is slog.Default.
func WithLogger(logger *slog.Logger) PackOption {
	return func(p *packOptions) {
		p.logger = logger
	}
}

// WithIdleFinalize finalizes a session's open archive after it has not
// been written to for the given duration.
func WithIdleFinalize(d time.Duration) PackOption {
	return func(p *packOptions) {
		p.idleFinalize = d
	}
}

// WithSessionLease ends sessions that have not written for the given
// duration, dropping what they did not commit.
func WithSessionLease(d time.Duration) PackOption {
	return func(p *packOptions) {
		p.sessionLease = d
	}
}

// WithClaims sets how long an archive of a session stays open, and how
// much longer its claim holds while the archive is closed and verified,
// when the index is a ClaimIndex. A commit waits up to their sum for an
// archive another process holds open.
func WithClaims(open, grace time.Duration) PackOption {
	return func(p *packOptions) {
		p.claimOpen = open
		p.claimGrace = grace
	}
}

// PackOption configures NewPackStorage.
type PackOption func(p *packOptions)

// CompactionConfig tunes Compact; when it runs is the caller's business.
type CompactionConfig struct {
	// Small is the size under which a committed archive is merged; zero
	// means 16 MiB.
	Small uint64
	// Batch is how much the small archives must add up to before Compact
	// merges them; zero means 256 MiB.
	Batch uint64
	// MinimumCandidates is how many small archives make Compact merge them
	// whatever they add up to.
	MinimumCandidates int
	// Workers is how many archives a rewrite, by Compact or Collect, reads
	// at once; zero means four per GOMAXPROCS, at most 16. Compact gives
	// each its own output, Collect each set and age of what survives.
	Workers int
	// Chunk is how many archives a rewrite takes at a time, retiring each
	// chunk as it finishes; zero means 1000.
	Chunk int
}

// WithCompaction tunes Compact.
func WithCompaction(config CompactionConfig) PackOption {
	return func(p *packOptions) {
		p.compaction = config
	}
}

// WithMaxParallel bounds the archives open for writing at once.
func WithMaxParallel(max uint) PackOption {
	return func(p *packOptions) {
		p.maxParallel = max
	}
}

// WithMaxSize is the size at which an open archive is finalized.
func WithMaxSize(max uint64) PackOption {
	return func(p *packOptions) {
		p.maxSize = max
	}
}

// WithArchiveStorage is where the archives live; required.
func WithArchiveStorage(storage ArchiveStorage) PackOption {
	return func(p *packOptions) {
		p.storage = storage
	}
}

// WithLocalSessions says only this process writes the store's sessions, so
// a Put under one returns once its index row is queued rather than
// written. Every row is in before the archive is finalized. A store that
// several processes serve must not use it.
func WithLocalSessions() PackOption {
	return func(p *packOptions) {
		p.localSessions = true
	}
}

// WithForeignRefs lets a commit reference objects the store doesn't hold,
// for a store in front of another that holds them: its commit checks
// nothing.
func WithForeignRefs() PackOption {
	return func(p *packOptions) {
		p.foreignRefs = true
	}
}

// WithCloseBeforeRead finalizes an open archive before an object is read
// from it, for storages that cannot read an upload in flight.
func WithCloseBeforeRead(do bool) PackOption {
	return func(p *packOptions) {
		p.closeBeforeRead = do
	}
}

// WithMetadataCache keeps commits, trees and files in cache as well.
func WithMetadataCache(cache backup.ObjectStore) PackOption {
	return func(p *packOptions) {
		p.cache = cache
	}
}

// WithOwned hands the store things it closes after itself, such as an
// archive index nobody else holds.
func WithOwned(closers ...io.Closer) PackOption {
	return func(p *packOptions) {
		p.owned = append(p.owned, closers...)
	}
}

// WithArchiveIndex is the index of archives and sessions; required.
func WithArchiveIndex(index ArchiveIndex) PackOption {
	return func(p *packOptions) {
		p.index = index
	}
}
