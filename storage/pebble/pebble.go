// Package pebble is a backup.ObjectStore on a local pebble database, for
// caches that should hold little memory.
package pebble

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/vfs"
	"go.opentelemetry.io/otel"
	"google.golang.org/protobuf/encoding/protowire"
)

// ErrLocked is returned by New when another process has the Store open.
var ErrLocked = errors.New("another process has the store open")

// New opens or creates a Store at path. It fails with ErrLocked while
// another process has it open.
func New(path string, with ...Option) (*Store, error) {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, err
	}

	lock, err := pebble.LockDirectory(path, vfs.Default)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrLocked, err)
	}

	cache := pebble.NewCache(16 << 20)
	defer cache.Unref()

	opts := &pebble.Options{
		Cache:                       cache,
		Lock:                        lock,
		Logger:                      slogLogger{},
		MemTableSize:                8 << 20,
		MemTableStopWritesThreshold: 2,
	}
	opts.EnsureDefaults()

	for l := range opts.Levels {
		opts.Levels[l].FilterPolicy = bloom.FilterPolicy(10)
	}

	db, err := pebble.Open(path, opts)
	if err != nil {
		_ = lock.Close()
		return nil, err
	}

	s := &Store{db: db, lock: lock}
	for _, option := range with {
		option(s)
	}

	if s.lru != nil {
		if err := s.lru.load(db); err != nil {
			_ = s.close()
			return nil, fmt.Errorf("loading the cache's last uses: %w", err)
		}
	}

	return s, nil
}

// Store keeps objects in pebble, keyed by ref. Each value is the object
// type, the length-prefixed object header and the stored bytes. Every
// object has a used key beside it holding its size and last use.
type Store struct {
	db   *pebble.DB
	lock *pebble.Lock
	lru  *lru
}

var _ backup.ObjectStore = (*Store)(nil)

var tracer = otel.Tracer("goback.io/storage/pebble")

const (
	objectKeyPrefix = "objects|"
	usedKeyPrefix   = "used|"
)

func objectKey(hash []byte) []byte {
	return append([]byte(objectKeyPrefix), hash...)
}

func usedKey(hash []byte) []byte {
	return append([]byte(usedKeyPrefix), hash...)
}

// prefixEnd is the first key after every key starting with prefix.
func prefixEnd(prefix string) []byte {
	end := []byte(prefix)
	end[len(end)-1]++

	return end
}

func encodeEntry(hdr *proto.ObjectHeader, stored []byte) []byte {
	hdrBytes := proto.Bytes(hdr)
	val := []byte{byte(hdr.Type)}
	val = protowire.AppendVarint(val, uint64(len(hdrBytes)))
	val = append(val, hdrBytes...)

	return append(val, stored...)
}

func objectFromEntry(val []byte, ref *proto.Ref) (*proto.Object, error) {
	if len(val) == 0 {
		return nil, fmt.Errorf("corrupt entry for %x", ref.Hash)
	}

	val = val[1:]

	hdrLen, n := protowire.ConsumeVarint(val)
	if n < 0 || uint64(len(val)-n) < hdrLen {
		return nil, fmt.Errorf("corrupt entry for %x", ref.Hash)
	}

	hdr, err := proto.NewObjectHeaderFromBytes(val[n : n+int(hdrLen)])
	if err != nil {
		return nil, err
	}

	if !hdr.Ref.Equal(ref) {
		return nil, proto.ErrRefMismatch
	}

	return proto.ObjectFromStored(hdr, val[n+int(hdrLen):])
}

// Put implements backup.ObjectStore.
func (s *Store) Put(ctx context.Context, object *proto.Object) error {
	_, span := tracer.Start(ctx, "PebbleStore.Put")
	defer span.End()

	hdr, stored, err := proto.HeaderFor(object)
	if err != nil {
		return err
	}

	if s.lru != nil {
		return s.lru.put(s.db, hdr.Ref.Hash, encodeEntry(hdr, stored))
	}

	return putEntry(s.db, hdr.Ref.Hash, encodeEntry(hdr, stored), 0)
}

// putEntry writes an object's entry with its used key, as last used at
// used.
func putEntry(db *pebble.DB, hash, entry []byte, used int64) error {
	key := objectKey(hash)

	batch := db.NewBatch()
	defer batch.Close()

	if err := batch.Set(key, entry, nil); err != nil {
		return err
	}

	if err := batch.Set(usedKey(hash), encodeUse(used, int64(len(key)+len(entry))), nil); err != nil {
		return err
	}

	return batch.Commit(pebble.NoSync)
}

// Get implements backup.ObjectStore.
func (s *Store) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	_, span := tracer.Start(ctx, "PebbleStore.Get")
	defer span.End()

	val, closer, err := s.db.Get(objectKey(ref.Hash))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, backup.ErrNotFound
	}

	if err != nil {
		return nil, err
	}

	// the object keeps its stored bytes, which pebble reuses once closed
	obj, err := objectFromEntry(bytes.Clone(val), ref)
	_ = closer.Close()

	if err != nil {
		return nil, err
	}

	if s.lru != nil {
		s.lru.use(s.db, ref.Hash)
	}

	return obj, nil
}

// Delete implements backup.ObjectStore.
func (s *Store) Delete(ctx context.Context, ref *proto.Ref) error {
	_, span := tracer.Start(ctx, "PebbleStore.Delete")
	defer span.End()

	if s.lru != nil {
		return s.lru.delete(s.db, ref.Hash)
	}

	return deleteEntry(s.db, ref.Hash)
}

func deleteEntry(db *pebble.DB, hash []byte) error {
	batch := db.NewBatch()
	defer batch.Close()

	if err := batch.Delete(objectKey(hash), nil); err != nil {
		return err
	}

	if err := batch.Delete(usedKey(hash), nil); err != nil {
		return err
	}

	return batch.Commit(pebble.NoSync)
}

// Walk implements backup.ObjectStore.
func (s *Store) Walk(ctx context.Context, load bool, filterFor proto.ObjectType, receiver backup.ObjectReceiver) error {
	ctx, span := tracer.Start(ctx, "PebbleStore.Walk")
	defer span.End()

	it, err := s.db.NewIterWithContext(ctx, &pebble.IterOptions{
		LowerBound: []byte(objectKeyPrefix),
		UpperBound: prefixEnd(objectKeyPrefix),
	})
	if err != nil {
		return err
	}

	for it.First(); it.Valid(); it.Next() {
		val, err := it.ValueAndErr()
		if err != nil {
			return errors.Join(err, it.Close())
		}

		if len(val) == 0 || proto.ObjectType(val[0]) != filterFor {
			continue
		}

		var obj *proto.Object
		if load {
			ref := &proto.Ref{Hash: bytes.Clone(it.Key()[len(objectKeyPrefix):])}

			obj, err = objectFromEntry(bytes.Clone(val), ref)
			if err != nil {
				return errors.Join(err, it.Close())
			}
		}

		if err := receiver(obj); err != nil {
			return errors.Join(err, it.Close())
		}
	}

	return it.Close()
}

// Has implements backup.ObjectStore.
func (s *Store) Has(ctx context.Context, ref *proto.Ref) (bool, error) {
	_, span := tracer.Start(ctx, "PebbleStore.Has")
	defer span.End()

	_, closer, err := s.db.Get(objectKey(ref.Hash))
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, closer.Close()
}

// Close releases the database.
func (s *Store) Close() error {
	if s.lru != nil {
		s.lru.flush(s.db)
	}

	return s.close()
}

func (s *Store) close() error {
	return errors.Join(s.db.Close(), s.lock.Close())
}
