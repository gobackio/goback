// Package statcache is the per-machine sidecar that remembers what each
// file looked like when it was last read, so a run can detect changes that
// mtime and size alone miss and can be dropped at any time without losing
// correctness.
package statcache

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
	gobadger "github.com/gobackio/goback/storage/badger"

	"github.com/dgraph-io/badger/v4"
)

const (
	keyPrefix   = "stat|"
	recordSize  = 8 + 8 + 8 + 8 + 4 + proto.HashSize
	recordMtime = 0
	recordCtime = 8
	recordInode = 16
	recordSize0 = 24
	recordMode  = 32
	recordRef   = 36
)

// TTL is how long an entry lasts after it was last stored or matched.
const TTL = 30 * 24 * time.Hour

// renewAfter is how much of an entry's TTL a match lets pass before it
// renews it, so a file reused on every run is rewritten at most daily.
const renewAfter = 24 * time.Hour

// Cache is a badger-backed backup.StatCache. An entry neither stored nor
// matched within TTL expires.
type Cache struct {
	db *badger.DB
}

var _ backup.StatCache = (*Cache)(nil)

// ErrLocked is returned by Open when another process has the cache open.
var ErrLocked = errors.New("another process has the cache open")

// Open opens or creates the cache in dir. It fails with ErrLocked while
// another process has it open.
func Open(dir string) (*Cache, error) {
	db, err := badger.Open(gobadger.Options(dir).WithLogger(nil))
	if err != nil && strings.Contains(err.Error(), "Another process is using this Badger database") {
		return nil, fmt.Errorf("%w: %v", ErrLocked, err)
	}

	if err != nil {
		return nil, err
	}

	return &Cache{db: db}, nil
}

// Close closes the database.
func (c *Cache) Close() error {
	return c.db.Close()
}

func encode(info os.FileInfo, ref *proto.Ref) []byte {
	ctime, inode := backup.FileIdentity(info)

	rec := make([]byte, recordSize)
	binary.BigEndian.PutUint64(rec[recordMtime:], uint64(info.ModTime().UnixNano()))
	binary.BigEndian.PutUint64(rec[recordCtime:], uint64(ctime))
	binary.BigEndian.PutUint64(rec[recordInode:], inode)
	binary.BigEndian.PutUint64(rec[recordSize0:], uint64(info.Size()))
	binary.BigEndian.PutUint32(rec[recordMode:], uint32(info.Mode()))
	copy(rec[recordRef:], ref.GetHash())

	return rec
}

func matches(rec []byte, info os.FileInfo, ref *proto.Ref) bool {
	if len(rec) != recordSize {
		return false
	}

	ctime, inode := backup.FileIdentity(info)

	return binary.BigEndian.Uint64(rec[recordMtime:]) == uint64(info.ModTime().UnixNano()) &&
		binary.BigEndian.Uint64(rec[recordCtime:]) == uint64(ctime) &&
		binary.BigEndian.Uint64(rec[recordInode:]) == inode &&
		binary.BigEndian.Uint64(rec[recordSize0:]) == uint64(info.Size()) &&
		binary.BigEndian.Uint32(rec[recordMode:]) == uint32(info.Mode()) &&
		bytes.Equal(rec[recordRef:], ref.GetHash())
}

func key(path string) []byte {
	return append([]byte(keyPrefix), path...)
}

// Lookup implements backup.StatCache.
func (c *Cache) Lookup(path string, info os.FileInfo, ref *proto.Ref) (present bool, matched bool) {
	var renew bool

	_ = c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key(path))
		if err != nil {
			return nil
		}

		present = true
		renew = time.Unix(int64(item.ExpiresAt()), 0).Before(time.Now().Add(TTL - renewAfter))

		return item.Value(func(val []byte) error {
			matched = matches(val, info, ref)
			return nil
		})
	})

	if matched && renew {
		_ = c.Store(path, info, ref)
	}

	return present, matched
}

// Store implements backup.StatCache.
func (c *Cache) Store(path string, info os.FileInfo, ref *proto.Ref) error {
	return c.db.Update(func(txn *badger.Txn) error {
		return txn.SetEntry(badger.NewEntry(key(path), encode(info, ref)).WithTTL(TTL))
	})
}
