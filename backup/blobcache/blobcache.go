// Package blobcache keeps copies of stored objects on local disk, keyed by
// ref and verified on every read.
package blobcache

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/gobackio/goback/proto"

	pb "google.golang.org/protobuf/proto"
)

// Cache is one store's blob directory. It is safe for concurrent use by
// several processes: entries are written next to their final name and
// renamed into place.
type Cache struct {
	dir   string
	limit int64
}

// Open returns the cache of one store under dir. limit caps the cache in
// bytes when Sweep runs; 0 leaves it unbounded.
func Open(dir, storeID string, limit int64) (*Cache, error) {
	c := &Cache{dir: filepath.Join(dir, storeID), limit: limit}

	return c, os.MkdirAll(c.dir, 0o755)
}

// Dir is the cache directory.
func (c *Cache) Dir() string { return c.dir }

func (c *Cache) path(ref *proto.Ref) string {
	name := fmt.Sprintf("%x", ref.Hash)

	return filepath.Join(c.dir, name[:2], name)
}

// Get returns the cached object for ref after checking that it still
// hashes to it. A corrupt entry is removed.
func (c *Cache) Get(ref *proto.Ref) (*proto.Object, bool) {
	path := c.path(ref)

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}

	stored := &proto.Stored{}
	err = pb.Unmarshal(data, stored)

	var obj *proto.Object
	if err == nil {
		obj, err = stored.Object()
	}

	if err == nil {
		err = obj.Verify(ref)
	}

	if err != nil {
		_ = os.Remove(path)
		return nil, false
	}

	now := time.Now()
	_ = os.Chtimes(path, now, now)

	return obj, true
}

// Put stores the object under ref unless it is already there.
func (c *Cache) Put(ref *proto.Ref, obj *proto.Object) error {
	path := c.path(ref)

	if _, err := os.Stat(path); err == nil {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	stored, err := obj.Encoded()
	if err != nil {
		return err
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), ".put-*")
	if err != nil {
		return err
	}

	_, err = tmp.Write(proto.Bytes(stored))
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}

	if err == nil {
		err = os.Rename(tmp.Name(), path)
	}

	if err != nil {
		_ = os.Remove(tmp.Name())

		if _, statErr := os.Stat(path); statErr == nil {
			return nil
		}
	}

	return err
}

// Drop removes the entry for ref.
func (c *Cache) Drop(ref *proto.Ref) {
	_ = os.Remove(c.path(ref))
}

type entry struct {
	path    string
	size    int64
	modTime time.Time
}

// Sweep removes the least recently used entries until the cache fits its
// limit and returns how many it removed.
func (c *Cache) Sweep() (int, error) {
	if c.limit <= 0 {
		return 0, nil
	}

	var entries []entry
	var total int64

	err := filepath.WalkDir(c.dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}

		info, err := d.Info()
		if err != nil {
			return nil
		}

		entries = append(entries, entry{path: path, size: info.Size(), modTime: info.ModTime()})
		total += info.Size()

		return nil
	})
	if err != nil {
		return 0, err
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].modTime.Before(entries[j].modTime) })

	removed := 0
	for _, e := range entries {
		if total <= c.limit {
			break
		}

		if os.Remove(e.path) == nil {
			total -= e.size
			removed++
		}
	}

	return removed, nil
}
