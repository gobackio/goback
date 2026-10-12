package backup

import (
	"context"
	"path"
	"time"

	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"
)

// FindFile lists up to count versions of the file at the slash-separated
// plaintext path in set, none newer than notAfter, newest first, with
// their stats opened under key.
func FindFile(ctx context.Context, idx Index, key *storekey.Key, set, file string, notAfter time.Time, count int) ([]*proto.TreeNode, error) {
	return indexed(key, path.Dir("/"+file), func(key *storekey.Key) ([]*proto.TreeNode, error) {
		return idx.FileInfo(ctx, set, IndexPath(key, file), notAfter, count)
	})
}

// ListDir lists what set held directly under the slash-separated plaintext
// dir at notAfter, as DirLister.ReadDir does, with the stats opened under
// key.
func ListDir(ctx context.Context, lister DirLister, key *storekey.Key, set, dir string, notAfter time.Time) ([]*proto.TreeNode, error) {
	return indexed(key, dir, func(key *storekey.Key) ([]*proto.TreeNode, error) {
		return lister.ReadDir(ctx, set, IndexPath(key, dir), notAfter)
	})
}

// indexed reads entries of dir under the key's sealed path, and under the
// plain path when that finds none: a set that took a key later keeps its
// earlier versions there.
func indexed(key *storekey.Key, dir string, read func(*storekey.Key) ([]*proto.TreeNode, error)) ([]*proto.TreeNode, error) {
	nodes, err := read(key)
	if err != nil {
		return nil, err
	}

	if len(nodes) == 0 && key != nil {
		return read(nil)
	}

	_, parent := indexPath(key, dir)

	return openNodes(key, parent, nodes)
}
