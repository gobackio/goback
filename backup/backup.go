// Package backup writes and reads commits: a Walker backs a directory up
// into a set as a commit of trees, files and chunks, and a Restorer writes
// a commit, a subtree or one file back, downloading only what the
// destination lacks. Both work against an ObjectStore and an Index, which
// package goback opens from a storage location.
package backup

import (
	"context"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"
)

var (
	// ErrEmptyBuffer is returned when trying to read from a backup file into
	// an empty buffer
	ErrEmptyBuffer = errors.New("no buffer space provided")

	// ErrIllegalOffset is returned when trying to seek past the end of a file
	ErrIllegalOffset = errors.New("illegal offset")

	// ErrSkipFile can be returned during backup to make the TreeWrite skip/ignore a file
	ErrSkipFile = errors.New("skip file")
)

// BackupReader reads the files and trees of a commit back from a store.
type BackupReader struct {
	store ObjectStore
	key   *storekey.Key
}

// NewBackupReader returns a reader for a plaintext store; WithKey adds the
// store key.
func NewBackupReader(store ObjectStore) *BackupReader {
	return &BackupReader{
		store: store,
	}
}

// WithKey returns a reader that opens names and blobs with the store key.
func (br *BackupReader) WithKey(key *storekey.Key) *BackupReader {
	return &BackupReader{store: br.store, key: key}
}

// ForCommit returns a reader for commit's tree: one that opens it with the
// store key when the commit was sealed under it, and plain when it was
// written without one.
func (br *BackupReader) ForCommit(ctx context.Context, commit *proto.Commit) (*BackupReader, error) {
	key, err := CommitKey(ctx, br.store, commit, br.key)
	if err != nil {
		return nil, err
	}

	return &BackupReader{store: br.store, key: key}, nil
}

// ReadFile opens the file object at ref for seekable reading.
func (br *BackupReader) ReadFile(ctx context.Context, ref *proto.Ref) (io.ReadSeeker, error) {
	obj, err := br.store.Get(ctx, ref)
	if err != nil {
		return nil, errors.Wrapf(err, "Couldn't get file %x from store", ref.Hash)
	}

	if obj.Type() != proto.ObjectType_FILE {
		return nil, errors.New("Object doesn't describe a file")
	}

	return newFileReader(ctx, br.store, obj.GetFile(), br.key), nil
}

// WalkFn is called for every node a walk visits; a non-nil error stops the
// walk.
type WalkFn func(path string, info os.FileInfo, ref *proto.Ref) error

// walk visits an opened tree; parent is the token of the directory the
// tree describes, which opens the names of its subdirectories. short, when
// set, is handed the path of each directory that lost entries no node
// below it is marked lost for, as a lost tree split takes them.
func (br *BackupReader) walk(ctx context.Context, trees Getter, path string, parent []byte, tree *proto.Tree, walkFn WalkFn, short func(dir string)) error {
	explained := false

	for _, node := range tree.GetNodes() {
		info := node.Stat
		absPath := filepath.Join(path, string(info.Name))

		err := walkFn(absPath, proto.GetOSFileInfo(info), node.Ref)
		if err != nil {
			return errors.Wrap(err, "WalkFn returned error")
		}

		explained = explained || info.Lost || info.MetadataLost

		if info.IsDir() && !info.Lost {
			token := NameToken(br.key, parent, info.Name)

			subTree, err := OpenTree(ctx, trees, node.Ref, br.key, token)
			if err != nil {
				return errors.Wrapf(err, "Failed retrieving sub-tree %x for %s", node.Ref.Hash, absPath)
			}

			explained = explained || subTree.Incomplete

			err = br.walk(ctx, trees, absPath, token, subTree, walkFn, short)
			if err != nil {
				return err
			}
		}
	}

	if short != nil && tree.Incomplete && !explained {
		short(path)
	}

	return nil
}

// GetTree descends from ref along the plaintext path parts and returns the
// tree ref at the end, with the token of the directory it names.
func (br *BackupReader) GetTree(ctx context.Context, ref *proto.Ref, parts []string) (*proto.Ref, []byte, error) {
	return br.getTree(ctx, ref, nil, parts)
}

func (br *BackupReader) getTree(ctx context.Context, ref *proto.Ref, parent []byte, parts []string) (*proto.Ref, []byte, error) {
	if len(parts) == 0 {
		return ref, parent, nil
	}

	tree, err := OpenTree(ctx, br.store, ref, br.key, parent)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "Couldn't get tree %x from store", ref.Hash)
	}

	name := parts[0]
	for _, node := range tree.Nodes {
		if node.Stat.IsDir() && string(node.Stat.Name) == name {
			if node.Stat.Lost {
				return nil, nil, errors.Wrap(ErrLost, name)
			}

			return br.getTree(ctx, node.Ref, NameToken(br.key, parent, node.Stat.Name), parts[1:])
		}
	}

	return nil, nil, errors.New("Folder not found")
}

// WalkTree walks the tree at ref, whose directory has the given token (nil
// for the root of a commit). A store that can stream a subtree hands over
// every tree below ref in one call.
func (br *BackupReader) WalkTree(ctx context.Context, ref *proto.Ref, parent []byte, walkFn WalkFn) error {
	_, err := br.walkTree(ctx, ref, parent, walkFn, nil)

	return err
}

// walkTree is WalkTree, handing short what walk does, and reporting whether
// any tree it read is marked incomplete.
func (br *BackupReader) walkTree(ctx context.Context, ref *proto.Ref, parent []byte, walkFn WalkFn, short func(dir string)) (bool, error) {
	trees := &subtree{store: br.store}
	trees.fetcher, _ = br.store.(TreeFetcher)

	tree, err := OpenTree(ctx, trees, ref, br.key, parent)
	if err != nil {
		return false, errors.Wrapf(err, "Couldn't get tree %x from store", ref.Hash)
	}

	return tree.Incomplete, br.walk(ctx, trees, "", parent, tree, walkFn, short)
}

// subtree serves the trees below the first ref it is asked for from one
// GetTree of all of them, and anything it did not get from the store.
type subtree struct {
	store   Getter
	fetcher TreeFetcher
	objects map[string]*proto.Object
}

func (s *subtree) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	if s.objects == nil && s.fetcher != nil {
		s.objects = map[string]*proto.Object{}

		// a store that fails the prefetch still serves each tree on its own
		objects, err := s.fetcher.GetTree(ctx, ref, math.MaxUint32)
		if err == nil {
			for _, obj := range objects {
				s.objects[string(obj.Ref().Hash)] = obj
			}
		}
	}

	if obj, ok := s.objects[string(ref.GetHash())]; ok {
		return obj, nil
	}

	return s.store.Get(ctx, ref)
}
