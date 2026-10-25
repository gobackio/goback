package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"
)

// rootedFS is the local disk as a Restorer writes it: a path under a
// directory a restore confines itself to is reached through an os.Root
// opened on that directory, which never resolves a symlink to outside it,
// whatever is swapped in while the restore runs. Other paths are LocalFS's.
type rootedFS struct {
	mtx   sync.Mutex
	roots []*confinedDir
}

type confinedDir struct {
	dir  string
	root *os.Root
	held int
}

var _ RandomWriteFS = (*rootedFS)(nil)

// confine reaches every path under dir, which must exist, through a root
// on it until release is called.
func (l *rootedFS) confine(dir string) (release func(), err error) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	for _, c := range l.roots {
		if c.dir == dir {
			c.held++
			return func() { l.release(c) }, nil
		}
	}

	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}

	c := &confinedDir{dir: dir, root: root, held: 1}
	l.roots = append(l.roots, c)

	return func() { l.release(c) }, nil
}

func (l *rootedFS) release(c *confinedDir) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	if c.held--; c.held > 0 {
		return
	}

	l.roots = slices.DeleteFunc(l.roots, func(d *confinedDir) bool { return d == c })
	_ = c.root.Close()
}

// resolve returns the root path is under, the deepest one, and path
// relative to it; root is nil for a path under none.
func (l *rootedFS) resolve(path string) (root *os.Root, rel string) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	var deepest *confinedDir

	for _, c := range l.roots {
		r, err := filepath.Rel(c.dir, path)
		if err != nil || filepath.IsAbs(r) || r == ".." || strings.HasPrefix(r, ".."+string(filepath.Separator)) {
			continue
		}

		if deepest == nil || len(c.dir) > len(deepest.dir) {
			deepest, rel = c, r
		}
	}

	if deepest == nil {
		return nil, ""
	}

	return deepest.root, rootRel(rel)
}

// named puts path back into the error a root returned for rel.
func named(err error, path string) error {
	var pathErr *fs.PathError
	if errors.As(err, &pathErr) {
		pathErr.Path = path
	}

	var linkErr *os.LinkError
	if errors.As(err, &linkErr) {
		linkErr.New = path
	}

	return err
}

// Join implements WriteFS.
func (*rootedFS) Join(dir, name string) string {
	return filepath.Join(dir, name)
}

// Split implements WriteFS.
func (*rootedFS) Split(path string) (string, string) {
	return LocalFS{}.Split(path)
}

// Lstat implements WriteFS.
func (l *rootedFS) Lstat(ctx context.Context, path string) (os.FileInfo, error) {
	root, rel := l.resolve(path)
	if root == nil {
		return LocalFS{}.Lstat(ctx, path)
	}

	info, err := root.Lstat(rel)

	return info, named(err, path)
}

// ReadDir implements WriteFS. It refuses a directory that is a link by
// the time it is opened.
func (l *rootedFS) ReadDir(ctx context.Context, dir string) ([]fs.DirEntry, error) {
	root, rel := l.resolve(dir)
	if root == nil {
		return LocalFS{}.ReadDir(ctx, dir)
	}

	f, err := root.Open(rel)
	if err != nil {
		return nil, named(err, dir)
	}
	defer f.Close()

	opened, err := f.Stat()
	if err != nil {
		return nil, err
	}

	if listed, err := root.Lstat(rel); err != nil || !listed.IsDir() || !os.SameFile(opened, listed) {
		return nil, fmt.Errorf("%s is no longer the directory listed", dir)
	}

	entries, err := f.ReadDir(-1)
	slices.SortFunc(entries, func(a, b fs.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })

	return entries, named(err, dir)
}

// MkdirAll implements WriteFS.
func (l *rootedFS) MkdirAll(ctx context.Context, dir string) error {
	root, rel := l.resolve(dir)
	if root == nil {
		return LocalFS{}.MkdirAll(ctx, dir)
	}

	return named(root.MkdirAll(rel, 0o775), dir)
}

// WriteFile implements WriteFS.
func (l *rootedFS) WriteFile(ctx context.Context, path string, r io.Reader) (int64, error) {
	root, rel := l.resolve(path)
	if root == nil {
		return LocalFS{}.WriteFile(ctx, path, r)
	}

	f, err := root.OpenFile(rel, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return 0, named(err, path)
	}

	n, err := io.Copy(f, r)
	if err == nil {
		err = f.Sync()
	}

	return n, errors.Join(err, f.Close())
}

// Rename implements WriteFS as LocalFS does; both paths are under the same
// root or under none.
func (l *rootedFS) Rename(ctx context.Context, from, to string) error {
	root, relFrom := l.resolve(from)
	toRoot, relTo := l.resolve(to)

	switch {
	case root == nil && toRoot == nil:
		return LocalFS{}.Rename(ctx, from, to)
	case root != toRoot:
		return fmt.Errorf("renaming %s to %s leaves the directory restored into", from, to)
	}

	if err := l.rename(root, relFrom, relTo, from, to); err != nil {
		return err
	}

	if runtime.GOOS == "windows" {
		return nil
	}

	d, err := root.Open(filepath.Dir(relTo))
	if err != nil {
		return named(err, filepath.Dir(to))
	}
	defer d.Close()

	return d.Sync()
}

func (l *rootedFS) rename(root *os.Root, relFrom, relTo, from, to string) error {
	err := root.Rename(relFrom, relTo)
	if err == nil {
		return nil
	}

	failed := &os.LinkError{Op: "rename", Old: from, New: to, Err: errors.Unwrap(err)}

	info, statErr := root.Lstat(relTo)
	if statErr != nil || info.Mode()&0o200 != 0 {
		return failed
	}

	if root.Chmod(relTo, info.Mode().Perm()|0o200) != nil {
		return failed
	}

	if err := root.Rename(relFrom, relTo); err != nil {
		return &os.LinkError{Op: "rename", Old: from, New: to, Err: errors.Unwrap(err)}
	}

	return nil
}

// Remove implements WriteFS.
func (l *rootedFS) Remove(ctx context.Context, path string) error {
	root, rel := l.resolve(path)
	if root == nil {
		return LocalFS{}.Remove(ctx, path)
	}

	return named(root.Remove(rel), path)
}

// RemoveAll implements WriteFS.
func (l *rootedFS) RemoveAll(ctx context.Context, path string) error {
	root, rel := l.resolve(path)
	if root == nil {
		return LocalFS{}.RemoveAll(ctx, path)
	}

	return named(root.RemoveAll(rel), path)
}

// Symlink implements WriteFS.
func (l *rootedFS) Symlink(ctx context.Context, target, path string) error {
	root, rel := l.resolve(path)
	if root == nil {
		return LocalFS{}.Symlink(ctx, target, path)
	}

	return unsupportedLink(symlinkIn(root, target, rel, path))
}

// Readlink implements WriteFS.
func (l *rootedFS) Readlink(ctx context.Context, path string) (string, error) {
	root, rel := l.resolve(path)
	if root == nil {
		return LocalFS{}.Readlink(ctx, path)
	}

	target, err := root.Readlink(rel)

	return target, named(err, path)
}

// Chmod implements WriteFS.
func (l *rootedFS) Chmod(ctx context.Context, path string, mode os.FileMode) error {
	root, rel := l.resolve(path)
	if root == nil {
		return LocalFS{}.Chmod(ctx, path, mode)
	}

	return named(root.Chmod(rel, mode), path)
}

// Chtimes implements WriteFS.
func (l *rootedFS) Chtimes(ctx context.Context, path string, mtime time.Time) error {
	root, rel := l.resolve(path)
	if root == nil {
		return LocalFS{}.Chtimes(ctx, path, mtime)
	}

	return named(root.Chtimes(rel, time.Now(), mtime), path)
}

// Open implements RandomWriteFS.
func (l *rootedFS) Open(ctx context.Context, path string) (File, error) {
	root, rel := l.resolve(path)
	if root == nil {
		return LocalFS{}.Open(ctx, path)
	}

	f, err := root.Open(rel)
	if err != nil {
		return nil, named(err, path)
	}

	return f, nil
}

// OpenFile implements RandomWriteFS.
func (l *rootedFS) OpenFile(ctx context.Context, path string) (RandomFile, error) {
	root, rel := l.resolve(path)
	if root == nil {
		return LocalFS{}.OpenFile(ctx, path)
	}

	f, err := root.OpenFile(rel, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, named(err, path)
	}

	return f, nil
}
