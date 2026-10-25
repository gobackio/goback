package backup

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// WriteFS is the file tree a Restorer writes. Paths are the FS's own
// strings, built with Join. Symlink, Chmod and Chtimes fail with an error
// matching errors.ErrUnsupported where the tree cannot hold links, modes or
// times; a restore then goes on without them. A missing path's error
// matches fs.ErrNotExist.
type WriteFS interface {
	Join(dir, name string) string
	// Split is path's directory and its last element, which Join puts back
	// together.
	Split(path string) (dir, name string)
	Lstat(ctx context.Context, path string) (os.FileInfo, error)
	// ReadDir lists dir without following symlinks; a restore lists only
	// to delete what its tree does not hold.
	ReadDir(ctx context.Context, dir string) ([]fs.DirEntry, error)
	// MkdirAll makes dir and the directories above it that are missing.
	MkdirAll(ctx context.Context, dir string) error
	// WriteFile writes what r holds to path, front to back, replacing a
	// file there.
	WriteFile(ctx context.Context, path string, r io.Reader) (int64, error)
	// Rename moves from to to, replacing a file there.
	Rename(ctx context.Context, from, to string) error
	Remove(ctx context.Context, path string) error
	RemoveAll(ctx context.Context, path string) error
	Symlink(ctx context.Context, target, path string) error
	Readlink(ctx context.Context, path string) (string, error)
	// Chmod sets the permission, setuid, setgid and sticky bits of mode.
	Chmod(ctx context.Context, path string, mode os.FileMode) error
	Chtimes(ctx context.Context, path string, mtime time.Time) error
}

// MetadataWriteFS is a WriteFS that puts back what a MetadataFS records.
// LocalFS is one on Windows.
type MetadataWriteFS interface {
	// WriteStream writes one alternate data stream of path, replacing one
	// of that name.
	WriteStream(ctx context.Context, path, name string, r io.Reader) error
	// SetSecurity sets path's owner, group, DACL and the SACL the
	// descriptor holds. It fails with ErrNotPrivileged when the process may
	// not set any owner.
	SetSecurity(ctx context.Context, path string, sd []byte) error
	// SetAttributes sets path's creation time, unless it is zero, and the
	// FILE_ATTRIBUTE_* flags a file can be given.
	SetAttributes(ctx context.Context, path string, birth time.Time, attributes uint32) error
}

// OwnerWriteFS is a WriteFS whose entries take a Unix owner, as a
// Restorer's Owner gives them. LocalFS is one on Unix.
type OwnerWriteFS interface {
	// Lchown sets path's uid and gid without following a symlink.
	Lchown(ctx context.Context, path string, uid, gid int) error
}

// ErrNotPrivileged is returned for a security descriptor the process may
// not set, which needs an elevated administrator.
var ErrNotPrivileged = errors.New("setting a security descriptor needs an elevated administrator")

// RandomWriteFS is a WriteFS whose files are read and written at any
// offset, so a restore takes parts from the file already there, resumes
// a partial file and fetches the parts of one file in parallel.
type RandomWriteFS interface {
	WriteFS
	// Open opens path to read; a File that is no io.ReaderAt is read only
	// front to back.
	Open(ctx context.Context, path string) (File, error)
	// OpenFile opens path to read and write, creating it.
	OpenFile(ctx context.Context, path string) (RandomFile, error)
}

// RandomFile is a file of a RandomWriteFS. WriteAt may be called from
// several goroutines at once.
type RandomFile interface {
	io.ReaderAt
	io.WriterAt
	Truncate(size int64) error
	// Sync makes what was written durable.
	Sync() error
	Stat() (os.FileInfo, error)
	Close() error
}

var _ RandomWriteFS = LocalFS{}

// Split implements WriteFS.
func (LocalFS) Split(path string) (string, string) {
	return filepath.Dir(path), filepath.Base(path)
}

// MkdirAll implements WriteFS.
func (LocalFS) MkdirAll(_ context.Context, dir string) error {
	return os.MkdirAll(dir, 0o775)
}

// WriteFile implements WriteFS.
func (LocalFS) WriteFile(_ context.Context, path string, r io.Reader) (int64, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return 0, err
	}

	n, err := io.Copy(f, r)
	if err == nil {
		err = f.Sync()
	}

	return n, errors.Join(err, f.Close())
}

// Rename implements WriteFS. It makes the rename durable, and replaces a
// read-only file, which Windows refuses to, by making it writable first.
func (LocalFS) Rename(_ context.Context, from, to string) error {
	err := os.Rename(from, to)
	if err != nil {
		info, statErr := os.Lstat(to)
		if statErr != nil || info.Mode()&0o200 != 0 {
			return err
		}

		if chmodErr := os.Chmod(to, info.Mode().Perm()|0o200); chmodErr != nil {
			return err
		}

		if err = os.Rename(from, to); err != nil {
			return err
		}
	}

	return syncDir(filepath.Dir(to))
}

// Remove implements WriteFS.
func (LocalFS) Remove(_ context.Context, path string) error {
	return os.Remove(path)
}

// RemoveAll implements WriteFS.
func (LocalFS) RemoveAll(_ context.Context, path string) error {
	return os.RemoveAll(path)
}

// Symlink implements WriteFS.
func (LocalFS) Symlink(_ context.Context, target, path string) error {
	return os.Symlink(target, path)
}

// Chmod implements WriteFS.
func (LocalFS) Chmod(_ context.Context, path string, mode os.FileMode) error {
	return os.Chmod(path, mode)
}

// Chtimes implements WriteFS.
func (LocalFS) Chtimes(_ context.Context, path string, mtime time.Time) error {
	return os.Chtimes(path, time.Now(), mtime)
}

// OpenFile implements RandomWriteFS.
func (LocalFS) OpenFile(_ context.Context, path string) (RandomFile, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
}

func isLocalWrite(fsys WriteFS) bool {
	switch fsys.(type) {
	case LocalFS, *LocalFS, *rootedFS:
		return true
	}

	return false
}

// settable is the part of mode Chmod sets.
func settable(mode os.FileMode) os.FileMode {
	return mode & (os.ModePerm | os.ModeSetuid | os.ModeSetgid | os.ModeSticky)
}

// setMeta gives path its mode and mtime, as far as the tree holds them.
func setMeta(ctx context.Context, fsys WriteFS, path string, mode os.FileMode, mtime time.Time) error {
	if err := fsys.Chtimes(ctx, path, mtime); err != nil && !errors.Is(err, errors.ErrUnsupported) {
		return err
	}

	if err := fsys.Chmod(ctx, path, settable(mode)); err != nil && !errors.Is(err, errors.ErrUnsupported) {
		return err
	}

	return nil
}

// removeIfThere removes path, which may already be gone.
func removeIfThere(ctx context.Context, fsys WriteFS, path string) error {
	if err := fsys.Remove(ctx, path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	return nil
}

// opener makes the directories a restore writes into, which are not part
// of the tree it restores, writable for their owner while it runs.
type opener struct {
	fsys   WriteFS
	opened []openedDir
}

type openedDir struct {
	path string
	mode os.FileMode
}

// open adds the owner's write bit to dir, or to the nearest directory
// above it while it does not exist, where it lacks it. Where the tree
// does not let it, the write that follows fails as it would have.
func (o *opener) open(ctx context.Context, dir string) {
	info, err := o.fsys.Lstat(ctx, dir)
	if errors.Is(err, fs.ErrNotExist) {
		if parent, _ := o.fsys.Split(dir); parent != dir && parent != "" {
			o.open(ctx, parent)
		}

		return
	}

	if err != nil || !info.IsDir() || info.Mode().Perm()&0o200 != 0 {
		return
	}

	if o.fsys.Chmod(ctx, dir, settable(info.Mode())|0o200) == nil {
		o.opened = append(o.opened, openedDir{path: dir, mode: info.Mode()})
	}
}

// close puts back the modes open found, the last opened first, whether
// the restore succeeded or not.
func (o *opener) close(ctx context.Context) {
	ctx = context.WithoutCancel(ctx)

	for i := len(o.opened) - 1; i >= 0; i-- {
		_ = o.fsys.Chmod(ctx, o.opened[i].path, settable(o.opened[i].mode))
	}

	o.opened = nil
}
