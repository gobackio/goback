package backup

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// FS is the file tree a Walker reads. Paths are the FS's own strings: Join
// builds them from the Walker's Root, and the walker passes them back
// unchanged. An error for lack of access wraps fs.ErrPermission, so the
// walk keeps the previous version instead of failing, and one for an entry
// that is gone wraps fs.ErrNotExist, so the walk leaves it out.
type FS interface {
	// ReadDir lists dir sorted by name, without following symlinks. Info on
	// an entry may stat it; the walker only asks for entries it includes.
	ReadDir(ctx context.Context, dir string) ([]fs.DirEntry, error)
	// Stat stats path, following a symlink; the walker uses it on Root.
	Stat(ctx context.Context, path string) (os.FileInfo, error)
	Lstat(ctx context.Context, path string) (os.FileInfo, error)
	Open(ctx context.Context, path string) (File, error)
	Readlink(ctx context.Context, path string) (string, error)
	Join(dir, name string) string
	// Owners names a file's user and group; empty when unknown.
	Owners(info os.FileInfo) (user, group string)
	// Traits is read once as a walk starts.
	Traits() Traits
}

// OwnerIDFS is an FS that knows the numeric owner of its entries, which a
// walk records beside the names. LocalFS is one.
type OwnerIDFS interface {
	// OwnerIDs returns a file's uid and gid; 0, 0 where there are none.
	OwnerIDs(info os.FileInfo) (uid, gid uint32)
}

// MetadataFS is an FS whose entries carry a security descriptor and
// alternate data streams, which a walk records with the entry. LocalFS is
// one on Windows.
type MetadataFS interface {
	// Security returns the entry's self-relative security descriptor: owner,
	// group and DACL, and the SACL when the process may read it.
	Security(ctx context.Context, path string) ([]byte, error)
	// Streams lists the entry's alternate data streams by name.
	Streams(ctx context.Context, path string) ([]StreamInfo, error)
	// OpenStream opens one of the entry's streams for reading.
	OpenStream(ctx context.Context, path, name string) (File, error)
}

// StreamInfo names an alternate data stream and its size.
type StreamInfo struct {
	Name string
	Size int64
}

// File is an open file of an FS. One that is also an io.ReaderAt lets the
// walker re-read a single part rather than the whole file.
type File interface {
	io.ReadCloser
	Stat() (os.FileInfo, error)
}

// Traits say what an FS's stat data is worth to the change test.
type Traits struct {
	// Resolution is the step mtimes are truncated to; 0 means exact. An FS
	// must report every mtime truncated to it, the same way on every run,
	// or files read with a finer time once look changed the next time.
	Resolution time.Duration
	// Skew bounds how far the FS's clock may be from ours.
	Skew time.Duration
	// Identity reports whether stat data carries ctime and inode, which
	// the stat cache compares; without it the walk consults no stat cache.
	Identity bool
	// Reread reports whether reading a file again is cheap enough for a
	// sampled re-hash; without it ForceHashPercent is ignored.
	Reread bool
}

// LocalFS is the local disk, the FS a Walker reads when its FS is nil.
// Paths are the operating system's.
type LocalFS struct{}

var (
	_ FS        = LocalFS{}
	_ OwnerIDFS = LocalFS{}
)

// ReadDir implements FS.
func (LocalFS) ReadDir(_ context.Context, dir string) ([]fs.DirEntry, error) {
	return localReadDir(dir)
}

// Stat implements FS.
func (LocalFS) Stat(_ context.Context, path string) (os.FileInfo, error) {
	return os.Stat(path)
}

// Lstat implements FS.
func (LocalFS) Lstat(_ context.Context, path string) (os.FileInfo, error) {
	return os.Lstat(path)
}

// Open implements FS.
func (LocalFS) Open(_ context.Context, path string) (File, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return file, nil
}

// Readlink implements FS.
func (LocalFS) Readlink(_ context.Context, path string) (string, error) {
	return os.Readlink(path)
}

// Join implements FS.
func (LocalFS) Join(dir, name string) string {
	return filepath.Join(dir, name)
}

// Owners implements FS.
func (LocalFS) Owners(info os.FileInfo) (string, string) {
	return OwnerNames(info)
}

// OwnerIDs implements OwnerIDFS.
func (LocalFS) OwnerIDs(info os.FileInfo) (uint32, uint32) {
	return ownerIDs(info)
}

// Traits implements FS.
func (LocalFS) Traits() Traits {
	return localTraits
}

// isLocal reports whether fsys is the local disk.
func isLocal(fsys FS) bool {
	switch fsys.(type) {
	case nil, LocalFS, *LocalFS:
		return true
	}

	return false
}
