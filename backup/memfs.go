package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"sync"
	"time"

	"github.com/gobackio/goback/proto"
)

// ReadFile reads the file ref describes into memory, fetching it as
// RestoreFile does, so from a store that serves parts only to a restore
// too. It suits files of a few KiB.
func (r *Restorer) ReadFile(ctx context.Context, stat *proto.FileInfo, ref *proto.Ref) ([]byte, error) {
	mem := &memWriteFS{files: map[string]*memWriteFile{}}
	restorer := &Restorer{Store: r.Store, Key: r.Key, FS: mem, Seeds: r.Seeds, Cache: r.Cache, Workers: r.Workers}

	if _, err := restorer.RestoreFile(ctx, "file", stat, ref); err != nil {
		return nil, err
	}

	return mem.files["file"].data, nil
}

// memWriteFS is a tree of files in memory, without directories, links, modes or
// times.
type memWriteFS struct {
	mtx   sync.Mutex
	files map[string]*memWriteFile
}

var _ RandomWriteFS = (*memWriteFS)(nil)

func (m *memWriteFS) file(name string) (*memWriteFile, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	f, ok := m.files[name]
	if !ok {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}

	return f, nil
}

func (m *memWriteFS) Join(dir, name string) string {
	return path.Join(dir, name)
}

func (m *memWriteFS) Split(name string) (string, string) {
	return path.Split(name)
}

func (m *memWriteFS) Lstat(_ context.Context, name string) (os.FileInfo, error) {
	f, err := m.file(name)
	if err != nil {
		return nil, err
	}

	return f.Stat()
}

func (m *memWriteFS) ReadDir(context.Context, string) ([]fs.DirEntry, error) {
	return nil, fmt.Errorf("listing memory: %w", errors.ErrUnsupported)
}

func (m *memWriteFS) MkdirAll(context.Context, string) error {
	return nil
}

func (m *memWriteFS) WriteFile(_ context.Context, name string, r io.Reader) (int64, error) {
	data, err := io.ReadAll(r)

	m.mtx.Lock()
	m.files[name] = &memWriteFile{name: path.Base(name), data: data}
	m.mtx.Unlock()

	return int64(len(data)), err
}

func (m *memWriteFS) Rename(_ context.Context, from, to string) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	f, ok := m.files[from]
	if !ok {
		return &fs.PathError{Op: "rename", Path: from, Err: fs.ErrNotExist}
	}

	delete(m.files, from)
	f.name = path.Base(to)
	m.files[to] = f

	return nil
}

func (m *memWriteFS) Remove(_ context.Context, name string) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if _, ok := m.files[name]; !ok {
		return &fs.PathError{Op: "remove", Path: name, Err: fs.ErrNotExist}
	}

	delete(m.files, name)

	return nil
}

func (m *memWriteFS) RemoveAll(_ context.Context, name string) error {
	m.mtx.Lock()
	delete(m.files, name)
	m.mtx.Unlock()

	return nil
}

func (m *memWriteFS) Symlink(context.Context, string, string) error {
	return fmt.Errorf("links in memory: %w", errors.ErrUnsupported)
}

func (m *memWriteFS) Readlink(context.Context, string) (string, error) {
	return "", fmt.Errorf("links in memory: %w", errors.ErrUnsupported)
}

func (m *memWriteFS) Chmod(context.Context, string, os.FileMode) error {
	return fmt.Errorf("modes in memory: %w", errors.ErrUnsupported)
}

func (m *memWriteFS) Chtimes(context.Context, string, time.Time) error {
	return fmt.Errorf("times in memory: %w", errors.ErrUnsupported)
}

func (m *memWriteFS) Open(_ context.Context, name string) (File, error) {
	f, err := m.file(name)
	if err != nil {
		return nil, err
	}

	f.mtx.Lock()
	defer f.mtx.Unlock()

	return memWriteReader{Reader: bytes.NewReader(bytes.Clone(f.data)), info: f.info()}, nil
}

func (m *memWriteFS) OpenFile(_ context.Context, name string) (RandomFile, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	f, ok := m.files[name]
	if !ok {
		f = &memWriteFile{name: path.Base(name)}
		m.files[name] = f
	}

	return f, nil
}

// memWriteFile is a file of a memWriteFS, open for as long as the memWriteFS holds it.
type memWriteFile struct {
	mtx  sync.Mutex
	name string
	data []byte
}

func (f *memWriteFile) ReadAt(b []byte, off int64) (int, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}

	n := copy(b, f.data[off:])
	if n < len(b) {
		return n, io.EOF
	}

	return n, nil
}

func (f *memWriteFile) WriteAt(b []byte, off int64) (int, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	if end := off + int64(len(b)); end > int64(len(f.data)) {
		f.data = append(f.data, make([]byte, end-int64(len(f.data)))...)
	}

	return copy(f.data[off:], b), nil
}

func (f *memWriteFile) Truncate(size int64) error {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	if size <= int64(len(f.data)) {
		f.data = f.data[:size]
	} else {
		f.data = append(f.data, make([]byte, size-int64(len(f.data)))...)
	}

	return nil
}

func (f *memWriteFile) Sync() error {
	return nil
}

func (f *memWriteFile) Stat() (os.FileInfo, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	return f.info(), nil
}

func (f *memWriteFile) info() memWriteInfo {
	return memWriteInfo{name: f.name, size: int64(len(f.data))}
}

func (f *memWriteFile) Close() error {
	return nil
}

type memWriteReader struct {
	*bytes.Reader
	info memWriteInfo
}

func (r memWriteReader) Stat() (os.FileInfo, error) {
	return r.info, nil
}

func (r memWriteReader) Close() error {
	return nil
}

type memWriteInfo struct {
	name string
	size int64
}

func (i memWriteInfo) Name() string       { return i.name }
func (i memWriteInfo) Size() int64        { return i.size }
func (i memWriteInfo) Mode() os.FileMode  { return 0o600 }
func (i memWriteInfo) ModTime() time.Time { return time.Time{} }
func (i memWriteInfo) IsDir() bool        { return false }
func (i memWriteInfo) Sys() any           { return nil }
