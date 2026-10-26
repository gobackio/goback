package pack

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newLocal(base string) *localArchiveStorage {
	return &localArchiveStorage{base}
}

var _ ArchiveStorage = (*localArchiveStorage)(nil)

// localArchiveStorage keeps archives in a directory; a name with slashes
// lands in a subdirectory.
type localArchiveStorage struct {
	base string
}

func (las *localArchiveStorage) DeleteAll() error {
	err := os.RemoveAll(las.base)
	if err != nil {
		return err
	}

	return os.MkdirAll(las.base, 0755)
}

func (las *localArchiveStorage) path(name string) string {
	return filepath.Join(las.base, filepath.FromSlash(name))
}

func (las *localArchiveStorage) Open(name string) (File, error) {
	return os.OpenFile(las.path(name), os.O_RDONLY, 0644)
}

func (las *localArchiveStorage) Create(name string) (File, error) {
	err := os.MkdirAll(filepath.Dir(las.path(name)), 0755)
	if err != nil {
		return nil, err
	}

	file, err := os.Create(las.path(name))
	if err != nil {
		return nil, err
	}

	return &stampedFile{File: file}, nil
}

// stampedFile gives each file it closes a modification time newer than the
// last one's, as a bucket's creation times are; the file system's own
// clock may give two files written in quick succession the same time.
type stampedFile struct {
	*os.File
}

var stamps struct {
	sync.Mutex
	last time.Time
}

func (f *stampedFile) Close() error {
	// a bucket stores what a writer closes, so a file another process
	// deleted while it was open, as POSIX allows, comes back
	if _, err := os.Stat(f.Name()); errors.Is(err, fs.ErrNotExist) {
		if err := f.restore(); err != nil {
			_ = f.File.Close()
			return err
		}
	}

	if err := f.File.Close(); err != nil {
		return err
	}

	stamps.Lock()
	stamp := time.Now().Truncate(time.Microsecond)
	if !stamp.After(stamps.last) {
		stamp = stamps.last.Add(time.Microsecond)
	}
	stamps.last = stamp
	stamps.Unlock()

	return os.Chtimes(f.Name(), stamp, stamp)
}

// restore writes what the open file holds back to its name.
func (f *stampedFile) restore() error {
	info, err := f.File.Stat()
	if err != nil {
		return err
	}

	data, err := io.ReadAll(io.NewSectionReader(f.File, 0, info.Size()))
	if err != nil {
		return err
	}

	return os.WriteFile(f.Name(), data, 0644)
}

func (las *localArchiveStorage) CreateNew(name string, data []byte) error {
	err := os.MkdirAll(filepath.Dir(las.path(name)), 0755)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(las.path(name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if errors.Is(err, fs.ErrExist) {
		return ErrFileExists
	}

	if err != nil {
		return err
	}

	_, err = file.Write(data)
	if err != nil {
		_ = file.Close()
		return err
	}

	return file.Close()
}

// Delete removes the file and the directories it leaves empty.
func (las *localArchiveStorage) Delete(name string) error {
	err := os.Remove(las.path(name))
	if err != nil {
		return err
	}

	las.pruneDirs(name)

	return nil
}

func (las *localArchiveStorage) Move(from, to string) error {
	if err := os.MkdirAll(filepath.Dir(las.path(to)), 0755); err != nil {
		return err
	}

	if err := os.Rename(las.path(from), las.path(to)); errors.Is(err, fs.ErrNotExist) {
		return ErrFileNotFound
	} else if err != nil {
		return err
	}

	las.pruneDirs(from)

	return nil
}

// pruneDirs removes the directories the named file leaves empty.
func (las *localArchiveStorage) pruneDirs(name string) {
	for dir := filepath.Dir(las.path(name)); dir != las.base && strings.HasPrefix(dir, las.base); dir = filepath.Dir(dir) {
		if os.Remove(dir) != nil {
			break
		}
	}
}

func (las *localArchiveStorage) List(extension string) ([]string, error) {
	files, err := las.ListInfo(extension)

	names := make([]string, len(files))
	for i, file := range files {
		names[i] = file.Name
	}

	return names, err
}

func (las *localArchiveStorage) ListInfo(extension string) ([]ListedFile, error) {
	var files []ListedFile

	err := filepath.WalkDir(las.base, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() || !strings.HasSuffix(p, extension) {
			return nil
		}

		rel, err := filepath.Rel(las.base, p)
		if err != nil {
			return err
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		files = append(files, ListedFile{Name: filepath.ToSlash(rel), Size: info.Size(), Modified: info.ModTime()})

		return nil
	})

	return files, err
}

func TestLocalStorageNestedNames(t *testing.T) {
	base := t.TempDir()
	storage := newLocal(base)

	file, err := storage.Create("s1/t1/session/archive" + ArchiveSuffix)
	require.NoError(t, err)
	_, err = file.Write([]byte("data"))
	require.NoError(t, err)
	require.NoError(t, file.Close())

	flat, err := storage.Create("root" + ArchiveSuffix)
	require.NoError(t, err)
	require.NoError(t, flat.Close())

	names, err := storage.List(ArchiveSuffix)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"s1/t1/session/archive" + ArchiveSuffix, "root" + ArchiveSuffix}, names)

	file, err = storage.Open("s1/t1/session/archive" + ArchiveSuffix)
	require.NoError(t, err)
	data, err := io.ReadAll(file)
	require.NoError(t, err)
	require.Equal(t, "data", string(data))
	require.NoError(t, file.Close())

	require.NoError(t, storage.Delete("s1/t1/session/archive"+ArchiveSuffix))

	_, err = os.Stat(filepath.Join(base, "s1"))
	require.True(t, os.IsNotExist(err), "empty directories are removed with the last file")

	_, err = os.Stat(base)
	require.NoError(t, err, "the base directory stays")
}
