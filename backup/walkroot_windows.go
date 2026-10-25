//go:build windows

package backup

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

// localRoot is the directory a walk of the local disk lists and opens what
// it reads in.
type localRoot = *pathRoot

type pathRoot struct {
	dir string
}

// Close implements io.Closer.
func (*pathRoot) Close() error {
	return nil
}

func (r *pathRoot) path(rel string) string {
	if rel == "" || rel == "." {
		return r.dir
	}

	return filepath.Join(r.dir, filepath.FromSlash(rel))
}

func openLocalRoot(dir string) (localRoot, error) {
	return &pathRoot{dir: dir}, nil
}

// listIn lists the directory at rel in root sorted by name, with entries
// whose Info stats the path itself, and returns that directory's stat. It
// fails with errVanished when rel is now a link or no longer a directory.
func listIn(root localRoot, rel string) ([]os.DirEntry, os.FileInfo, error) {
	dir := root.path(rel)

	file, self, err := openDirectory(dir, rel != "")
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	entries, err := file.ReadDir(-1)
	if err != nil {
		return nil, nil, err
	}

	slices.SortFunc(entries, func(a, b os.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })

	listed := make([]lstatEntry, len(entries))
	for i, entry := range entries {
		listed[i] = lstatEntry{DirEntry: entry, dir: dir}
		entries[i] = &listed[i]
	}

	return entries, self, nil
}

// openDirectory opens the directory at dir to list it and returns it with
// its stat. With exact, a link at dir is not followed but fails with
// errVanished, as a dir that is no longer a directory does either way.
func openDirectory(dir string, exact bool) (*os.File, *identified, error) {
	name, err := windows.UTF16PtrFromString(extended(dir))
	if err != nil {
		return nil, nil, err
	}

	share := uint32(windows.FILE_SHARE_READ | windows.FILE_SHARE_WRITE | windows.FILE_SHARE_DELETE)
	access := uint32(windows.FILE_LIST_DIRECTORY | windows.FILE_READ_ATTRIBUTES | windows.SYNCHRONIZE)

	open := func(reparse bool) (windows.Handle, error) {
		flags := uint32(windows.FILE_FLAG_BACKUP_SEMANTICS)
		if reparse {
			flags |= windows.FILE_FLAG_OPEN_REPARSE_POINT
		}

		return windows.CreateFile(name, access, share, nil, windows.OPEN_EXISTING, flags, 0)
	}

	var (
		handle windows.Handle
		self   *identified
	)

	if exact {
		handle, self, err = openExactly(dir, filepath.Base(dir), open)
	} else if handle, err = open(false); err == nil {
		if self, err = handleStat(handle, filepath.Base(dir)); err != nil {
			_ = windows.CloseHandle(handle)
		}
	}

	if err != nil {
		return nil, nil, &os.PathError{Op: "open", Path: dir, Err: err}
	}

	if self.stat.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY == 0 {
		_ = windows.CloseHandle(handle)
		return nil, nil, fmt.Errorf("%w: %s is no longer a directory", errVanished, dir)
	}

	return os.NewFile(uintptr(handle), dir), self, nil
}

// openIn opens the file at rel in root to read, found at full on the disk,
// in the directory listed as parent, without following a link there. It
// fails with errVanished when rel's directory is no longer that one or the
// file is now a link.
func openIn(root localRoot, rel, full string, parent os.FileInfo) (File, error) {
	up := path.Dir(rel)

	dir, self, err := openDirectory(root.path(up), up != ".")
	if err != nil {
		return nil, err
	}
	defer dir.Close()

	if !sameFile(self, parent) {
		return nil, fmt.Errorf("%w: the directory of %s is no longer the one listed", errVanished, full)
	}

	base := path.Base(rel)

	handle, stat, err := openExactly(full, base, func(reparse bool) (windows.Handle, error) {
		var options uint32
		if reparse {
			options = windows.FILE_OPEN_REPARSE_POINT
		}

		return openRelative(windows.Handle(dir.Fd()), base, options)
	})
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: full, Err: err}
	}

	return &handleFile{File: os.NewFile(uintptr(handle), full), name: stat.name}, nil
}

// reparseTagNameSurrogate marks a reparse point that names another file,
// as a symlink or junction does.
const reparseTagNameSurrogate = 0x2000_0000

// openExactly opens the entry at full, named name, with open, which is told
// whether to open a reparse point there itself, and returns the handle and
// its stat. A link fails with errVanished. Any other reparse point, such as
// a cloud placeholder, is opened again through its filter, and must still
// be the same file.
func openExactly(full, name string, open func(reparse bool) (windows.Handle, error)) (windows.Handle, *identified, error) {
	handle, err := open(true)
	if err != nil {
		return 0, nil, err
	}

	stat, err := handleStat(handle, name)
	if err != nil || stat.stat.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT == 0 {
		if err != nil {
			_ = windows.CloseHandle(handle)
			return 0, nil, err
		}

		return handle, stat, nil
	}

	_ = windows.CloseHandle(handle)

	if stat.stat.ReparseTag&reparseTagNameSurrogate != 0 {
		return 0, nil, fmt.Errorf("%w: %s is now a link", errVanished, full)
	}

	if handle, err = open(false); err != nil {
		return 0, nil, err
	}

	followed, err := handleStat(handle, name)
	if err == nil && followed.stat.FileID != stat.stat.FileID {
		err = fmt.Errorf("%w: %s changed while it was opened", errVanished, full)
	}

	if err != nil {
		_ = windows.CloseHandle(handle)
		return 0, nil, err
	}

	return handle, followed, nil
}

// openRelative opens the entry named name in the directory dir to read,
// sharing it as openShared does, with options added to NtCreateFile's.
func openRelative(dir windows.Handle, name string, options uint32) (windows.Handle, error) {
	ntName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return 0, err
	}

	attrs := windows.OBJECT_ATTRIBUTES{
		Length:        uint32(unsafe.Sizeof(windows.OBJECT_ATTRIBUTES{})),
		RootDirectory: dir,
		ObjectName:    ntName,
	}

	var (
		handle windows.Handle
		iosb   windows.IO_STATUS_BLOCK
	)

	share := uint32(windows.FILE_SHARE_READ | windows.FILE_SHARE_WRITE | windows.FILE_SHARE_DELETE)

	err = windows.NtCreateFile(&handle, windows.FILE_GENERIC_READ, &attrs, &iosb, nil, 0, share, windows.FILE_OPEN,
		windows.FILE_SYNCHRONOUS_IO_NONALERT|windows.FILE_OPEN_FOR_BACKUP_INTENT|options, 0, 0)
	if status, ok := err.(windows.NTStatus); ok {
		return 0, status.Errno()
	}

	return handle, err
}

// handleStat is the stat of the open handle: its FILE_STAT_INFORMATION or,
// where Windows cannot tell that, what GetFileInformationByHandle does.
func handleStat(handle windows.Handle, name string) (*identified, error) {
	info := &identified{name: name}

	var iosb windows.IO_STATUS_BLOCK
	err := windows.NtQueryInformationFile(handle, &iosb, (*byte)(unsafe.Pointer(&info.stat)), uint32(unsafe.Sizeof(info.stat)), fileStatInformationClass)
	if err == nil {
		return info, nil
	}

	var by windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &by); err != nil {
		return nil, err
	}

	info.stat = fileStatInformation{
		FileID:         int64(by.FileIndexHigh)<<32 | int64(by.FileIndexLow),
		CreationTime:   filetime(by.CreationTime),
		LastAccessTime: filetime(by.LastAccessTime),
		LastWriteTime:  filetime(by.LastWriteTime),
		EndOfFile:      int64(by.FileSizeHigh)<<32 | int64(by.FileSizeLow),
		FileAttributes: by.FileAttributes,
		NumberOfLinks:  by.NumberOfLinks,
	}

	return info, nil
}

func filetime(t windows.Filetime) int64 {
	return int64(t.HighDateTime)<<32 | int64(t.LowDateTime)
}

// handleFile is a file openIn opened, whose Stat comes from its handle.
type handleFile struct {
	*os.File
	name string
}

// Stat implements File.
func (f *handleFile) Stat() (os.FileInfo, error) {
	stat, err := handleStat(windows.Handle(f.Fd()), f.name)
	if err != nil {
		return nil, err
	}

	return stat, nil
}

// sameFile reports whether a and b are the same file by the file IDs a
// walk's stats carry, and is true when either carries none.
func sameFile(a, b os.FileInfo) bool {
	x, ok := a.Sys().(*fileStatInformation)
	y, ok2 := b.Sys().(*fileStatInformation)

	if !ok || !ok2 {
		return true
	}

	return x.FileID == y.FileID
}
