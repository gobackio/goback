//go:build windows

package backup

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// IsPermissionError reports whether an open or list failed for lack of access.
func IsPermissionError(err error) bool {
	return errors.Is(err, os.ErrPermission) ||
		errors.Is(err, windows.ERROR_SHARING_VIOLATION) ||
		errors.Is(err, windows.ERROR_CANT_ACCESS_FILE)
}

// IsLockError reports whether a read failed because another process holds
// a byte range lock.
func IsLockError(err error) bool {
	return errors.Is(err, windows.ERROR_LOCK_VIOLATION)
}

// vanished reports whether an operation on an entry failed because the
// entry is gone, or is no longer the directory it was listed as.
func vanished(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) || errors.Is(err, windows.ERROR_DIRECTORY)
}

// entryError reports whether an open, read or listing failed for that entry
// alone: for lack of access, on a failing medium or at a path too long or
// unresolvable to reach.
func entryError(err error) bool {
	return IsPermissionError(err) || IsLockError(err) ||
		errors.Is(err, windows.ERROR_CRC) || errors.Is(err, windows.ERROR_IO_DEVICE) ||
		errors.Is(err, windows.ERROR_FILENAME_EXCED_RANGE) || errors.Is(err, windows.ERROR_CANT_RESOLVE_FILENAME)
}

// lockedElsewhere reports whether another process holds the file open
// exclusively or has locked its first byte, as a Java FileChannel lock is.
func lockedElsewhere(path string) bool {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return errors.Is(err, windows.ERROR_SHARING_VIOLATION)
	}
	defer file.Close()

	var overlapped windows.Overlapped
	handle := windows.Handle(file.Fd())

	err = windows.LockFileEx(handle, windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &overlapped)
	if err != nil {
		return true
	}

	_ = windows.UnlockFileEx(handle, 0, 1, 0, &overlapped)

	return false
}

// OwnerNames is empty on Windows: there is no uid or gid to record.
func OwnerNames(os.FileInfo) (string, string) {
	return "", ""
}

// linkID finds no hard links: a walk on Windows records none.
func linkID(os.FileInfo) (fileID, bool) {
	return fileID{}, false
}

func ownerIDs(os.FileInfo) (uint32, uint32) {
	return 0, 0
}

// FileIdentity returns the change time and file ID of a file LocalFS
// listed, the signals the stat cache adds on top of mtime and size; it is
// 0, 0 for any other FileInfo.
func FileIdentity(info os.FileInfo) (ctimeNs int64, id uint64) {
	stat, ok := info.Sys().(*fileStatInformation)
	if !ok {
		return 0, 0
	}

	return filetimeNs(stat.ChangeTime), uint64(stat.FileID)
}

var localTraits = Traits{Identity: true, Reread: true}

// localReadDir lists dir with entries whose Info stats the path itself:
// the listing lags behind writes through a handle another process holds
// open, so a file a server keeps writing would look unchanged.
func localReadDir(dir string) ([]os.DirEntry, error) {
	entries, err := os.ReadDir(dir)

	listed := make([]lstatEntry, len(entries))
	for i, entry := range entries {
		listed[i] = lstatEntry{DirEntry: entry, dir: dir}
		entries[i] = &listed[i]
	}

	return entries, err
}

// openLocalRoot opens no root here: a walk on the local disk lists and
// opens by path.
func openLocalRoot(string) (*os.Root, error) {
	return nil, nil
}

// listIn and openIn are never called without a root from openLocalRoot.
func listIn(*os.Root, string) ([]os.DirEntry, os.FileInfo, error) {
	return nil, nil, errors.ErrUnsupported
}

func openIn(*os.Root, string, string, os.FileInfo) (File, error) {
	return nil, errors.ErrUnsupported
}

type lstatEntry struct {
	os.DirEntry
	dir string
}

func (e *lstatEntry) Info() (os.FileInfo, error) {
	return lstat(filepath.Join(e.dir, e.Name()), e.Name())
}

// lstat is os.Lstat with fileStatInformation as its Sys, at the cost of
// one query by name.
func lstat(path, name string) (os.FileInfo, error) {
	stat, err := statByName(path)
	if err != nil {
		return os.Lstat(path)
	}

	if stat.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		info, err := os.Lstat(path)
		if err != nil {
			return nil, err
		}

		return &reparsed{FileInfo: info, stat: stat}, nil
	}

	return &identified{name: name, stat: stat}, nil
}

// reparsed is os.Lstat's view of a reparse point, which knows a symlink
// from a placeholder, with fileStatInformation as its Sys.
type reparsed struct {
	os.FileInfo
	stat fileStatInformation
}

func (r *reparsed) Sys() any { return &r.stat }

var queryInformationByName = windows.NewLazySystemDLL("ntdll.dll").NewProc("NtQueryInformationByName")

const fileStatInformationClass = 68

// fileStatInformation is FILE_STAT_INFORMATION.
type fileStatInformation struct {
	FileID          int64
	CreationTime    int64
	LastAccessTime  int64
	LastWriteTime   int64
	ChangeTime      int64
	AllocationSize  int64
	EndOfFile       int64
	FileAttributes  uint32
	ReparseTag      uint32
	NumberOfLinks   uint32
	EffectiveAccess uint32
}

func statByName(path string) (fileStatInformation, error) {
	var stat fileStatInformation

	if err := queryInformationByName.Find(); err != nil {
		return stat, err
	}

	dosName, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return stat, err
	}

	var ntName windows.NTUnicodeString
	if err := windows.RtlDosPathNameToNtPathName(dosName, &ntName, nil, nil); err != nil {
		return stat, err
	}
	defer windows.LocalFree(windows.Handle(unsafe.Pointer(ntName.Buffer)))

	attrs := windows.OBJECT_ATTRIBUTES{
		Length:     uint32(unsafe.Sizeof(windows.OBJECT_ATTRIBUTES{})),
		ObjectName: &ntName,
		Attributes: windows.OBJ_CASE_INSENSITIVE,
	}

	var iosb windows.IO_STATUS_BLOCK
	status, _, _ := queryInformationByName.Call(
		uintptr(unsafe.Pointer(&attrs)),
		uintptr(unsafe.Pointer(&iosb)),
		uintptr(unsafe.Pointer(&stat)),
		unsafe.Sizeof(stat),
		fileStatInformationClass,
	)
	if status != 0 {
		return stat, windows.NTStatus(status)
	}

	return stat, nil
}

// filetimeNs converts 100-nanosecond intervals since 1601 to Unix
// nanoseconds, as os.FileInfo.ModTime does.
func filetimeNs(t int64) int64 {
	ft := windows.Filetime{LowDateTime: uint32(t), HighDateTime: uint32(t >> 32)}
	return ft.Nanoseconds()
}

// identified is what os.Lstat returns for a file or directory that is not
// a reparse point, with fileStatInformation as its Sys.
type identified struct {
	name string
	stat fileStatInformation
}

func (i *identified) Name() string { return i.name }

func (i *identified) Size() int64 { return i.stat.EndOfFile }

func (i *identified) Mode() os.FileMode {
	mode := os.FileMode(0o666)
	if i.stat.FileAttributes&windows.FILE_ATTRIBUTE_READONLY != 0 {
		mode = 0o444
	}

	if i.stat.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		mode |= os.ModeDir | 0o111
	}

	return mode
}

func (i *identified) ModTime() time.Time { return time.Unix(0, filetimeNs(i.stat.LastWriteTime)) }

func (i *identified) IsDir() bool { return i.Mode().IsDir() }

func (i *identified) Sys() any { return &i.stat }

// openShared opens path to read without keeping a restore from renaming
// it meanwhile.
func openShared(path string) (*os.File, error) {
	name, err := windows.UTF16PtrFromString(extended(path))
	if err != nil {
		return nil, err
	}

	share := uint32(windows.FILE_SHARE_READ | windows.FILE_SHARE_WRITE | windows.FILE_SHARE_DELETE)

	handle, err := windows.CreateFile(name, windows.GENERIC_READ, share, nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}

	return os.NewFile(uintptr(handle), path), nil
}
