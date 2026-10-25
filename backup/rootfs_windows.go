//go:build windows

package backup

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

var _ MetadataWriteFS = (*rootedFS)(nil)

// rootRel is rel with the trailing dots and spaces of each element
// dropped, as Windows drops them from a path that a root does not hold.
func rootRel(rel string) string {
	parts := strings.Split(rel, `\`)
	for i, part := range parts {
		if trimmed := strings.TrimRight(part, ". "); trimmed != "" {
			parts[i] = trimmed
		}
	}

	return strings.Join(parts, `\`)
}

// parent opens the directory holding path through its root and returns it
// with path's last element; dir is nil for a path under no root or a root
// itself.
func (l *rootedFS) parent(path string) (dir *os.File, name string, err error) {
	root, rel := l.resolve(path)
	if root == nil || rel == "." {
		return nil, "", nil
	}

	return openParent(root, rel, path)
}

func openParent(root *os.Root, rel, path string) (*os.File, string, error) {
	dir, err := root.Open(filepath.Dir(rel))
	if err != nil {
		return nil, "", named(err, filepath.Dir(path))
	}

	return dir, filepath.Base(rel), nil
}

// WriteStream implements MetadataWriteFS.
func (l *rootedFS) WriteStream(ctx context.Context, path, name string, r io.Reader) error {
	dir, entry, err := l.parent(path)
	if err != nil {
		return err
	}

	if dir == nil {
		return LocalFS{}.WriteStream(ctx, path, name, r)
	}
	defer dir.Close()

	return writeStream(path, name, r, func() (*os.File, error) {
		handle, err := openAt(dir, entry+":"+name, windows.FILE_GENERIC_WRITE, windows.FILE_OVERWRITE_IF, 0)
		if err != nil {
			return nil, named(err, path+":"+name)
		}

		return os.NewFile(uintptr(handle), path+":"+name), nil
	})
}

// SetSecurity implements MetadataWriteFS.
func (l *rootedFS) SetSecurity(ctx context.Context, path string, sd []byte) error {
	dir, entry, err := l.parent(path)
	if err != nil {
		return err
	}

	if dir == nil {
		return LocalFS{}.SetSecurity(ctx, path, sd)
	}
	defer dir.Close()

	return putSecurityOn(path, sd, func(access uint32) (windows.Handle, error) { return openMetadataAt(dir, entry, path, access) })
}

// SetAttributes implements MetadataWriteFS.
func (l *rootedFS) SetAttributes(ctx context.Context, path string, birth time.Time, attributes uint32) error {
	dir, entry, err := l.parent(path)
	if err != nil {
		return err
	}

	if dir == nil {
		return LocalFS{}.SetAttributes(ctx, path, birth, attributes)
	}
	defer dir.Close()

	return putAttributesOn(path, birth, attributes, func(access uint32) (windows.Handle, error) { return openMetadataAt(dir, entry, path, access) })
}

func openMetadataAt(dir *os.File, name, path string, access uint32) (windows.Handle, error) {
	handle, err := openAt(dir, name, access, windows.FILE_OPEN, windows.FILE_OPEN_FOR_BACKUP_INTENT)
	if err != nil {
		return windows.InvalidHandle, named(err, path)
	}

	return handle, nil
}

// openAt opens name in dir itself, never a reparse point's target, as
// NtCreateFile does with the disposition and options given.
func openAt(dir *os.File, name string, access, disposition, options uint32) (windows.Handle, error) {
	objectName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return windows.InvalidHandle, &os.PathError{Op: "open", Path: name, Err: err}
	}

	attrs := &windows.OBJECT_ATTRIBUTES{RootDirectory: windows.Handle(dir.Fd()), ObjectName: objectName, Attributes: windows.OBJ_CASE_INSENSITIVE}
	attrs.Length = uint32(unsafe.Sizeof(*attrs))

	share := uint32(windows.FILE_SHARE_READ | windows.FILE_SHARE_WRITE | windows.FILE_SHARE_DELETE)

	var handle windows.Handle

	err = windows.NtCreateFile(&handle, access|windows.SYNCHRONIZE, attrs, &windows.IO_STATUS_BLOCK{}, nil, windows.FILE_ATTRIBUTE_NORMAL, share, disposition,
		options|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT, 0, 0)
	if err != nil {
		var status windows.NTStatus
		if errors.As(err, &status) {
			err = status.Errno()
		}

		return windows.InvalidHandle, &os.PathError{Op: "NtCreateFile", Path: name, Err: err}
	}

	return handle, nil
}

// symlinkPrivilege enables the privilege an administrator makes links
// with, once.
var symlinkPrivilege = sync.OnceFunc(func() { enablePrivileges("SeCreateSymbolicLinkPrivilege") })

const symlinkFlagRelative = 1

// symlinkIn makes at rel in root the link os.Symlink would make at path: a
// directory link when target, relative to path's directory, is one.
func symlinkIn(root *os.Root, target, rel, path string) error {
	if target == "" {
		return &os.LinkError{Op: "symlink", Old: target, New: path, Err: syscall.EINVAL}
	}

	target = filepath.FromSlash(target)
	if filepath.VolumeName(target) != "" && !filepath.IsAbs(target) {
		if full, err := syscall.FullPath(target); err == nil {
			target = full
		}
	}

	dest := target
	if filepath.VolumeName(target) == "" {
		if os.IsPathSeparator(target[0]) {
			dest = filepath.VolumeName(path) + target
		} else {
			dest = filepath.Join(filepath.Dir(path), target)
		}
	}

	info, err := os.Stat(dest)
	isDir := err == nil && info.IsDir()

	dir, name, err := openParent(root, rel, path)
	if err != nil {
		return err
	}
	defer dir.Close()

	if err := symlinkAt(dir, name, target, isDir); err != nil {
		return &os.LinkError{Op: "symlink", Old: target, New: path, Err: err}
	}

	return nil
}

func symlinkAt(dir *os.File, name, target string, isDir bool) error {
	symlinkPrivilege()

	options := uint32(windows.FILE_NON_DIRECTORY_FILE)
	if isDir {
		options = windows.FILE_DIRECTORY_FILE
	}

	handle, err := openAt(dir, name, windows.FILE_WRITE_ATTRIBUTES|windows.DELETE, windows.FILE_CREATE, options|windows.FILE_OPEN_FOR_BACKUP_INTENT)
	if err != nil {
		return errors.Unwrap(err)
	}
	defer windows.CloseHandle(handle)

	relative := filepath.VolumeName(target) == ""

	substitute := target
	switch {
	case relative:
	case strings.HasPrefix(target, `\\?\`):
		substitute = `\??\` + target[4:]
	case strings.HasPrefix(target, `\\`):
		substitute = `\??\UNC\` + target[2:]
	default:
		substitute = `\??\` + target
	}

	sub16, err := windows.UTF16FromString(substitute)
	if err != nil {
		return err
	}

	print16, err := windows.UTF16FromString(target)
	if err != nil {
		return err
	}

	sub16, print16 = sub16[:len(sub16)-1], print16[:len(print16)-1]

	// a REPARSE_DATA_BUFFER holding a SymbolicLinkReparseBuffer, the
	// substitute name, as the NT path an absolute target is, then the
	// print name
	const header, linkHeader = 8, 12

	names := append(sub16, print16...)
	buf := make([]byte, header+linkHeader+2*len(names))
	binary.LittleEndian.PutUint32(buf[0:], windows.IO_REPARSE_TAG_SYMLINK)
	binary.LittleEndian.PutUint16(buf[4:], uint16(linkHeader+2*len(names)))
	binary.LittleEndian.PutUint16(buf[10:], uint16(2*len(sub16)))
	binary.LittleEndian.PutUint16(buf[12:], uint16(2*len(sub16)))
	binary.LittleEndian.PutUint16(buf[14:], uint16(2*len(print16)))

	if relative {
		binary.LittleEndian.PutUint32(buf[16:], symlinkFlagRelative)
	}

	for i, c := range names {
		binary.LittleEndian.PutUint16(buf[header+linkHeader+2*i:], c)
	}

	var returned uint32

	err = windows.DeviceIoControl(handle, windows.FSCTL_SET_REPARSE_POINT, &buf[0], uint32(len(buf)), nil, 0, &returned, nil)
	if err != nil {
		remove := byte(1)
		_ = windows.SetFileInformationByHandle(handle, windows.FileDispositionInfo, &remove, 1)

		return err
	}

	return nil
}
