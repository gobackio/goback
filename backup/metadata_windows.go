//go:build windows

package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/gobackio/goback/proto"
	"golang.org/x/sys/windows"
)

var _ MetadataFS = LocalFS{}

const (
	seBackupPrivilege   = "SeBackupPrivilege"
	seRestorePrivilege  = "SeRestorePrivilege"
	seSecurityPrivilege = "SeSecurityPrivilege"
)

var (
	advapi32              = windows.NewLazySystemDLL("advapi32.dll")
	adjustTokenPrivileges = advapi32.NewProc("AdjustTokenPrivileges")

	kernel32        = windows.NewLazySystemDLL("kernel32.dll")
	findFirstStream = kernel32.NewProc("FindFirstStreamW")
	findNextStream  = kernel32.NewProc("FindNextStreamW")
)

// readPrivileges enables what an elevated administrator may use to read
// every file and its SACL, once, and reports which it got.
var readPrivileges = sync.OnceValue(func() map[string]bool {
	return enablePrivileges(seBackupPrivilege, seSecurityPrivilege)
})

// writePrivileges is readPrivileges for setting any owner and the SACL.
var writePrivileges = sync.OnceValue(func() map[string]bool {
	return enablePrivileges(seRestorePrivilege, seSecurityPrivilege)
})

// enablePrivileges enables each privilege the process token holds and
// reports which are enabled.
func enablePrivileges(names ...string) map[string]bool {
	enabled := map[string]bool{}

	var token windows.Token
	if err := windows.OpenProcessToken(windows.CurrentProcess(), windows.TOKEN_ADJUST_PRIVILEGES|windows.TOKEN_QUERY, &token); err != nil {
		return enabled
	}
	defer token.Close()

	for _, name := range names {
		var luid windows.LUID
		if err := windows.LookupPrivilegeValue(nil, windows.StringToUTF16Ptr(name), &luid); err != nil {
			continue
		}

		privileges := windows.Tokenprivileges{
			PrivilegeCount: 1,
			Privileges:     [1]windows.LUIDAndAttributes{{Luid: luid, Attributes: windows.SE_PRIVILEGE_ENABLED}},
		}

		// it succeeds without enabling a privilege the token lacks, which
		// only the last error tells
		ok, _, err := adjustTokenPrivileges.Call(uintptr(token), 0, uintptr(unsafe.Pointer(&privileges)), 0, 0, 0)
		enabled[name] = ok != 0 && !errors.Is(err, windows.ERROR_NOT_ALL_ASSIGNED)
	}

	return enabled
}

// Security implements MetadataFS.
func (LocalFS) Security(_ context.Context, path string) ([]byte, error) {
	info := windows.SECURITY_INFORMATION(windows.OWNER_SECURITY_INFORMATION | windows.GROUP_SECURITY_INFORMATION | windows.DACL_SECURITY_INFORMATION)
	if readPrivileges()[seSecurityPrivilege] {
		info |= windows.SACL_SECURITY_INFORMATION
	}

	sd, err := windows.GetNamedSecurityInfo(extended(path), windows.SE_FILE_OBJECT, info)
	if err != nil {
		return nil, &os.PathError{Op: "GetNamedSecurityInfo", Path: path, Err: err}
	}

	return unsafe.Slice((*byte)(unsafe.Pointer(sd)), sd.Length()), nil
}

// win32FindStreamData is WIN32_FIND_STREAM_DATA.
type win32FindStreamData struct {
	StreamSize int64
	StreamName [windows.MAX_PATH + 36]uint16
}

// Streams implements MetadataFS.
func (LocalFS) Streams(_ context.Context, path string) ([]StreamInfo, error) {
	readPrivileges()

	name, err := windows.UTF16PtrFromString(extended(path))
	if err != nil {
		return nil, err
	}

	var data win32FindStreamData

	handle, _, err := findFirstStream.Call(uintptr(unsafe.Pointer(name)), 0, uintptr(unsafe.Pointer(&data)), 0)
	if windows.Handle(handle) == windows.InvalidHandle {
		if errors.Is(err, windows.ERROR_HANDLE_EOF) {
			return nil, nil
		}

		return nil, &os.PathError{Op: "FindFirstStreamW", Path: path, Err: err}
	}
	defer windows.FindClose(windows.Handle(handle))

	var streams []StreamInfo

	for {
		if name, ok := streamName(windows.UTF16ToString(data.StreamName[:])); ok {
			streams = append(streams, StreamInfo{Name: name, Size: data.StreamSize})
		}

		ok, _, err := findNextStream.Call(handle, uintptr(unsafe.Pointer(&data)))
		if ok == 0 {
			if errors.Is(err, windows.ERROR_HANDLE_EOF) {
				return streams, nil
			}

			return nil, &os.PathError{Op: "FindNextStreamW", Path: path, Err: err}
		}
	}
}

// streamName takes the name out of ":name:$DATA", and reports false for
// the unnamed stream that holds the content.
func streamName(listed string) (string, bool) {
	name := strings.TrimSuffix(strings.TrimPrefix(listed, ":"), ":$DATA")

	return name, name != ""
}

// OpenStream implements MetadataFS.
func (LocalFS) OpenStream(_ context.Context, path, name string) (File, error) {
	readPrivileges()

	file, err := os.Open(path + ":" + name)
	if err != nil {
		return nil, err
	}

	return file, nil
}

// extended returns path in the \\?\ form, which Win32 calls that Go does
// not wrap need beyond MAX_PATH.
func extended(path string) string {
	if strings.HasPrefix(path, `\\?\`) || strings.HasPrefix(path, `\\.\`) {
		return path
	}

	if !filepath.IsAbs(path) {
		abs, err := filepath.Abs(path)
		if err != nil {
			return path
		}

		path = abs
	}

	if strings.HasPrefix(path, `\\`) {
		return `\\?\UNC\` + path[2:]
	}

	return `\\?\` + path
}

// recordPlatform adds the change and creation times, the attributes and a
// reparse tag to a stat taken from LocalFS.
func recordPlatform(stat *proto.FileInfo, info os.FileInfo) {
	switch sys := info.Sys().(type) {
	case *fileStatInformation:
		stat.CtimeNs = filetimeNs(sys.ChangeTime)
		stat.BirthtimeNs = filetimeNs(sys.CreationTime)
		stat.Attributes = sys.FileAttributes

		if sys.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 && sys.ReparseTag != windows.IO_REPARSE_TAG_SYMLINK {
			stat.ReparseTag = sys.ReparseTag
		}
	case *syscall.Win32FileAttributeData:
		stat.BirthtimeNs = sys.CreationTime.Nanoseconds()
		stat.Attributes = sys.FileAttributes
	}
}

var _ MetadataWriteFS = LocalFS{}

// WriteStream implements MetadataWriteFS.
func (LocalFS) WriteStream(_ context.Context, path, name string, r io.Reader) error {
	if name == "" || strings.ContainsAny(name, "\\/:\x00") {
		return fmt.Errorf("%s: %q is not a stream name", path, name)
	}

	file, err := os.OpenFile(path+":"+name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		return err
	}

	_, err = io.Copy(file, r)
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}

	return err
}

// SetSecurity implements MetadataWriteFS. It sets the descriptor on path
// alone, without the propagation to what a directory holds that
// SetNamedSecurityInfo would do.
func (LocalFS) SetSecurity(_ context.Context, path string, sd []byte) error {
	privileges := writePrivileges()
	if !privileges[seRestorePrivilege] {
		return ErrNotPrivileged
	}

	if len(sd) < int(unsafe.Sizeof(windows.SECURITY_DESCRIPTOR{})) {
		return fmt.Errorf("%s: the recorded security descriptor is %d bytes", path, len(sd))
	}

	desc := (*windows.SECURITY_DESCRIPTOR)(unsafe.Pointer(&sd[0]))
	if !desc.IsValid() || desc.Length() != uint32(len(sd)) {
		return fmt.Errorf("%s: the recorded security descriptor is not valid", path)
	}

	info := windows.SECURITY_INFORMATION(windows.OWNER_SECURITY_INFORMATION | windows.GROUP_SECURITY_INFORMATION | windows.DACL_SECURITY_INFORMATION)
	access := uint32(windows.WRITE_DAC | windows.WRITE_OWNER)

	if sacl, _, err := desc.SACL(); err == nil && sacl != nil && privileges[seSecurityPrivilege] {
		info |= windows.SACL_SECURITY_INFORMATION
		access |= windows.ACCESS_SYSTEM_SECURITY
	}

	handle, err := openForMetadata(path, access)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(handle)

	if err := windows.SetKernelObjectSecurity(handle, info, desc); err != nil {
		return &os.PathError{Op: "SetKernelObjectSecurity", Path: path, Err: err}
	}

	return nil
}

// settableAttributes are the FILE_ATTRIBUTE_* flags SetAttributes gives.
const settableAttributes = windows.FILE_ATTRIBUTE_READONLY | windows.FILE_ATTRIBUTE_HIDDEN |
	windows.FILE_ATTRIBUTE_SYSTEM | windows.FILE_ATTRIBUTE_ARCHIVE |
	windows.FILE_ATTRIBUTE_NOT_CONTENT_INDEXED | windows.FILE_ATTRIBUTE_TEMPORARY

// fileBasicInfo is FILE_BASIC_INFO; a zero time leaves that time alone.
type fileBasicInfo struct {
	CreationTime   int64
	LastAccessTime int64
	LastWriteTime  int64
	ChangeTime     int64
	FileAttributes uint32
	_              uint32
}

// SetAttributes implements MetadataWriteFS.
func (LocalFS) SetAttributes(_ context.Context, path string, birth time.Time, attributes uint32) error {
	writePrivileges()

	info := fileBasicInfo{FileAttributes: attributes & settableAttributes}
	if info.FileAttributes == 0 {
		info.FileAttributes = windows.FILE_ATTRIBUTE_NORMAL
	}

	if !birth.IsZero() {
		ft := windows.NsecToFiletime(birth.UnixNano())
		info.CreationTime = int64(ft.HighDateTime)<<32 | int64(ft.LowDateTime)
	}

	handle, err := openForMetadata(path, windows.FILE_WRITE_ATTRIBUTES)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(handle)

	if err := windows.SetFileInformationByHandle(handle, windows.FileBasicInfo, (*byte)(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info))); err != nil {
		return &os.PathError{Op: "SetFileInformationByHandle", Path: path, Err: err}
	}

	return nil
}

// openForMetadata opens path itself, a directory or a reparse point too,
// with backup semantics, which a process holding SeRestorePrivilege opens
// whatever the DACL says.
func openForMetadata(path string, access uint32) (windows.Handle, error) {
	name, err := windows.UTF16PtrFromString(extended(path))
	if err != nil {
		return windows.InvalidHandle, err
	}

	share := uint32(windows.FILE_SHARE_READ | windows.FILE_SHARE_WRITE | windows.FILE_SHARE_DELETE)

	handle, err := windows.CreateFile(name, access, share, nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		return windows.InvalidHandle, &os.PathError{Op: "CreateFile", Path: path, Err: err}
	}

	return handle, nil
}
