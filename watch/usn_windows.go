package watch

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"golang.org/x/sys/windows"
)

// MaxFiles bounds how many changed files a read of the NTFS change journal
// looks at for other hard links before it counts reading the tree in full
// as cheaper.
const MaxFiles = 200_000

const (
	fsctlQueryUSNJournal = 0x000900f4
	fsctlReadUSNJournal  = 0x000900bb

	usnReasonFileCreate    = 0x00000100
	usnReasonRenameNewName = 0x00002000
)

// usnSource reads the NTFS change journal of the volume holding a root.
// The journal outlives the process, so a position stays good across
// restarts as long as the journal reaches back to it. It needs an elevated
// administrator or LocalSystem.
type usnSource struct {
	root string
	id   fileIdentity
}

// fileIdentity tells a directory from another that takes its path.
type fileIdentity struct {
	volume uint32
	index  uint64
}

func openUSN(root string) (*usnSource, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}

	id, err := identifyDir(root)
	if err != nil {
		return nil, err
	}

	if _, err := journalNow(root); err != nil {
		return nil, fmt.Errorf("%w: the change journal: %w", ErrUnavailable, err)
	}

	return &usnSource{root: root, id: id}, nil
}

func identifyDir(path string) (fileIdentity, error) {
	h, err := openDir(path)
	if err != nil {
		return fileIdentity{}, err
	}
	defer windows.CloseHandle(h)

	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(h, &info); err != nil {
		return fileIdentity{}, err
	}

	return fileIdentity{volume: info.VolumeSerialNumber, index: uint64(info.FileIndexHigh)<<32 | uint64(info.FileIndexLow)}, nil
}

func (s *usnSource) Backend() string { return "usn" }

// checkRoot reports the root moving away or being replaced.
func (s *usnSource) checkRoot() error {
	if id, err := identifyDir(s.root); err != nil || id != s.id {
		return fmt.Errorf("%w: the root was moved, removed or replaced", ErrInDoubt)
	}

	return nil
}

func (s *usnSource) Now() (Position, error) {
	if err := s.checkRoot(); err != nil {
		return Position{}, err
	}

	p, err := journalNow(s.root)
	if err != nil {
		return Position{}, err
	}

	return p.position(), nil
}

func (s *usnSource) Read(from Position) (*Changes, Position, error) {
	if err := s.checkRoot(); err != nil {
		return nil, Position{}, err
	}

	p, ok := usnPositionOf(from)
	if !ok {
		return nil, Position{}, fmt.Errorf("%w: the position isn't the change journal's", ErrInDoubt)
	}

	changes, end, err := journalRead(s.root, p)
	if err != nil {
		return nil, Position{}, err
	}

	return changes, end.position(), nil
}

func (s *usnSource) Close() error { return nil }

// usnPosition is how far a volume's change journal has been read.
type usnPosition struct {
	journalID uint64
	usn       int64
}

func (p usnPosition) position() Position {
	return Position{Stream: "usn-" + strconv.FormatUint(p.journalID, 16), Seq: p.usn}
}

func usnPositionOf(p Position) (usnPosition, bool) {
	id, ok := strings.CutPrefix(p.Stream, "usn-")
	if !ok {
		return usnPosition{}, false
	}

	journalID, err := strconv.ParseUint(id, 16, 64)
	if err != nil {
		return usnPosition{}, false
	}

	return usnPosition{journalID: journalID, usn: p.Seq}, true
}

// journalNow is the position of the end of the journal of the volume
// holding root.
func journalNow(root string) (usnPosition, error) {
	v, err := openVolume(root)
	if err != nil {
		return usnPosition{}, err
	}
	defer windows.CloseHandle(v)

	data, err := queryJournal(v)
	if err != nil {
		return usnPosition{}, err
	}

	return usnPosition{journalID: data.UsnJournalID, usn: data.NextUsn}, nil
}

// journalRead reads the journal of the volume holding root from from to
// its end: the directories under root that changed, including each
// directory holding another hard link of a changed file, the directories
// created or renamed as trees, and the position of the end.
func journalRead(root string, from usnPosition) (*Changes, usnPosition, error) {
	v, err := openVolume(root)
	if err != nil {
		return nil, usnPosition{}, err
	}
	defer windows.CloseHandle(v)

	data, err := queryJournal(v)
	if err != nil {
		return nil, usnPosition{}, err
	}

	switch {
	case data.UsnJournalID != from.journalID:
		return nil, usnPosition{}, fmt.Errorf("%w: the journal was recreated", ErrInDoubt)
	case from.usn < data.LowestValidUsn || from.usn < data.FirstUsn:
		return nil, usnPosition{}, fmt.Errorf("%w: it no longer reaches back to %d", ErrInDoubt, from.usn)
	}

	parents, files, trees := map[fileID]int64{}, map[fileID]int64{}, map[fileID]int64{}
	start := from.usn

	for start < data.NextUsn {
		next, records, err := readJournal(v, start, data.UsnJournalID)
		if err != nil {
			return nil, usnPosition{}, err
		}

		for _, r := range records {
			parents[r.parent] = max(parents[r.parent], r.usn)

			switch {
			case r.attributes&windows.FILE_ATTRIBUTE_DIRECTORY == 0:
				files[r.file] = max(files[r.file], r.usn)
			case r.reason&(usnReasonFileCreate|usnReasonRenameNewName) != 0:
				trees[r.file] = max(trees[r.file], r.usn)
			}
		}

		if len(parents)+len(trees) > MaxDirs || len(files) > MaxFiles {
			return nil, usnPosition{}, fmt.Errorf("%w: more than %d directories or %d files on the volume changed", ErrInDoubt, MaxDirs, MaxFiles)
		}

		if next <= start {
			break
		}

		start = next
	}

	changes, err := changesOf(root, parents, files, trees)
	if err != nil {
		return nil, usnPosition{}, err
	}

	return changes, usnPosition{journalID: data.UsnJournalID, usn: start}, nil
}

// openVolume opens the volume holding path for its change journal.
func openVolume(path string) (windows.Handle, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return 0, err
	}

	mount := make([]uint16, windows.MAX_PATH+1)
	if err := windows.GetVolumePathName(windows.StringToUTF16Ptr(abs), &mount[0], uint32(len(mount))); err != nil {
		return 0, fmt.Errorf("finding the volume of %s: %w", path, err)
	}

	name := make([]uint16, 64)
	if err := windows.GetVolumeNameForVolumeMountPoint(&mount[0], &name[0], uint32(len(name))); err != nil {
		return 0, fmt.Errorf("naming the volume of %s: %w", path, err)
	}

	volume := strings.TrimSuffix(windows.UTF16ToString(name), `\`)

	h, err := windows.CreateFile(windows.StringToUTF16Ptr(volume), windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE, nil, windows.OPEN_EXISTING, 0, 0)
	if err != nil {
		return 0, fmt.Errorf("opening %s: %w", volume, err)
	}

	return h, nil
}

// journalData is USN_JOURNAL_DATA_V0.
type journalData struct {
	UsnJournalID    uint64
	FirstUsn        int64
	NextUsn         int64
	LowestValidUsn  int64
	MaxUsn          int64
	MaximumSize     uint64
	AllocationDelta uint64
}

func queryJournal(v windows.Handle) (journalData, error) {
	var data journalData
	var n uint32

	err := windows.DeviceIoControl(v, fsctlQueryUSNJournal, nil, 0, (*byte)(unsafe.Pointer(&data)), uint32(unsafe.Sizeof(data)), &n, nil)

	switch {
	case errors.Is(err, windows.ERROR_JOURNAL_NOT_ACTIVE), errors.Is(err, windows.ERROR_JOURNAL_DELETE_IN_PROGRESS), errors.Is(err, windows.ERROR_INVALID_FUNCTION):
		return data, fmt.Errorf("%w: the volume keeps no change journal", ErrInDoubt)
	case err != nil:
		return data, fmt.Errorf("querying the change journal: %w", err)
	}

	return data, nil
}

// readData is READ_USN_JOURNAL_DATA_V1.
type readData struct {
	StartUsn          int64
	ReasonMask        uint32
	ReturnOnlyOnClose uint32
	Timeout           uint64
	BytesToWaitFor    uint64
	UsnJournalID      uint64
	MinMajorVersion   uint16
	MaxMajorVersion   uint16
}

func readJournal(v windows.Handle, start int64, journal uint64) (int64, []record, error) {
	in := readData{StartUsn: start, ReasonMask: 0xffffffff, UsnJournalID: journal, MinMajorVersion: 2, MaxMajorVersion: 3}
	buf := make([]byte, 1<<16)

	var n uint32

	err := windows.DeviceIoControl(v, fsctlReadUSNJournal, (*byte)(unsafe.Pointer(&in)), uint32(unsafe.Sizeof(in)), &buf[0], uint32(len(buf)), &n, nil)
	switch {
	case errors.Is(err, windows.ERROR_JOURNAL_ENTRY_DELETED):
		return 0, nil, fmt.Errorf("%w: it no longer reaches back to %d", ErrInDoubt, start)
	case errors.Is(err, windows.ERROR_JOURNAL_NOT_ACTIVE):
		return 0, nil, fmt.Errorf("%w: the volume keeps no change journal", ErrInDoubt)
	}

	if err != nil {
		return 0, nil, fmt.Errorf("reading the change journal: %w", err)
	}

	return parseJournal(buf[:n])
}

// fileID is a file reference number, 64-bit on NTFS and 128-bit on ReFS.
type fileID [16]byte

type record struct {
	file, parent fileID
	usn          int64
	reason       uint32
	attributes   uint32
}

// parseJournal reads the next USN and the USN_RECORD_V2 and V3 records of
// a journal read's output.
func parseJournal(buf []byte) (int64, []record, error) {
	if len(buf) < 8 {
		return 0, nil, errors.New("a change journal read returned no next position")
	}

	next := int64(binary.LittleEndian.Uint64(buf))

	var records []record

	for off := 8; off < len(buf); {
		if len(buf)-off < 8 {
			return 0, nil, errors.New("a change journal record is cut off")
		}

		length := int(binary.LittleEndian.Uint32(buf[off:]))
		major := binary.LittleEndian.Uint16(buf[off+4:])

		if length < 8 || off+length > len(buf) {
			return 0, nil, fmt.Errorf("a change journal record claims %d bytes", length)
		}

		rec := buf[off : off+length]

		var r record

		switch major {
		case 2:
			if len(rec) < 60 {
				return 0, nil, errors.New("a version 2 change journal record is cut off")
			}

			copy(r.file[:8], rec[8:16])
			copy(r.parent[:8], rec[16:24])
			r.usn = int64(binary.LittleEndian.Uint64(rec[24:]))
			r.reason = binary.LittleEndian.Uint32(rec[40:])
			r.attributes = binary.LittleEndian.Uint32(rec[52:])
		case 3:
			if len(rec) < 76 {
				return 0, nil, errors.New("a version 3 change journal record is cut off")
			}

			copy(r.file[:], rec[8:24])
			copy(r.parent[:], rec[24:40])
			r.usn = int64(binary.LittleEndian.Uint64(rec[40:]))
			r.reason = binary.LittleEndian.Uint32(rec[56:])
			r.attributes = binary.LittleEndian.Uint32(rec[68:])
		default:
			return 0, nil, fmt.Errorf("%w: a version %d change journal record", ErrInDoubt, major)
		}

		records = append(records, r)
		off += length
	}

	return next, records, nil
}

var (
	procOpenFileByID       = windows.NewLazySystemDLL("kernel32.dll").NewProc("OpenFileById")
	procRtlGetLastNtStatus = windows.NewLazySystemDLL("ntdll.dll").NewProc("RtlGetLastNtStatus")
	procAdjustPrivileges   = windows.NewLazySystemDLL("advapi32.dll").NewProc("AdjustTokenPrivileges")
)

func lastNtStatus() windows.NTStatus {
	r, _, _ := procRtlGetLastNtStatus.Call()
	return windows.NTStatus(r)
}

// fileIDDescriptor is FILE_ID_DESCRIPTOR with an ExtendedFileIdType id.
type fileIDDescriptor struct {
	Size uint32
	Type uint32
	ID   fileID
}

const extendedFileIDType = 2

// changesOf places the directories parents names under root, those
// holding other hard links of files, and the trees, each as of its USN;
// ones that no longer exist are left out, since their removal changed a
// directory that does.
func changesOf(root string, parents, files, trees map[fileID]int64) (*Changes, error) {
	h, err := openDir(root)
	if err != nil {
		return nil, fmt.Errorf("%w: opening the root: %w", ErrInDoubt, err)
	}
	defer windows.CloseHandle(h)

	base, err := finalPath(h)
	if err != nil {
		return nil, err
	}

	base = strings.ToLower(strings.TrimSuffix(base, `\`))
	c := NewChanges()

	for id, usn := range parents {
		path, err := pathOf(h, id)
		if err != nil {
			return nil, err
		}

		if rel, ok := under(base, path); ok {
			c.Mark(rel, usn)
		}
	}

	for id, usn := range trees {
		path, err := pathOf(h, id)
		if err != nil {
			return nil, err
		}

		if rel, ok := under(base, path); ok {
			c.MarkTree(rel, usn)
		}
	}

	for id, usn := range files {
		dirs, err := linkDirs(h, id)
		if err != nil {
			return nil, err
		}

		for _, dir := range dirs {
			if rel, ok := under(base, dir); ok {
				c.Mark(rel, usn)
			}
		}
	}

	return c, nil
}

// under is path as a lower-case slash-separated path below base, when it
// is at or below it.
func under(base, path string) (string, bool) {
	path = strings.ToLower(path)

	switch {
	case path == "":
		return "", false
	case path == base:
		return "", true
	case strings.HasPrefix(path, base+`\`):
		return filepath.ToSlash(path[len(base)+1:]), true
	}

	return "", false
}

func openDir(path string) (windows.Handle, error) {
	return windows.CreateFile(windows.StringToUTF16Ptr(path), 0,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
}

// pathOf is the path of the file id names on hint's volume, or "" when it
// no longer exists.
func pathOf(hint windows.Handle, id fileID) (string, error) {
	h, err := openByID(hint, id, 0)
	if err != nil || h == 0 {
		return "", err
	}
	defer windows.CloseHandle(h)

	return finalPath(h)
}

// backupPrivilege lets opening a file by id pass its DACL, so a file of any
// owner can be placed.
var backupPrivilege = sync.OnceFunc(func() {
	var token windows.Token
	if err := windows.OpenProcessToken(windows.CurrentProcess(), windows.TOKEN_ADJUST_PRIVILEGES|windows.TOKEN_QUERY, &token); err != nil {
		return
	}
	defer token.Close()

	var luid windows.LUID
	if err := windows.LookupPrivilegeValue(nil, windows.StringToUTF16Ptr("SeBackupPrivilege"), &luid); err != nil {
		return
	}

	privileges := windows.Tokenprivileges{
		PrivilegeCount: 1,
		Privileges:     [1]windows.LUIDAndAttributes{{Luid: luid, Attributes: windows.SE_PRIVILEGE_ENABLED}},
	}

	_, _, _ = procAdjustPrivileges.Call(uintptr(token), 0, uintptr(unsafe.Pointer(&privileges)), 0, 0, 0)
})

// openByID opens the file id names on hint's volume, or returns 0 when it
// no longer exists or is being deleted.
func openByID(hint windows.Handle, id fileID, access uint32) (windows.Handle, error) {
	backupPrivilege()

	desc := fileIDDescriptor{Size: uint32(unsafe.Sizeof(fileIDDescriptor{})), Type: extendedFileIDType, ID: id}

	// the NT status telling a file being deleted from a denied one is the
	// calling thread's
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	r, _, err := procOpenFileByID.Call(uintptr(hint), uintptr(unsafe.Pointer(&desc)), uintptr(access),
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, 0, windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT)

	h := windows.Handle(r)
	if h == windows.InvalidHandle {
		switch {
		case errors.Is(err, windows.ERROR_INVALID_PARAMETER), errors.Is(err, windows.ERROR_FILE_NOT_FOUND), errors.Is(err, windows.ERROR_PATH_NOT_FOUND):
			return 0, nil
		case errors.Is(err, windows.ERROR_ACCESS_DENIED) && lastNtStatus() == windows.STATUS_DELETE_PENDING:
			return 0, nil
		}

		return 0, fmt.Errorf("opening a changed file by id: %w", err)
	}

	return h, nil
}

var (
	procFindFirstFileName = windows.NewLazySystemDLL("kernel32.dll").NewProc("FindFirstFileNameW")
	procFindNextFileName  = windows.NewLazySystemDLL("kernel32.dll").NewProc("FindNextFileNameW")
)

// linkDirs is the directories holding the file id names on hint's volume
// when it has more than one hard link; a file with one link is in the
// directory its journal record names.
func linkDirs(hint windows.Handle, id fileID) ([]string, error) {
	h, err := openByID(hint, id, windows.FILE_READ_ATTRIBUTES)
	switch {
	// only the system's paging files refuse even this open, and they have
	// one link
	case errors.Is(err, windows.ERROR_SHARING_VIOLATION):
		return nil, nil
	case err != nil || h == 0:
		return nil, err
	}
	defer windows.CloseHandle(h)

	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(h, &info); err != nil {
		return nil, fmt.Errorf("reading a changed file's link count: %w", err)
	}

	if info.NumberOfLinks < 2 {
		return nil, nil
	}

	path, err := finalPath(h)
	if err != nil {
		return nil, err
	}

	names, err := linkNames(path)
	if err != nil {
		return nil, err
	}

	volume := filepath.VolumeName(path)
	dirs := make([]string, len(names))

	for i, name := range names {
		dirs[i] = filepath.Dir(volume + name)
	}

	return dirs, nil
}

// linkNames is every hard link of the file at path, each relative to the
// root of its volume.
func linkNames(path string) ([]string, error) {
	buf := make([]uint16, windows.MAX_LONG_PATH)
	n := uint32(len(buf))

	r, _, err := procFindFirstFileName.Call(uintptr(unsafe.Pointer(windows.StringToUTF16Ptr(path))), 0, uintptr(unsafe.Pointer(&n)), uintptr(unsafe.Pointer(&buf[0])))

	find := windows.Handle(r)
	if find == windows.InvalidHandle {
		return nil, fmt.Errorf("listing the hard links of %s: %w", path, err)
	}
	defer windows.FindClose(find)

	names := []string{windows.UTF16ToString(buf[:n])}

	for {
		n = uint32(len(buf))

		ok, _, err := procFindNextFileName.Call(uintptr(find), uintptr(unsafe.Pointer(&n)), uintptr(unsafe.Pointer(&buf[0])))
		if ok == 0 {
			if errors.Is(err, windows.ERROR_HANDLE_EOF) {
				return names, nil
			}

			return nil, fmt.Errorf("listing the hard links of %s: %w", path, err)
		}

		names = append(names, windows.UTF16ToString(buf[:n]))
	}
}

func finalPath(h windows.Handle) (string, error) {
	buf := make([]uint16, windows.MAX_LONG_PATH)

	n, err := windows.GetFinalPathNameByHandle(h, &buf[0], uint32(len(buf)), 0)
	if err != nil {
		return "", err
	}

	return windows.UTF16ToString(buf[:n]), nil
}
