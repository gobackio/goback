package watch

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

func idOf(t *testing.T, path string) fileID {
	t.Helper()

	h, err := openDir(path)
	require.NoError(t, err)
	defer windows.CloseHandle(h)

	var info windows.ByHandleFileInformation
	require.NoError(t, windows.GetFileInformationByHandle(h, &info))

	var id fileID
	binary.LittleEndian.PutUint32(id[0:], info.FileIndexLow)
	binary.LittleEndian.PutUint32(id[4:], info.FileIndexHigh)

	return id
}

func TestChangedDirectoriesAndTheirAncestorsAreNotUnchanged(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{"a/b/c", "a/quiet", "other", "gone", "Moved/In"} {
		require.NoError(t, os.MkdirAll(filepath.Join(root, filepath.FromSlash(dir)), 0o755))
	}

	outside := t.TempDir()
	gone := idOf(t, filepath.Join(root, "gone"))
	require.NoError(t, os.Remove(filepath.Join(root, "gone")))

	changes, err := changesOf(root, map[fileID]int64{
		idOf(t, filepath.Join(root, "a", "b")): 7,
		idOf(t, filepath.Join(root, "a")):      3,
		idOf(t, outside):                       9,
		gone:                                   9,
	}, nil, map[fileID]int64{idOf(t, filepath.Join(root, "Moved")): 5})
	require.NoError(t, err)

	require.Equal(t, map[string]int64{"": 7, "a": 7, "a/b": 7, "moved": 5}, changes.Dirs, "an ancestor holds its latest change; below a changed directory nothing changed")
	require.Equal(t, map[string]int64{"moved": 5}, changes.Trees, "a directory created or renamed counts as changed with everything under it")

	unchanged := changes.since(0)
	require.False(t, unchanged.unchanged(fold("Moved/In")))
	require.True(t, unchanged.unchanged(fold("A/Quiet")))
}

func TestADirectoryHoldingAnotherHardLinkOfAChangedFileChanged(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{"a", "b/deep", "c"} {
		require.NoError(t, os.MkdirAll(filepath.Join(root, filepath.FromSlash(dir)), 0o755))
	}

	shared := filepath.Join(root, "a", "shared.txt")
	require.NoError(t, os.WriteFile(shared, []byte("x"), 0o644))
	require.NoError(t, os.Link(shared, filepath.Join(root, "b", "deep", "shared.txt")))

	alone := filepath.Join(root, "c", "alone.txt")
	require.NoError(t, os.WriteFile(alone, []byte("y"), 0o644))

	changes, err := changesOf(root, map[fileID]int64{idOf(t, filepath.Join(root, "a")): 1}, map[fileID]int64{
		idOf(t, shared): 2,
		idOf(t, alone):  3,
	}, nil)
	require.NoError(t, err)

	require.Contains(t, changes.Dirs, "a")
	require.Equal(t, int64(2), changes.Dirs["b/deep"], "it holds another link of the changed file")
	require.Contains(t, changes.Dirs, "b")
	require.NotContains(t, changes.Dirs, "c", "a file with one link is placed by its record alone")
}

func TestAFileBeingDeletedIsGone(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "going.txt")
	require.NoError(t, os.WriteFile(path, []byte("x"), 0o644))

	id := idOf(t, path)

	h, err := windows.CreateFile(windows.StringToUTF16Ptr(path), windows.DELETE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING, 0, 0)
	require.NoError(t, err)
	defer windows.CloseHandle(h)

	disposition := uint32(1)
	require.NoError(t, windows.SetFileInformationByHandle(h, windows.FileDispositionInfo, (*byte)(unsafe.Pointer(&disposition)), uint32(unsafe.Sizeof(disposition))))

	changes, err := changesOf(root, map[fileID]int64{id: 1}, map[fileID]int64{id: 1}, nil)
	require.NoError(t, err)
	require.Zero(t, changes.Len())
}

func TestAnAppAliasIsPlacedAsItself(t *testing.T) {
	apps := filepath.Join(os.Getenv("LOCALAPPDATA"), "Microsoft", "WindowsApps")
	aliases, _ := filepath.Glob(filepath.Join(apps, "*.exe"))
	if len(aliases) == 0 {
		t.Skip("no app execution aliases on this machine")
	}

	h, err := windows.CreateFile(windows.StringToUTF16Ptr(aliases[0]), windows.FILE_READ_ATTRIBUTES,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	require.NoError(t, err)

	var info windows.ByHandleFileInformation
	require.NoError(t, windows.GetFileInformationByHandle(h, &info))
	windows.CloseHandle(h)

	var id fileID
	binary.LittleEndian.PutUint32(id[0:], info.FileIndexLow)
	binary.LittleEndian.PutUint32(id[4:], info.FileIndexHigh)

	_, err = changesOf(apps, map[fileID]int64{}, map[fileID]int64{id: 1}, nil)
	require.NoError(t, err, "an alias Windows can't follow is a file of its own")
}

func TestAPagingFileIsPlacedByItsRecord(t *testing.T) {
	id, ok := rootFileID(t, `C:\`, "pagefile.sys")
	if !ok {
		t.Skip("no paging file on C:")
	}

	_, err := changesOf(`C:\`, map[fileID]int64{}, map[fileID]int64{id: 1}, nil)
	require.NoError(t, err, "nothing can open a paging file, which has one link")
}

// rootFileID is the id of the entry name in dir, read from the directory's
// listing since the entry itself may not open.
func rootFileID(t *testing.T, dir, name string) (fileID, bool) {
	t.Helper()

	h, err := windows.CreateFile(windows.StringToUTF16Ptr(dir), windows.FILE_LIST_DIRECTORY,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	require.NoError(t, err)
	defer windows.CloseHandle(h)

	buf := make([]byte, 64<<10)
	for {
		err := windows.GetFileInformationByHandleEx(h, windows.FileIdBothDirectoryInfo, &buf[0], uint32(len(buf)))
		if errors.Is(err, windows.ERROR_NO_MORE_FILES) {
			return fileID{}, false
		}
		require.NoError(t, err)

		for off := 0; ; {
			entry := buf[off:]
			n := binary.LittleEndian.Uint32(entry[60:])
			units := unsafe.Slice((*uint16)(unsafe.Pointer(&entry[104])), n/2)

			if strings.EqualFold(windows.UTF16ToString(units), name) {
				var id fileID
				copy(id[:8], entry[96:104])

				return id, true
			}

			next := binary.LittleEndian.Uint32(entry)
			if next == 0 {
				break
			}
			off += int(next)
		}
	}
}

func record2(file, parent uint64, usn int64, reason uint32, name string) []byte {
	n := 60 + 2*len(name)
	n = (n + 7) &^ 7
	rec := make([]byte, n)
	binary.LittleEndian.PutUint32(rec[0:], uint32(n))
	binary.LittleEndian.PutUint16(rec[4:], 2)
	binary.LittleEndian.PutUint64(rec[8:], file)
	binary.LittleEndian.PutUint64(rec[16:], parent)
	binary.LittleEndian.PutUint64(rec[24:], uint64(usn))
	binary.LittleEndian.PutUint32(rec[40:], reason)
	binary.LittleEndian.PutUint32(rec[52:], windows.FILE_ATTRIBUTE_DIRECTORY)
	binary.LittleEndian.PutUint16(rec[56:], uint16(2*len(name)))
	binary.LittleEndian.PutUint16(rec[58:], 60)

	return rec
}

func record3(file, parent fileID, usn int64, reason uint32) []byte {
	rec := make([]byte, 80)
	binary.LittleEndian.PutUint32(rec[0:], 80)
	binary.LittleEndian.PutUint16(rec[4:], 3)
	copy(rec[8:], file[:])
	copy(rec[24:], parent[:])
	binary.LittleEndian.PutUint64(rec[40:], uint64(usn))
	binary.LittleEndian.PutUint32(rec[56:], reason)
	binary.LittleEndian.PutUint32(rec[68:], windows.FILE_ATTRIBUTE_ARCHIVE)
	binary.LittleEndian.PutUint16(rec[74:], 76)

	return rec
}

func TestJournalRecordsOfBothVersionsAreRead(t *testing.T) {
	buf := binary.LittleEndian.AppendUint64(nil, 4242)
	buf = append(buf, record2(7, 5, 4100, 0x100, "new.txt")...)

	var file, parent fileID
	file[15], parent[15] = 1, 2
	buf = append(buf, record3(file, parent, 4200, 0x200)...)

	next, records, err := parseJournal(buf)
	require.NoError(t, err)
	require.EqualValues(t, 4242, next)
	require.Len(t, records, 2)

	require.Equal(t, fileID{7}, records[0].file)
	require.Equal(t, fileID{5}, records[0].parent)
	require.EqualValues(t, 4100, records[0].usn)
	require.EqualValues(t, 0x100, records[0].reason)
	require.EqualValues(t, windows.FILE_ATTRIBUTE_DIRECTORY, records[0].attributes)
	require.Equal(t, parent, records[1].parent)
	require.EqualValues(t, 4200, records[1].usn)
	require.EqualValues(t, windows.FILE_ATTRIBUTE_ARCHIVE, records[1].attributes)

	_, _, err = parseJournal(buf[:len(buf)-4])
	require.Error(t, err, "a cut-off record")

	v4 := record3(file, parent, 0, 0)
	binary.LittleEndian.PutUint16(v4[4:], 4)
	_, _, err = parseJournal(append(binary.LittleEndian.AppendUint64(nil, 1), v4...))
	require.ErrorIs(t, err, ErrInDoubt)
}

func TestAJournalPositionSurvivesAsAPosition(t *testing.T) {
	p := usnPosition{journalID: 0x01d9_0000_beef, usn: 4242}

	back, ok := usnPositionOf(p.position())
	require.True(t, ok)
	require.Equal(t, p, back)

	_, ok = usnPositionOf(Position{Stream: "inotify-1", Seq: 4242})
	require.False(t, ok)
}

func elevated(t *testing.T) {
	if os.Getenv("GOBACK_ELEVATED_TESTS") == "" {
		t.Skip("reading a change journal needs an elevated shell and GOBACK_ELEVATED_TESTS=1")
	}
}

func TestTheJournalTellsWhichDirectoriesChanged(t *testing.T) {
	elevated(t)

	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "busy"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "quiet"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "old", "inner"), 0o755))

	src, err := openUSN(root)
	require.NoError(t, err)

	from, err := src.Now()
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(filepath.Join(root, "busy", "new.txt"), []byte("x"), 0o644))
	require.NoError(t, os.Rename(filepath.Join(root, "old"), filepath.Join(root, "renamed")))

	changes, end, err := src.Read(from)
	require.NoError(t, err)
	require.Greater(t, changes.Dirs["busy"], from.Seq)
	require.NotContains(t, changes.Dirs, "quiet")
	require.Contains(t, changes.Trees, "renamed")
	require.Equal(t, from.Stream, end.Stream)
	require.Greater(t, end.Seq, changes.Dirs["busy"])

	_, _, err = src.Read(Position{Stream: "usn-1", Seq: from.Seq})
	require.ErrorIs(t, err, ErrInDoubt)
}

func TestATrackerResumesFromAPositionAJournalStillHolds(t *testing.T) {
	elevated(t)

	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "a"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "b"), 0o755))

	src, err := openUSN(root)
	require.NoError(t, err)

	before, err := src.Now()
	require.NoError(t, err)

	// a change made while nothing follows the root, as while the agent is
	// down
	require.NoError(t, os.WriteFile(filepath.Join(root, "a", "new.txt"), []byte("x"), 0o644))

	tr := NewTracker(func() (Source, error) { return openUSN(root) })
	require.NoError(t, tr.Poll(before))

	unchanged, ok := tr.After(before)
	require.True(t, ok)
	require.False(t, unchanged("a"))
	require.True(t, unchanged("b"))
}
