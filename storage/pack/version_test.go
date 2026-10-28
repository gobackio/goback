package pack

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

func TestAnUnversionedIndexFileReadsWithEveryVersionUnset(t *testing.T) {
	idx := makeIndex()[:3]

	buf := new(bytes.Buffer)
	buf.Write(unversionedMagicBytes)
	require.NoError(t, binary.Write(buf, indexEndianness, uint32(len(idx))))
	for _, record := range idx {
		require.NoError(t, binary.Write(buf, indexEndianness, unversionedRecord{Sum: record.Sum, Offset: record.Offset, Length: record.Length, Type: record.Type}))
	}

	var read IndexFile
	_, err := read.ReadFrom(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.Equal(t, idx, read)

	path := filepath.Join(t.TempDir(), "old.idx")
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0o644))

	file, err := os.Open(path)
	require.NoError(t, err)

	scanner, err := newFileScanner(file)
	require.NoError(t, err)
	t.Cleanup(func() { _ = scanner.close() })

	for _, want := range idx {
		got, err := scanner.next()
		require.NoError(t, err)
		require.Equal(t, want, *got)
	}
}

func TestAVersionedIndexFileLaysEachRecordOutInItsFieldOrder(t *testing.T) {
	idx := makeIndex()[:2]
	idx[1] = idx[1].Carry(Version{Time: time.Unix(1_790_000_000, 1000), Offset: 9})

	want := new(bytes.Buffer)
	want.Write(indexFileMagicBytes)
	require.NoError(t, binary.Write(want, indexEndianness, uint32(len(idx))))
	for _, r := range idx {
		require.NoError(t, binary.Write(want, indexEndianness, struct {
			Sum                  [proto.HashSize]byte
			Offset, Length, Type uint32
			CarriedTime          int64
			CarriedOffset        uint32
		}{r.Sum, r.Offset, r.Length, r.Type, r.CarriedTime, r.CarriedOffset}))
	}

	got := new(bytes.Buffer)
	_, err := idx.WriteTo(got)
	require.NoError(t, err)
	require.Equal(t, want.Bytes(), got.Bytes())
}

func TestAVersionedIndexFileKeepsCarriedVersions(t *testing.T) {
	idx := makeIndex()[:3]
	idx[1] = idx[1].Carry(Version{Time: time.Unix(1_790_000_000, 1000), Offset: 9})

	buf := new(bytes.Buffer)
	_, err := idx.WriteTo(buf)
	require.NoError(t, err)

	var read IndexFile
	_, err = read.ReadFrom(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.Equal(t, idx, read)
}

// versionsOf returns the version each object's committed copy has now.
func versionsOf(t *testing.T, store *PackStorage, objects []*proto.Object) map[string]Version {
	t.Helper()

	versions := make(map[string]Version)
	for _, obj := range objects {
		loc, err := store.index.LocateObject(obj.Ref(), Scope{})
		require.NoError(t, err)

		info, known, err := store.index.LookupArchive(loc.Archive)
		require.NoError(t, err)
		require.True(t, known)
		require.False(t, info.Created.IsZero(), "archive %s has no creation time", loc.Archive)

		versions[string(obj.Ref().Hash)] = loc.Record.Version(info.Created)
	}

	return versions
}

func TestARewriteKeepsTheVersionsOfWhatItMoves(t *testing.T) {
	store := newTestStore(t, t.TempDir(), WithMaxSize(64*1024*1024), WithCompaction(CompactionConfig{MinimumCandidates: 0, Workers: 1}))
	t.Cleanup(func() { _ = store.Close() })

	objects := makeTestData(t, 40)
	for i, object := range objects {
		require.NoError(t, store.Put(context.Background(), object))
		if i%10 == 9 {
			require.NoError(t, store.Flush())
		}
	}

	before := versionsOf(t, store, objects)

	moved, _, err := store.archiveNames()
	require.NoError(t, err)

	compact(t, context.Background(), store)

	after, _, err := store.archiveNames()
	require.NoError(t, err)
	require.Less(t, len(after), len(moved), "the compaction rewrote nothing")

	require.Equal(t, before, versionsOf(t, store, objects))
}

func TestAnArchiveTheIndexKnowsWithoutACreationTimeGetsOne(t *testing.T) {
	base := t.TempDir()

	first := newTestStore(t, base)
	obj := makeTestData(t, 1)[0]
	require.NoError(t, first.Put(context.Background(), obj))
	require.NoError(t, first.Flush())

	loc, err := first.index.LocateObject(obj.Ref(), Scope{})
	require.NoError(t, err)
	require.NoError(t, first.Close())

	file, err := os.Stat(filepath.Join(base, filepath.FromSlash(loc.Archive)+IndexExt))
	require.NoError(t, err)

	stale := NewInMemoryIndex()
	require.NoError(t, stale.IndexArchive(ArchiveInfo{Name: loc.Archive}, IndexFile{loc.Record}))

	second := newTestStore(t, base, WithArchiveIndex(stale))
	t.Cleanup(func() { _ = second.Close() })

	info, _, err := stale.LookupArchive(loc.Archive)
	require.NoError(t, err)
	require.True(t, file.ModTime().Truncate(time.Microsecond).Equal(info.Created), "created %s, index file %s", info.Created, file.ModTime())
}

// failingReads fails the first read of each named file, as a bucket's
// transient error would.
type failingReads struct {
	*localArchiveStorage

	mtx  sync.Mutex
	fail map[string]bool
}

var errTransient = errors.New("transient read error")

func (f *failingReads) Open(name string) (File, error) {
	file, err := f.localArchiveStorage.Open(name)
	if err != nil {
		return nil, err
	}

	return &failingFile{File: file, storage: f, name: name}, nil
}

type failingFile struct {
	File
	storage *failingReads
	name    string
}

func (f *failingFile) Read(p []byte) (int, error) {
	f.storage.mtx.Lock()
	fail := f.storage.fail[f.name]
	delete(f.storage.fail, f.name)
	f.storage.mtx.Unlock()

	if fail {
		return 0, errTransient
	}

	return f.File.Read(p)
}

// deletedInOneArchive stores an object in one archive and its tombstone in
// a later one, and returns the object and the first archive's name.
func deletedInOneArchive(t *testing.T, base string) (*proto.Object, string) {
	t.Helper()

	store := newGCStore(t, base)
	blob := makeTestData(t, 1)[0]
	putAll(t, store, []*proto.Object{blob})
	require.Len(t, store.archives, 1)
	name := store.archives[0].name

	require.NoError(t, store.Delete(context.Background(), blob.Ref()))
	require.NoError(t, store.Flush())
	requirePresent(t, store, []*proto.Object{blob}, false)
	require.NoError(t, store.Close())

	return blob, name
}

func TestAnIndexFileThatFailsToReadIsNotReplaced(t *testing.T) {
	base := t.TempDir()
	blob, name := deletedInOneArchive(t, base)

	idxPath := filepath.Join(base, filepath.FromSlash(name)+IndexExt)
	before, err := os.ReadFile(idxPath)
	require.NoError(t, err)
	stat, err := os.Stat(idxPath)
	require.NoError(t, err)

	storage := &failingReads{localArchiveStorage: newLocal(base), fail: map[string]bool{name + IndexExt: true}}
	failing, err := NewPackStorage(WithArchiveStorage(storage), WithArchiveIndex(NewInMemoryIndex()))
	require.NoError(t, err)
	require.ErrorIs(t, failing.Open(), errTransient)
	require.NoError(t, failing.Close())

	after, err := os.ReadFile(idxPath)
	require.NoError(t, err)
	require.Equal(t, before, after)
	restat, err := os.Stat(idxPath)
	require.NoError(t, err)
	require.Equal(t, stat.ModTime(), restat.ModTime(), "the index file keeps the time that versions its records")

	rebuilt := newGCStore(t, base)
	t.Cleanup(func() { _ = rebuilt.Close() })
	requirePresent(t, rebuilt, []*proto.Object{blob}, false)
}

func TestAMissingIndexFileIsRecoveredOnlyInMemory(t *testing.T) {
	base := t.TempDir()
	blob, name := deletedInOneArchive(t, base)

	idxPath := filepath.Join(base, filepath.FromSlash(name)+IndexExt)
	require.NoError(t, os.Remove(idxPath))

	for range 2 {
		rebuilt := newGCStore(t, base)
		requirePresent(t, rebuilt, []*proto.Object{blob}, false)
		require.NoError(t, rebuilt.Close())

		_, err := os.Stat(idxPath)
		require.True(t, os.IsNotExist(err), "no index file is written after the fact")
	}
}
