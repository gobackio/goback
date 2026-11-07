package pack

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T, base string, opts ...PackOption) *PackStorage {
	t.Helper()

	options := append([]PackOption{
		WithArchiveStorage(newLocal(base)),
		WithArchiveIndex(NewInMemoryIndex()),
		WithMaxSize(1024 * 1024),
	}, opts...)

	store, err := NewPackStorage(options...)
	require.NoError(t, err)
	require.NoError(t, store.Open())

	return store
}

func archiveFiles(t *testing.T, base string) []string {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join(base, "*"+ArchiveSuffix))
	require.NoError(t, err)
	require.NotEmpty(t, matches)

	return matches
}

func TestGetDetectsCorruption(t *testing.T) {
	base := t.TempDir()
	store := newTestStore(t, base)

	objects := makeTestData(t, 20)
	for _, obj := range objects {
		require.NoError(t, store.Put(context.Background(), obj))
	}
	require.NoError(t, store.Close())

	// flip one byte in the middle of every archive's payload region
	for _, name := range archiveFiles(t, base) {
		data, err := os.ReadFile(name)
		require.NoError(t, err)

		data[archiveHeaderSize+len(data)/2] ^= 0xff
		require.NoError(t, os.WriteFile(name, data, 0644))
	}

	store = newTestStore(t, base)
	defer store.Close()

	var failed int
	for _, obj := range objects {
		got, err := store.Get(context.Background(), obj.Ref())
		if err != nil {
			require.ErrorIs(t, err, proto.ErrRefMismatch)
			failed++
			continue
		}

		require.Equal(t, obj.Ref().Hash, got.Ref().Hash)
	}

	require.Greater(t, failed, 0, "no read noticed the flipped byte")
}

func TestOpenRefusesForeignArchive(t *testing.T) {
	base := t.TempDir()
	store := newTestStore(t, base)
	require.NoError(t, store.Put(context.Background(), makeTestData(t, 1)[0]))
	require.NoError(t, store.Close())

	for _, name := range archiveFiles(t, base) {
		data, err := os.ReadFile(name)
		require.NoError(t, err)

		// an archive from before the format had a header starts with an
		// object length varint, never with the magic
		require.NoError(t, os.WriteFile(name, data[archiveHeaderSize:], 0644))
		require.NoError(t, os.Remove(name[:len(name)-len(ArchiveSuffix)]+IndexExt))
	}

	store, err := NewPackStorage(
		WithArchiveStorage(newLocal(base)),
		WithArchiveIndex(NewInMemoryIndex()),
	)
	require.NoError(t, err)
	require.ErrorContains(t, store.Open(), "not a goback archive")
}

func TestCompactionRefusesCorruptSource(t *testing.T) {
	base := t.TempDir()
	store := newTestStore(t, base, WithMaxSize(16*1024), WithCompaction(CompactionConfig{MinimumCandidates: 1}))

	objects := makeTestData(t, 40)
	for _, obj := range objects {
		require.NoError(t, store.Put(context.Background(), obj))
	}
	require.NoError(t, store.Close())

	name := archiveFiles(t, base)[0]

	stored, err := os.Open(strings.TrimSuffix(name, ArchiveSuffix) + IndexExt)
	require.NoError(t, err)
	var index IndexFile
	_, err = index.ReadFrom(stored)
	require.NoError(t, stored.Close())
	require.NoError(t, err)
	require.NotEmpty(t, index)

	// the last byte of a record is its payload's, which its ref covers
	data, err := os.ReadFile(name)
	require.NoError(t, err)
	data[index[0].Offset+index[0].Length-1] ^= 0xff
	require.NoError(t, os.WriteFile(name, data, 0644))

	// reopen with a larger archive limit so every small archive is a candidate
	store = newTestStore(t, base, WithCompaction(CompactionConfig{MinimumCandidates: 0}))
	defer store.Close()

	_, err = store.Compact(context.Background())
	require.ErrorIs(t, err, proto.ErrRefMismatch)
}

func TestScrubDetectsBrokenChain(t *testing.T) {
	base := t.TempDir()
	store := newTestStore(t, base)
	ctx := context.Background()

	objects := makeTestData(t, 3)
	require.NoError(t, store.Put(ctx, objects[0]))

	// the second object gets written claiming an object that is not in the
	// archive as its predecessor
	store.mtx.RLock()
	writing := store.archives[0]
	store.mtx.RUnlock()
	writing.mtx.Lock()
	writing.last = objects[2].Ref()
	writing.mtx.Unlock()

	require.NoError(t, store.Put(ctx, objects[1]))
	require.NoError(t, store.Close())

	store = newTestStore(t, base)
	defer store.Close()

	report, err := store.Scrub(ctx)
	require.NoError(t, err)
	require.Len(t, report.Corrupt, 1)
	require.True(t, report.Corrupt[0].Ref.Equal(objects[1].Ref()))
	require.ErrorIs(t, report.Corrupt[0].Err, ErrBrokenChain)

	for _, obj := range objects[:2] {
		got, err := store.Get(ctx, obj.Ref())
		require.NoError(t, err, "a broken chain is a finding, not a read failure")
		require.Equal(t, obj.GetBlob().Data, got.GetBlob().Data)
	}
}

func TestTombstoneRef(t *testing.T) {
	base := t.TempDir()
	store := newTestStore(t, base)
	defer store.Close()

	obj := makeTestData(t, 1)[0]
	require.NoError(t, store.Put(context.Background(), obj))
	require.NoError(t, store.Delete(context.Background(), obj.Ref()))
	require.NoError(t, store.Flush())

	// the tombstone is addressable under its own typed ref and is not an object
	_, err := store.Get(context.Background(), proto.TombstoneRef(obj.Ref()))
	require.ErrorContains(t, err, "tombstone")
}
