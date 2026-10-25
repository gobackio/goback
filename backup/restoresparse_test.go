package backup

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// withZeroes is random data with runs of zeroes inside it and at its end.
func withZeroes(size int) []byte {
	data := randomData(size, 7)
	clear(data[size/10 : size*7/10])
	clear(data[size-size/5:])

	return data
}

// putSparse stores a file of data, then hole zero bytes the file left
// unallocated, then data again, as a backup of a sparse file does, and
// returns its ref and content.
func putSparse(t *testing.T, store ObjectStore, key *storekey.Key, data []byte, hole int64) (*proto.Ref, []byte) {
	t.Helper()

	writer := newFileWriter(context.Background(), store, key, 2*int64(len(data))+hole)
	_, err := writer.Write(data)
	require.NoError(t, err)
	writer.hole(hole)
	_, err = writer.Write(data)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	content := append(append(bytes.Clone(data), make([]byte, hole)...), data...)

	return writer.Ref(), content
}

func TestASparseFileIsRestoredFreshOverAPartialAndOverAnotherVersion(t *testing.T) {
	for name, key := range keyCases(t) {
		t.Run(name, func(t *testing.T) {
			store := newMemStore()
			ref, data := putSparse(t, store, key, randomData(maxBlobSize+3, 1), 3*maxBlobSize)
			stat := statFor(data)

			dir := t.TempDir()
			fresh, patched, replaced := filepath.Join(dir, "fresh"), filepath.Join(dir, "patched"), filepath.Join(dir, "replaced")
			writePartial(t, patched, bytes.Repeat([]byte{0xff}, len(data)))
			require.NoError(t, os.WriteFile(replaced, bytes.Repeat([]byte{0xee}, len(data)), 0o600))

			r := &Restorer{Store: store, Key: key, Verify: true}
			for _, path := range []string{fresh, patched, replaced} {
				_, err := r.RestoreFile(context.Background(), path, stat, ref)
				require.NoError(t, err)
				requireRestored(t, path, data, stat)
			}

			outcome, err := r.RestoreFile(context.Background(), fresh, stat, ref)
			require.NoError(t, err)
			require.Equal(t, OutcomeUnchanged, outcome)

			got, err := r.ReadFile(context.Background(), stat, ref)
			require.NoError(t, err)
			require.True(t, bytes.Equal(data, got))
		})
	}
}

func TestASparseFileIsRestoredFrontToBackAndInAGroup(t *testing.T) {
	store := filesStore{newMemStore()}
	small, smallData := putSparse(t, store, nil, randomData(1000, 2), sparseMin)
	large, largeData := putSparse(t, store, nil, randomData(maxBlobSize, 3), smallFile)

	dir := t.TempDir()
	var written atomic.Int64

	r := &Restorer{Store: store}
	err := r.RestoreFiles(context.Background(), []FileToRestore{
		{Path: filepath.Join(dir, "small"), Stat: statFor(smallData), Ref: small, Written: func(n int64) { written.Add(n) }},
	}, func(_ int, _ Outcome, err error) error { return err })
	require.NoError(t, err)
	requireRestored(t, filepath.Join(dir, "small"), smallData, statFor(smallData))
	require.EqualValues(t, len(smallData), written.Load())

	path := filepath.Join(dir, "large")
	_, err = (&Restorer{Store: store, FS: frontToBack{LocalFS{}}}).RestoreFile(context.Background(), path, statFor(largeData), large)
	require.NoError(t, err)
	requireRestored(t, path, largeData, statFor(largeData))
}

func TestZeroPartsReadBackAsZeroes(t *testing.T) {
	store := newMemStore()
	data := withZeroes(8*maxBlobSize + 11)
	ref := putFile(t, store, nil, data)

	dir := t.TempDir()
	fresh, patched := filepath.Join(dir, "fresh.dat"), filepath.Join(dir, "patched.dat")

	// a partial of other content, as a failed restore of another version
	// leaves it, must have its zeroes written
	writePartial(t, patched, bytes.Repeat([]byte{0xff}, len(data)))

	r := &Restorer{Store: store, Verify: true}
	for _, path := range []string{fresh, patched} {
		_, err := r.RestoreFile(context.Background(), path, statFor(data), ref)
		require.NoError(t, err)
		requireRestored(t, path, data, statFor(data))
	}
}

func TestZeroPartsOfSmallFilesReadBackAsZeroes(t *testing.T) {
	store := filesStore{newMemStore()}
	data := withZeroes(smallFile - 1)
	ref := putFile(t, store, nil, data)

	dir := t.TempDir()
	fresh, patched := filepath.Join(dir, "fresh.dat"), filepath.Join(dir, "patched.dat")
	writePartial(t, patched, bytes.Repeat([]byte{0xff}, len(data)))

	r := &Restorer{Store: store}
	err := r.RestoreFiles(context.Background(), []FileToRestore{
		{Path: fresh, Stat: statFor(data), Ref: ref},
		{Path: patched, Stat: statFor(data), Ref: ref},
	}, func(_ int, _ Outcome, err error) error { return err })
	require.NoError(t, err)

	requireRestored(t, fresh, data, statFor(data))
	requireRestored(t, patched, data, statFor(data))
}
