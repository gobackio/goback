package backup

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// withZeroes is random data with runs of zeroes inside it and at its end.
func withZeroes(size int) []byte {
	data := randomData(size, 7)
	clear(data[size/10 : size*7/10])
	clear(data[size-size/5:])

	return data
}

func TestZeroPartsReadBackAsZeroes(t *testing.T) {
	store := newMemStore()
	data := withZeroes(8*maxBlobSize + 11)
	ref := putFile(t, store, nil, data)

	dir := t.TempDir()
	fresh, patched := filepath.Join(dir, "fresh.dat"), filepath.Join(dir, "patched.dat")

	// a partial of other content, as a failed restore of another version
	// leaves it, must have its zeroes written
	require.NoError(t, os.WriteFile(PartialPath(patched), bytes.Repeat([]byte{0xff}, len(data)), 0o600))

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
	require.NoError(t, os.WriteFile(PartialPath(patched), bytes.Repeat([]byte{0xff}, len(data)), 0o600))

	r := &Restorer{Store: store}
	err := r.RestoreFiles(context.Background(), []FileToRestore{
		{Path: fresh, Stat: statFor(data), Ref: ref},
		{Path: patched, Stat: statFor(data), Ref: ref},
	}, func(_ int, _ Outcome, err error) error { return err })
	require.NoError(t, err)

	requireRestored(t, fresh, data, statFor(data))
	requireRestored(t, patched, data, statFor(data))
}
