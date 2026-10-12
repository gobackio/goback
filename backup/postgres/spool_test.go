package postgres

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTheSpoolListsOnlyWholeFiles(t *testing.T) {
	spool := Spool{Dir: t.TempDir()}

	require.NoError(t, spool.put("000000010000000000000002", []byte("first")))
	require.NoError(t, spool.put("000000010000000000000001", []byte("zeroth")))
	require.NoError(t, os.WriteFile(filepath.Join(spool.receiving(), "000000010000000000000003"), []byte("fir"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(spool.Dir, ".lock"), nil, 0o600))

	files, err := spool.Files()
	require.NoError(t, err)
	require.Equal(t, []string{"000000010000000000000001", "000000010000000000000002"}, files)

	require.NoError(t, spool.Remove("000000010000000000000001"))

	files, err = spool.Files()
	require.NoError(t, err)
	require.Equal(t, []string{"000000010000000000000002"}, files)
}

func TestReceivingAFileAgainSucceedsOnlyWithTheSameContents(t *testing.T) {
	spool := Spool{Dir: t.TempDir()}
	name := "00000002.history"

	require.NoError(t, spool.put(name, []byte("first")))
	require.NoError(t, spool.put(name, []byte("first")))
	require.ErrorIs(t, spool.put(name, []byte("other")), ErrConflict)

	held, err := os.ReadFile(filepath.Join(spool.Dir, name))
	require.NoError(t, err)
	require.Equal(t, "first", string(held))
}

func TestTheSpoolRefusesNamesOutsideIt(t *testing.T) {
	spool := Spool{Dir: t.TempDir()}

	for _, name := range []string{"", "../x", "a/b", ".hidden"} {
		require.Error(t, spool.put(name, []byte("x")), name)
	}
}
