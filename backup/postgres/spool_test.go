package postgres

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func segment(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "segment")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	return path
}

func TestTheSpoolHoldsOnlyWholeFiles(t *testing.T) {
	spool := Spool{Dir: t.TempDir()}

	require.NoError(t, spool.Add(segment(t, "first"), "000000010000000000000002"))
	require.NoError(t, spool.Add(segment(t, "zeroth"), "000000010000000000000001"))

	// a copy a crash cut short
	require.NoError(t, os.WriteFile(filepath.Join(spool.Dir, "000000010000000000000003"+spoolingSuffix), []byte("fir"), 0o600))

	files, err := spool.Files()
	require.NoError(t, err)
	require.Equal(t, []string{"000000010000000000000001", "000000010000000000000002"}, files)

	require.NoError(t, spool.Remove("000000010000000000000001"))

	files, err = spool.Files()
	require.NoError(t, err)
	require.Equal(t, []string{"000000010000000000000002"}, files)
}

func TestArchivingAgainSucceedsOnlyWithTheSameContents(t *testing.T) {
	spool := Spool{Dir: t.TempDir()}
	name := "000000010000000000000002"

	require.NoError(t, spool.Add(segment(t, "first"), name))
	require.NoError(t, spool.Add(segment(t, "first"), name))
	require.ErrorIs(t, spool.Add(segment(t, "other"), name), ErrConflict)

	held, err := os.ReadFile(filepath.Join(spool.Dir, name))
	require.NoError(t, err)
	require.Equal(t, "first", string(held))
}

func TestTheSpoolRefusesNamesOutsideIt(t *testing.T) {
	spool := Spool{Dir: t.TempDir()}

	for _, name := range []string{"", "../x", "a/b", ".hidden", "x" + spoolingSuffix} {
		require.Error(t, spool.Add(segment(t, "x"), name), name)
	}
}
