//go:build windows

package backup

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestListedInfoIsWhatLstatSays(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "file"), []byte("content"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "readonly"), []byte("x"), 0o444))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "dir"), 0o755))

	entries, err := localReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 3)

	for _, entry := range entries {
		info, err := entry.Info()
		require.NoError(t, err)

		want, err := os.Lstat(filepath.Join(dir, entry.Name()))
		require.NoError(t, err)

		require.Equal(t, want.Name(), info.Name())
		require.Equal(t, want.Mode(), info.Mode(), entry.Name())
		require.Equal(t, want.Size(), info.Size(), entry.Name())
		require.Equal(t, want.ModTime(), info.ModTime(), entry.Name())
		require.Equal(t, want.IsDir(), info.IsDir(), entry.Name())

		_, id := FileIdentity(info)
		require.NotZero(t, id, entry.Name())
	}
}

func TestIdentityChangesWhenARewriteKeepsMtimeAndSize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "file")
	require.NoError(t, os.WriteFile(path, []byte("version-1"), 0o644))

	before := listed(t, dir, "file")

	time.Sleep(20 * time.Millisecond)
	require.NoError(t, os.WriteFile(path, []byte("version-2"), 0o644))
	require.NoError(t, os.Chtimes(path, before.ModTime(), before.ModTime()))

	after := listed(t, dir, "file")
	require.Equal(t, before.ModTime(), after.ModTime())
	require.Equal(t, before.Size(), after.Size())

	ctimeBefore, idBefore := FileIdentity(before)
	ctimeAfter, idAfter := FileIdentity(after)
	require.Equal(t, idBefore, idAfter)
	require.Greater(t, ctimeAfter, ctimeBefore)
}

func listed(t *testing.T, dir, name string) os.FileInfo {
	t.Helper()

	entries, err := localReadDir(dir)
	require.NoError(t, err)

	for _, entry := range entries {
		if entry.Name() == name {
			info, err := entry.Info()
			require.NoError(t, err)

			return info
		}
	}

	t.Fatalf("%s is not listed", name)

	return nil
}
