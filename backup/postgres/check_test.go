package postgres

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAPasswordFileOthersCanAccessIsAProblemForPgBasebackup(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("libpq checks the password file's mode only on Unix")
	}

	path := filepath.Join(t.TempDir(), "pgpass")
	require.NoError(t, os.WriteFile(path, []byte("*:*:*:goback:secret\n"), 0o644))
	t.Setenv("PGPASSFILE", path)
	t.Setenv("PGPASSWORD", "")

	require.NotEmpty(t, checkPassFile())

	require.NoError(t, os.Chmod(path, 0o600))
	require.Empty(t, checkPassFile())

	require.NoError(t, os.Chmod(path, 0o644))
	t.Setenv("PGPASSWORD", "secret")
	require.Empty(t, checkPassFile(), "libpq takes PGPASSWORD before the file")
}

func TestPgBasebackupsVersionIsComparedByMajorVersion(t *testing.T) {
	require.Equal(t, "14", basebackupMajor("pg_basebackup (PostgreSQL) 14.11 (Debian 14.11-1.pgdg120+2)\n"))
	require.Equal(t, "9.6", basebackupMajor("pg_basebackup (PostgreSQL) 9.6.24\n"))
	require.Equal(t, "", basebackupMajor("command not found"))

	require.Equal(t, "14", majorString(140011))
	require.Equal(t, "9.6", majorString(90624))
}
