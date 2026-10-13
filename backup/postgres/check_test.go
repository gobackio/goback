package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPgBasebackupsVersionIsComparedByMajorVersion(t *testing.T) {
	require.Equal(t, "14", basebackupMajor("pg_basebackup (PostgreSQL) 14.11 (Debian 14.11-1.pgdg120+2)\n"))
	require.Equal(t, "9.6", basebackupMajor("pg_basebackup (PostgreSQL) 9.6.24\n"))
	require.Equal(t, "", basebackupMajor("command not found"))

	require.Equal(t, "14", majorString(140011))
	require.Equal(t, "9.6", majorString(90624))
}
