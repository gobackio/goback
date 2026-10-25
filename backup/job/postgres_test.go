package job

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAConnectionStringsPasswordIsSplitOut(t *testing.T) {
	for _, c := range []struct{ in, rest, password string }{
		{"host=db user=backup password=hunter2", "host='db' user='backup'", "hunter2"},
		{`host=db password='it\'s a secret' port=5433`, "host='db' port='5433'", "it's a secret"},
		{"host = db  password = p\\ w", "host='db'", "p w"},
		{"postgres://backup:hunter2@db:5432/postgres?sslmode=disable", "postgres://backup@db:5432/postgres?sslmode=disable", "hunter2"},
		{"postgresql://db/postgres?password=hunter2&user=backup", "postgresql://db/postgres?user=backup", "hunter2"},
		{"host=db user=backup", "host=db user=backup", ""},
		{"postgres://backup@db/postgres", "postgres://backup@db/postgres", ""},
		{"host='db password=x", "host='db password=x", ""},
		{"", "", ""},
	} {
		rest, password := withoutPassword(c.in)
		require.Equal(t, c.rest, rest, c.in)
		require.Equal(t, c.password, password, c.in)
	}
}
