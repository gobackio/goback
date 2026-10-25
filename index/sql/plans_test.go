package sql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// plan is how SQLite answers query on x's database.
func plan(t *testing.T, x *Index, query string, args ...any) string {
	t.Helper()

	rows, err := x.db.Query("EXPLAIN QUERY PLAN "+query, args...)
	require.NoError(t, err)
	defer rows.Close()

	var steps []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		steps = append(steps, detail)
	}
	require.NoError(t, rows.Err())

	return strings.Join(steps, "\n")
}

func TestTheVersionsOfFileObjectsAreFoundThroughAnIndex(t *testing.T) {
	x := openMemory(t, newMemStore())

	require.Contains(t, plan(t, x, "SELECT * FROM files WHERE ref IN (?, ?)", []byte("a"), []byte("b")), "USING INDEX files_by_ref")
}
