package sql

import (
	"strings"
	"testing"
	"time"

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

func TestADirectoryAtAnyTimeIsListedThroughAnIndex(t *testing.T) {
	x := openMemory(t, newMemStore())
	at := time.Date(2026, 9, 16, 8, 0, 0, 0, time.UTC)

	for table, index := range map[string]string{"files": "files_by_dir", "trees": "trees_by_dir"} {
		query := "SELECT * FROM " + table + " WHERE set_id = ? AND dir = ? AND valid_from <= ? AND (valid_until IS NULL OR valid_until > ?)"
		require.Contains(t, plan(t, x, query, 1, "d", at, at), "USING INDEX "+index, table)

		open := "SELECT * FROM " + table + " WHERE set_id = ? AND dir IN (?, ?) AND valid_until IS NULL"
		require.Regexp(t, `USING INDEX \w+_(open|by_dir \(set_id=\? AND dir=\? AND valid_until=\?\))`, plan(t, x, open, 1, "d", "e"),
			"%s: the open rows of a directory are found without its closed ones", table)
	}
}

func TestTheVersionsOfFileObjectsAreFoundThroughAnIndex(t *testing.T) {
	x := openMemory(t, newMemStore())

	require.Contains(t, plan(t, x, "SELECT * FROM files WHERE ref IN (?, ?)", []byte("a"), []byte("b")), "USING INDEX files_by_ref")
}
