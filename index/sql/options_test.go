package sql

import (
	"context"
	"database/sql"
	"testing"

	"entgo.io/ent/dialect"
	"github.com/stretchr/testify/require"
)

// TestWithDB opens an index over a database the test connected and
// leaves that database open after Close.
func TestWithDB(t *testing.T) {
	db, err := sql.Open("sqlite", "file:withdb?mode=memory&cache=shared&_pragma=foreign_keys(1)")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	x := New("", newMemStore(), WithDB(db, dialect.SQLite))
	require.NoError(t, x.Open())

	ctx := context.Background()
	_, err = x.setNamed(ctx, x.client, "world", 0)
	require.NoError(t, err)
	sets, err := x.ListSets(ctx)
	require.NoError(t, err)
	require.Len(t, sets, 1)

	require.NoError(t, x.Close())
	require.NoError(t, db.Ping(), "the caller's database outlives the index")

	var n int
	require.NoError(t, db.QueryRow("SELECT count(*) FROM sets").Scan(&n))
	require.Equal(t, 1, n)
}
