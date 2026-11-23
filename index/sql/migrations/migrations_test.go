package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobackio/goback/testing/testpg"

	"ariga.io/atlas/sql/migrate"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

var databases atomic.Int64

func memory(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", fmt.Sprintf("file:migrations-%d?mode=memory&cache=shared&_pragma=foreign_keys(1)", databases.Add(1)))
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	return db
}

func TestApplyRecordsRevisions(t *testing.T) {
	ctx := context.Background()
	db := memory(t)

	require.NoError(t, Apply(ctx, db, SQLite))

	dir, err := Dir(SQLite)
	require.NoError(t, err)
	files, err := dir.Files()
	require.NoError(t, err)
	require.NotEmpty(t, files)

	rrw := &revisions{db: db}
	require.Equal(t, "", rrw.Ident().Schema)
	revs, err := rrw.ReadRevisions(ctx)
	require.NoError(t, err)
	require.Len(t, revs, len(files))
	for i, rev := range revs {
		require.Equal(t, files[i].Version(), rev.Version)
		require.Equal(t, rev.Total, rev.Applied, "every statement of %s applied", rev.Version)
		require.Empty(t, rev.Error)
		require.Equal(t, "goback", rev.OperatorVersion)
		require.False(t, rev.ExecutedAt.IsZero())
	}

	last, err := rrw.ReadRevision(ctx, revs[len(revs)-1].Version)
	require.NoError(t, err)
	require.Equal(t, revs[len(revs)-1].Hash, last.Hash)

	_, err = rrw.ReadRevision(ctx, "nope")
	require.ErrorIs(t, err, migrate.ErrRevisionNotExist)

	require.NoError(t, Apply(ctx, db, SQLite), "an up-to-date database is left alone")

	// the tables are there
	var n int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM sets").Scan(&n))
	require.Zero(t, n)
}

// upTo is the dialect's directory up to and including version.
func upTo(t *testing.T, dialect, version string) *migrate.MemDir {
	t.Helper()

	full, err := Dir(dialect)
	require.NoError(t, err)
	files, err := full.Files()
	require.NoError(t, err)

	dir := &migrate.MemDir{}
	for _, f := range files {
		if f.Version() > version {
			break
		}

		require.NoError(t, dir.WriteFile(f.Name(), f.Bytes()))
	}

	sum, err := dir.Checksum()
	require.NoError(t, err)
	require.NoError(t, migrate.WriteSumFile(dir, sum))

	return dir
}

// TestApplyRebuildsTablesThatRowsReference migrates a database holding a
// set and rows that reference it from the first schema, through the
// migrations that rebuild the sets table.
func TestApplyRebuildsTablesThatRowsReference(t *testing.T) {
	ctx := context.Background()
	db := memory(t)

	require.NoError(t, applyDir(ctx, db, SQLite, upTo(t, SQLite, "20260917165456"), revisionsTable, "", false))

	for _, stmt := range []string{
		"INSERT INTO sets (name) VALUES ('s')",
		"INSERT INTO commits (ref, timestamp, received_at, tree, set_id) VALUES (zeroblob(32), '2026-09-16 08:00:00+00:00', '2026-09-16 08:00:00+00:00', zeroblob(32), 1)",
		"INSERT INTO files (path, dir, valid_from, ref, mtime_ns, mode, user, `group`, size, set_id) VALUES ('a', '', '2026-09-16 08:00:00+00:00', zeroblob(32), 0, 0, '', '', 0, 1)",
		"INSERT INTO trees (path, dir, valid_from, ref, set_id) VALUES ('d', '', '2026-09-16 08:00:00+00:00', zeroblob(32), 1)",
		"INSERT INTO set_refs (ref, set_id) VALUES (zeroblob(32), 1)",
	} {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	require.NoError(t, Apply(ctx, db, SQLite))

	var n int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM commits WHERE set_id = (SELECT id FROM sets WHERE name = 's')").Scan(&n))
	require.Equal(t, 1, n)

	var keys bool
	require.NoError(t, db.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&keys))
	require.True(t, keys, "the connection gets its foreign keys back")
}

// dirOf is a migration directory of the files, by name.
func dirOf(t *testing.T, files map[string]string) *migrate.MemDir {
	t.Helper()

	dir := &migrate.MemDir{}
	for name, data := range files {
		require.NoError(t, dir.WriteFile(name, []byte(data)))
	}

	sum, err := dir.Checksum()
	require.NoError(t, err)
	require.NoError(t, migrate.WriteSumFile(dir, sum))

	return dir
}

// inSchema opens the Postgres database at dsn on a schema of its own.
func inSchema(t *testing.T, dsn, name string) *sql.DB {
	t.Helper()

	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	_, err = db.Exec("CREATE SCHEMA " + name)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	db, err = sql.Open("pgx", dsn+"&search_path="+name)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	return db
}

func TestPostgresMigrationsOfTwoSchemasRunAtOnce(t *testing.T) {
	dsn := testpg.Start(t)
	ctx := context.Background()

	slow, fast := inSchema(t, dsn, "slow"), inSchema(t, dsn, "fast")

	started := make(chan error, 1)
	go func() {
		started <- ApplyDir(ctx, slow, Postgres, dirOf(t, map[string]string{"1_slow.sql": "SELECT pg_sleep(3);\n"}), "revisions", "lock")
	}()

	require.Eventually(t, func() bool {
		var held int
		require.NoError(t, fast.QueryRowContext(ctx, "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'").Scan(&held))
		return held > 0
	}, 10*time.Second, 10*time.Millisecond)

	began := time.Now()
	require.NoError(t, ApplyDir(ctx, fast, Postgres, dirOf(t, map[string]string{"1_fast.sql": "CREATE TABLE t (id int);\n"}), "revisions", "lock"))
	require.Less(t, time.Since(began), 2*time.Second, "another schema's migration does not wait for this one")

	require.NoError(t, <-started)
}

// TestPostgresMigrationResumesAnInterruptedIndexBuild fails a migration
// in a CREATE INDEX CONCURRENTLY, which leaves the index behind invalid,
// and expects the retry to build it.
func TestPostgresMigrationResumesAnInterruptedIndexBuild(t *testing.T) {
	ctx := context.Background()
	db := inSchema(t, testpg.Start(t), "store")

	dir := dirOf(t, map[string]string{
		"1_t.sql":   "CREATE TABLE t (id int);\nINSERT INTO t VALUES (1), (1);\n",
		"2_idx.sql": `CREATE UNIQUE INDEX CONCURRENTLY "t_id" ON t (id);` + "\n",
	})

	require.Error(t, ApplyDir(ctx, db, Postgres, dir, "revisions", "lock"))

	var valid bool
	require.NoError(t, db.QueryRowContext(ctx, "SELECT indisvalid FROM pg_index WHERE indexrelid = 't_id'::regclass").Scan(&valid))
	require.False(t, valid)

	_, err := db.ExecContext(ctx, "DELETE FROM t; INSERT INTO t VALUES (1)")
	require.NoError(t, err)

	require.NoError(t, ApplyDir(ctx, db, Postgres, dir, "revisions", "lock"))
	require.NoError(t, db.QueryRowContext(ctx, "SELECT indisvalid FROM pg_index WHERE indexrelid = 't_id'::regclass").Scan(&valid))
	require.True(t, valid)
}

func TestApplyRefusesADirtyDatabase(t *testing.T) {
	ctx := context.Background()
	db := memory(t)

	_, err := db.ExecContext(ctx, "CREATE TABLE stray (id INTEGER PRIMARY KEY)")
	require.NoError(t, err)

	var clean *migrate.NotCleanError
	require.ErrorAs(t, Apply(ctx, db, SQLite), &clean)
}

// TestDirectoryMatchesSchema replays the SQLite migrations on an empty
// database and expects no difference to the ent schema.
func TestDirectoryMatchesSchema(t *testing.T) {
	dir, err := Dir(SQLite)
	require.NoError(t, err)

	err = Diff(context.Background(), memory(t), SQLite, dir, "drift")
	require.ErrorIs(t, err, migrate.ErrNoPlan, "regenerate the migrations: cd index/sql/migrations && go run ./gen -name <change>")
}

func TestDirRejectsUnknownDialects(t *testing.T) {
	_, err := Dir("oracle")
	require.Error(t, err)

	require.Error(t, Apply(context.Background(), memory(t), "oracle"))
}

// TestArchiveKeysKeepEveryObjectWithItsArchive migrates objects of two
// archives, one ref held by both, to archives named by a numeric key.
func TestArchiveKeysKeepEveryObjectWithItsArchive(t *testing.T) {
	for _, tc := range []struct {
		dialect, before string
		db              func(t *testing.T) *sql.DB
	}{
		{SQLite, "20261027133407", memory},
		{Postgres, "20261027133411", func(t *testing.T) *sql.DB { return inSchema(t, testpg.Start(t), "store") }},
	} {
		t.Run(tc.dialect, func(t *testing.T) {
			ctx := context.Background()
			db := tc.db(t)

			require.NoError(t, applyDir(ctx, db, tc.dialect, upTo(t, tc.dialect, tc.before), revisionsTable, "lock", false))

			for _, stmt := range []string{
				"INSERT INTO archives (id, state) VALUES ('archive-b', 2), ('archive-a', 2)",
				"INSERT INTO objects (ref, start, length, type, archive_id) VALUES (" + ref(tc.dialect, 1) + ", 0, 10, 2, 'archive-a')",
				"INSERT INTO objects (ref, start, length, type, archive_id, carried_time, carried_offset) VALUES (" + ref(tc.dialect, 2) + ", 10, 20, 3, 'archive-b', 7, 4)",
				"INSERT INTO objects (ref, start, length, type, archive_id) VALUES (" + ref(tc.dialect, 2) + ", 30, 20, 3, 'archive-a')",
			} {
				_, err := db.ExecContext(ctx, stmt)
				require.NoError(t, err, stmt)
			}

			full, err := Dir(tc.dialect)
			require.NoError(t, err)
			require.NoError(t, applyDir(ctx, db, tc.dialect, full, revisionsTable, "lock", false))

			rows, err := db.QueryContext(ctx, "SELECT o.id, a.name, o.start, o.length, o.type, coalesce(o.carried_time, 0), coalesce(o.carried_offset, 0) FROM objects o JOIN archives a ON a.id = o.archive_id ORDER BY o.id")
			require.NoError(t, err)
			defer rows.Close()

			type row struct {
				id                                     int64
				archive                                string
				start, length, typ, carried, carriedAt int64
			}

			var got []row
			for rows.Next() {
				var r row
				require.NoError(t, rows.Scan(&r.id, &r.archive, &r.start, &r.length, &r.typ, &r.carried, &r.carriedAt))
				got = append(got, r)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, []row{{1, "archive-a", 0, 10, 2, 0, 0}, {2, "archive-b", 10, 20, 3, 7, 4}, {3, "archive-a", 30, 20, 3, 0, 0}}, got)

			_, err = db.ExecContext(ctx, "INSERT INTO archives (name, state) VALUES ('archive-c', 2)")
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, "INSERT INTO objects (ref, start, length, type, archive_id) SELECT "+ref(tc.dialect, 3)+", 0, 1, 2, id FROM archives WHERE name = 'archive-c'")
			require.NoError(t, err)

			var newest int64
			require.NoError(t, db.QueryRowContext(ctx, "SELECT max(id) FROM objects").Scan(&newest))
			require.Equal(t, int64(4), newest, "new objects are numbered after the migrated ones")
		})
	}
}

// ref is a 32-byte ref literal of the dialect ending in b.
func ref(dialect string, b byte) string {
	if dialect == Postgres {
		return fmt.Sprintf("decode(repeat('00', 31) || '%02x', 'hex')", b)
	}

	return fmt.Sprintf("x'%s%02x'", strings.Repeat("00", 31), b)
}
