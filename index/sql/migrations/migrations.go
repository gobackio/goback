// Package migrations holds the versioned migrations of the index, one
// directory per dialect, and applies them on open. After a schema change,
// regenerate the ent client and run `go run ./gen -name <change>` from
// index/sql/migrations to append a migration to every dialect's directory.
package migrations

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"time"

	entmigrate "github.com/gobackio/goback/index/sql/ent/migrate"

	"ariga.io/atlas/sql/migrate"
	"ariga.io/atlas/sql/postgres"
	"ariga.io/atlas/sql/schema"
	"ariga.io/atlas/sql/sqlite"
	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	entschema "entgo.io/ent/dialect/sql/schema"
)

// Dialects the index migrates.
const (
	SQLite   = "sqlite"
	Postgres = "postgres"
)

//go:embed sqlite postgres
var files embed.FS

// Dir returns a dialect's migration directory, checksum file included.
func Dir(dialect string) (*migrate.MemDir, error) {
	return ReadDir(files, dialect)
}

// ReadDir loads the migration directory named dialect from fsys, checksum
// file included, for a schema that ships its own migrations.
func ReadDir(fsys fs.FS, dialect string) (*migrate.MemDir, error) {
	entries, err := fs.ReadDir(fsys, dialect)
	if err != nil {
		return nil, fmt.Errorf("no migrations for dialect %q: %w", dialect, err)
	}

	dir := &migrate.MemDir{}
	for _, entry := range entries {
		data, err := fs.ReadFile(fsys, path.Join(dialect, entry.Name()))
		if err != nil {
			return nil, err
		}

		if err := dir.WriteFile(entry.Name(), data); err != nil {
			return nil, err
		}
	}

	if err := migrate.Validate(dir); err != nil {
		return nil, fmt.Errorf("migrations for %s: %w", dialect, err)
	}

	return dir, nil
}

// lockTimeout bounds the wait for another process's migration.
const lockTimeout = 5 * time.Minute

// Apply brings db to the newest migration of the dialect, recording what
// it applied in the atlas_schema_revisions table. A database that is up
// to date is left alone. Concurrent callers take turns: an SQLite
// migration is one write transaction, a Postgres one holds an advisory
// lock on its schema.
func Apply(ctx context.Context, db *sql.DB, dialect string) error {
	dir, err := Dir(dialect)
	if err != nil {
		return err
	}

	return applyDir(ctx, db, dialect, dir, revisionsTable, "goback_migrations", false)
}

// ApplyDir is Apply for a schema that ships its own migrations: dir is
// applied on top of whatever else the database holds, its history kept in
// the table named table and its Postgres runs on one schema serialised
// under lock.
func ApplyDir(ctx context.Context, db *sql.DB, dialect string, dir migrate.Dir, table, lock string) error {
	return applyDir(ctx, db, dialect, dir, table, lock, true)
}

func applyDir(ctx context.Context, db *sql.DB, dialect string, dir migrate.Dir, table, lock string, allowDirty bool) error {
	switch dialect {
	case SQLite:
		conn, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		defer conn.Close()

		return applySQLite(ctx, conn, dir, table, allowDirty)
	case Postgres:
		drv, err := postgres.Open(db)
		if err != nil {
			return err
		}

		var current string
		if err := db.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&current); err != nil {
			return fmt.Errorf("reading the current schema: %w", err)
		}

		// advisory locks span the database, which other schemas share
		unlock, err := drv.(schema.Locker).Lock(ctx, lock+"."+current, lockTimeout)
		if err != nil {
			return fmt.Errorf("waiting for another migration: %w", err)
		}
		defer func() { _ = unlock() }()

		return apply(ctx, db, dialect, dir, table, allowDirty)
	default:
		return fmt.Errorf("no migrations for dialect %q", dialect)
	}
}

// applySQLite runs the pending migrations in one transaction on conn with
// its foreign keys off, as a table rebuild needs: SQLite ignores the
// pragma inside a transaction, where dropping the old table would delete
// through the keys that reference it. The keys are checked before the
// commit and turned back on after it.
func applySQLite(ctx context.Context, conn *sql.Conn, dir migrate.Dir, table string, allowDirty bool) (err error) {
	var keys bool
	if err := conn.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&keys); err != nil {
		return fmt.Errorf("reading the foreign_keys pragma: %w", err)
	}

	if keys {
		if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys = off"); err != nil {
			return err
		}

		defer func() {
			if _, onErr := conn.ExecContext(context.WithoutCancel(ctx), "PRAGMA foreign_keys = on"); onErr != nil {
				// a connection without its foreign keys never goes back to
				// the pool
				_ = conn.Raw(func(any) error { return driver.ErrBadConn })
				err = errors.Join(err, onErr)
			}
		}()
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if err := apply(ctx, tx, SQLite, dir, table, allowDirty); err != nil {
		_ = tx.Rollback()
		return err
	}

	if keys {
		if err := foreignKeyCheck(ctx, tx); err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

// foreignKeyCheck fails on the first row a foreign key of the database no
// longer finds.
func foreignKeyCheck(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return err
	}
	defer rows.Close()

	if rows.Next() {
		var (
			child, parent string
			rowid         sql.NullInt64
			key           int64
		)

		if err := rows.Scan(&child, &rowid, &parent, &key); err != nil {
			return err
		}

		return fmt.Errorf("migrating leaves row %d of %s without its %s", rowid.Int64, child, parent)
	}

	return rows.Err()
}

// querier is what the migration runs on: a database or a transaction.
type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func apply(ctx context.Context, q querier, dialect string, dir migrate.Dir, table string, allowDirty bool) error {
	drv, err := open(q, dialect)
	if err != nil {
		return err
	}

	rrw, err := newRevisions(ctx, q, dialect, table)
	if err != nil {
		return err
	}

	ex, err := migrate.NewExecutor(drv, dir, rrw, migrate.WithOperatorVersion("goback"), migrate.WithAllowDirty(allowDirty))
	if err != nil {
		return err
	}

	err = ex.ExecuteN(ctx, 0)
	if errors.Is(err, migrate.ErrNoPendingFiles) {
		return nil
	}

	return err
}

// Diff writes into dir the migration named name that takes the schema dir
// reproduces to the current ent schema, replaying dir on db, an empty
// scratch database of the dialect that is left empty again. It returns
// migrate.ErrNoPlan when dir is up to date.
func Diff(ctx context.Context, db *sql.DB, dialect string, dir migrate.Dir, name string) error {
	return DiffWith(ctx, Schema(db, dialect), dir, name)
}

// Schema is goback's index schema over db in the dialect.
func Schema(db *sql.DB, dialect string) Differ {
	return entmigrate.NewSchema(entsql.OpenDB(EntDialect(dialect), db))
}

// Differ is what a generated ent migrate package's NewSchema returns.
type Differ interface {
	NamedDiff(ctx context.Context, name string, opts ...entschema.MigrateOption) error
}

// DiffWith is Diff for a schema that ships its own migrations.
func DiffWith(ctx context.Context, differ Differ, dir migrate.Dir, name string) error {
	return differ.NamedDiff(ctx, name,
		entschema.WithDir(dir),
		entschema.WithMigrationMode(entschema.ModeReplay),
		entschema.WithFormatter(migrate.DefaultFormatter),
		entschema.WithDropIndex(true),
		entschema.WithDropColumn(true),
		entschema.WithErrNoPlan(true),
	)
}

// EntDialect is the ent dialect of a migration dialect.
func EntDialect(d string) string {
	if d == Postgres {
		return dialect.Postgres
	}

	return dialect.SQLite
}

func open(q querier, dialect string) (migrate.Driver, error) {
	switch dialect {
	case SQLite:
		return sqlite.Open(q)
	case Postgres:
		return postgres.Open(q)
	default:
		return nil, fmt.Errorf("no migrations for dialect %q", dialect)
	}
}

// revisions keeps the applied migrations in atlas_schema_revisions, the
// table the atlas tools use, so they can read the history too.
type revisions struct {
	db querier
	// schema is the database schema holding the table, empty for SQLite.
	schema string
	// table is the revisions table; empty means revisionsTable.
	table string
}

func (r *revisions) name() string {
	if r.table == "" {
		return revisionsTable
	}

	return r.table
}

const revisionsTable = "atlas_schema_revisions"

const revisionColumns = "version, description, type, applied, total, executed_at, execution_time, error, error_stmt, hash, partial_hashes, operator_version"

func newRevisions(ctx context.Context, db querier, dialect, table string) (*revisions, error) {
	r := &revisions{db: db, table: table}

	if dialect == Postgres {
		err := db.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&r.schema)
		if err != nil {
			return nil, fmt.Errorf("reading the current schema: %w", err)
		}
	}

	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+r.name()+` (
	version TEXT PRIMARY KEY,
	description TEXT NOT NULL,
	type INTEGER NOT NULL,
	applied INTEGER NOT NULL,
	total INTEGER NOT NULL,
	executed_at TIMESTAMP NOT NULL,
	execution_time BIGINT NOT NULL,
	error TEXT NOT NULL DEFAULT '',
	error_stmt TEXT NOT NULL DEFAULT '',
	hash TEXT NOT NULL,
	partial_hashes TEXT NOT NULL DEFAULT '[]',
	operator_version TEXT NOT NULL DEFAULT ''
)`)
	if err != nil {
		return nil, fmt.Errorf("creating %s: %w", r.name(), err)
	}

	return r, nil
}

func (r *revisions) Ident() *migrate.TableIdent {
	return &migrate.TableIdent{Name: r.name(), Schema: r.schema}
}

func (r *revisions) ReadRevisions(ctx context.Context) ([]*migrate.Revision, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT `+revisionColumns+` FROM `+r.name()+` ORDER BY version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*migrate.Revision
	for rows.Next() {
		rev, err := scanRevision(rows)
		if err != nil {
			return nil, err
		}

		out = append(out, rev)
	}

	return out, rows.Err()
}

func (r *revisions) ReadRevision(ctx context.Context, version string) (*migrate.Revision, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT `+revisionColumns+` FROM `+r.name()+` WHERE version = $1`, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}

		return nil, migrate.ErrRevisionNotExist
	}

	return scanRevision(rows)
}

func (r *revisions) WriteRevision(ctx context.Context, rev *migrate.Revision) error {
	partial, err := json.Marshal(rev.PartialHashes)
	if err != nil {
		return err
	}

	_, err = r.db.ExecContext(ctx, `INSERT INTO `+r.name()+` (`+revisionColumns+`)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	ON CONFLICT (version) DO UPDATE SET description = $2, type = $3, applied = $4, total = $5, executed_at = $6,
	execution_time = $7, error = $8, error_stmt = $9, hash = $10, partial_hashes = $11, operator_version = $12`,
		rev.Version, rev.Description, int64(rev.Type), rev.Applied, rev.Total, rev.ExecutedAt.UTC(), int64(rev.ExecutionTime),
		rev.Error, rev.ErrorStmt, rev.Hash, string(partial), rev.OperatorVersion)

	return err
}

func (r *revisions) DeleteRevision(ctx context.Context, version string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM `+r.name()+` WHERE version = $1`, version)

	return err
}

func scanRevision(rows *sql.Rows) (*migrate.Revision, error) {
	var (
		rev      migrate.Revision
		kind     int64
		executed time.Time
		duration int64
		partial  string
	)

	err := rows.Scan(&rev.Version, &rev.Description, &kind, &rev.Applied, &rev.Total, &executed, &duration,
		&rev.Error, &rev.ErrorStmt, &rev.Hash, &partial, &rev.OperatorVersion)
	if err != nil {
		return nil, err
	}

	rev.Type = migrate.RevisionType(kind)
	rev.ExecutedAt = executed
	rev.ExecutionTime = time.Duration(duration)

	if partial != "" {
		if err := json.Unmarshal([]byte(partial), &rev.PartialHashes); err != nil {
			return nil, err
		}
	}

	return &rev, nil
}
