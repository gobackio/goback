// Package sql is the index of a store: the commits, versions and pins of
// its sets with their retention state, the presence filters, the store
// settings and the archive index of the pack store, in one database that
// is SQLite for a local store and Postgres for a server.
package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/index/sql/ent"
	"github.com/gobackio/goback/index/sql/migrations"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/pack"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"golang.org/x/sync/errgroup"
	_ "modernc.org/sqlite"
)

var (
	_ backup.Index         = (*Index)(nil)
	_ backup.Retention     = (*Index)(nil)
	_ backup.Retirer       = (*Index)(nil)
	_ backup.PresenceIndex = (*Index)(nil)
	_ backup.HeaderWalker  = (*Index)(nil)
	_ backup.RefScope      = (*Index)(nil)
	_ backup.CommitGate    = (*Index)(nil)
	_ backup.PolicySource  = (*Index)(nil)
	_ pack.ArchiveIndex    = (*Index)(nil)
)

// sqliteFile is the database file inside a local index directory.
const sqliteFile = "index.db"

// Index is the store index over one database.
type Index struct {
	backup.ObjectStore

	location string
	db       *sql.DB
	dialect  string
	client   *ent.Client
	// locking is set where SELECT ... FOR UPDATE takes row locks; SQLite
	// serialises transactions as a whole instead.
	locking bool

	external  *externalDB
	noMigrate bool
	behind    backup.ObjectStore

	// DefaultPolicy is the retention policy sets inherit when the store
	// has none set; nil means retention.Default.
	DefaultPolicy *retention.Policy
	// Now is the clock; nil means time.Now.
	Now func() time.Time
	// Logger is where the index reports; nil means slog.Default.
	Logger *slog.Logger
	// Grouping, when set, names the group each set is collected in: a
	// group carries everything its sets reach, whatever another group
	// reaches too. nil counts every set of the store together.
	Grouping func(ctx context.Context) (map[int64]int64, error)

	stampMu   sync.Mutex
	lastStamp time.Time
}

func (x *Index) logger() *slog.Logger {
	if x.Logger != nil {
		return x.Logger
	}

	return slog.Default()
}

// New returns an index over store at location: a directory holds an
// SQLite database and postgres:// names a Postgres server. Open connects
// and applies the pending migrations.
func New(location string, store backup.ObjectStore, opts ...Option) *Index {
	x := &Index{ObjectStore: store, location: location}
	for _, opt := range opts {
		opt(x)
	}

	return x
}

// NewMemory returns an index over an in-memory SQLite database that lives
// as long as the index is open; name keeps two of them apart.
func NewMemory(name string, store backup.ObjectStore, opts ...Option) *Index {
	return New("memory://"+name, store, opts...)
}

// Open connects to the database and applies the pending migrations.
func (x *Index) Open() error {
	var (
		db      *sql.DB
		dialect string
		err     error
	)

	switch {
	case x.external != nil:
		db, dialect = x.external.db, x.external.dialect
		x.locking = x.external.locking()
	case strings.HasPrefix(x.location, "postgres://"), strings.HasPrefix(x.location, "postgresql://"):
		db, dialect, err = openPostgres(x.location)
		x.locking = true
	case strings.HasPrefix(x.location, "memory://"):
		db, dialect, err = openSQLite("file:" + strings.TrimPrefix(x.location, "memory://") + "?mode=memory&cache=shared&_pragma=foreign_keys(1)&_pragma=busy_timeout(300000)")
		if err == nil {
			// one connection: a shared in-memory database is dropped
			// once its last connection closes, and the pool would open
			// and close them freely
			db.SetMaxOpenConns(1)
		}
	default:
		dir := strings.TrimPrefix(x.location, "sqlite://")
		if u, perr := url.Parse(x.location); perr == nil && u.Scheme == "file" {
			dir = u.Host + u.Path
		}

		path := filepath.ToSlash(filepath.Join(dir, sqliteFile))
		db, dialect, err = openSQLite("file:" + path + "?_pragma=foreign_keys(1)&_pragma=busy_timeout(300000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_txlock=immediate")
		if err == nil {
			// the pool keeps what it opens: a backup's workers query at
			// once, and reopening the file with its pragmas for each
			// statement costs more than the statement
			db.SetMaxIdleConns(sqliteIdle)
			db.SetConnMaxIdleTime(5 * time.Minute)
		}
	}

	if err != nil {
		return err
	}

	if err := ping(db); err != nil {
		db.Close()
		return fmt.Errorf("connecting to the index: %w", err)
	}

	if !x.noMigrate {
		err = migrations.Apply(context.Background(), db, migrationDialect(dialect))
		if err != nil {
			db.Close()
			return fmt.Errorf("migrating the index schema: %w", err)
		}
	}

	client := ent.NewClient(ent.Driver(entsql.OpenDB(dialect, db)))

	x.db = db
	x.dialect = dialect
	x.client = client

	return x.seedStamp(context.Background())
}

// migrationDialect names the migration directory of an ent dialect.
func migrationDialect(d string) string {
	if d == dialect.Postgres {
		return migrations.Postgres
	}

	return migrations.SQLite
}

// sqliteIdle is how many connections a file index keeps open between
// statements.
const sqliteIdle = 64

func openPostgres(dsn string) (*sql.DB, string, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, "", err
	}

	db.SetMaxOpenConns(100)
	// the pool keeps what it opens: closing a connection between
	// statements costs a socket in TIME_WAIT, which an index under a
	// backup runs through the ephemeral ports with in minutes
	db.SetMaxIdleConns(100)
	db.SetConnMaxIdleTime(5 * time.Minute)
	db.SetConnMaxLifetime(time.Hour)

	return db, dialect.Postgres, nil
}

func openSQLite(dsn string) (*sql.DB, string, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, "", err
	}

	return db, dialect.SQLite, nil
}

// Close releases the database; one passed through WithDB stays open.
func (x *Index) Close() error {
	if x.client == nil {
		return nil
	}

	client := x.client
	x.client = nil

	if x.external != nil {
		return nil
	}

	return client.Close()
}

func (x *Index) now() time.Time {
	now := time.Now()
	if x.Now != nil {
		now = x.Now()
	}

	// Postgres keeps microseconds; a stamp that survives the round trip
	// keeps a commit's hash stable
	return now.UTC().Truncate(time.Microsecond)
}

// stamp is the clock for receipt times: strictly increasing, so two
// commits of one set received within a microsecond stay ordered.
func (x *Index) stamp() time.Time {
	x.stampMu.Lock()
	defer x.stampMu.Unlock()

	now := x.now()
	if !now.After(x.lastStamp) {
		now = x.lastStamp.Add(time.Microsecond)
	}

	x.lastStamp = now

	return now
}

// ping opens the first connection. On SQLite the connection's pragmas
// need the file to themselves for a moment, which another process
// migrating it denies with SQLITE_BUSY, so a busy answer is retried.
func ping(db *sql.DB) error {
	deadline := time.Now().Add(busyWait)
	for {
		err := db.Ping()
		if err == nil || !strings.Contains(err.Error(), "SQLITE_BUSY") || time.Now().After(deadline) {
			return err
		}

		time.Sleep(50 * time.Millisecond)
	}
}

// busyWait bounds how long an open waits for another process's migration.
const busyWait = 5 * time.Minute

// tx runs fn in one transaction.
func (x *Index) tx(ctx context.Context, fn func(tx *ent.Tx) error) (err error) {
	tx, err := x.client.Tx(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()

	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

// lockable is a query that can take a row lock.
type lockable[Q any] interface {
	ForUpdate(...entsql.LockOption) Q
}

// forUpdate locks the rows a query selects where the database locks rows.
func forUpdate[Q lockable[Q]](x *Index, q Q) Q {
	if !x.locking {
		return q
	}

	return q.ForUpdate()
}

// Delete implements backup.ObjectStore: objects are retired through
// retention, never deleted directly.
func (x *Index) Delete(ctx context.Context, ref *proto.Ref) error {
	return errors.New("direct deletion of objects not supported")
}

// WalkHeaders implements backup.HeaderWalker when the store does.
func (x *Index) WalkHeaders(ctx context.Context, t proto.ObjectType, fn func(*proto.ObjectHeader) error) error {
	hw, ok := x.ObjectStore.(backup.HeaderWalker)
	if !ok {
		return backup.ErrNotImplemented
	}

	return hw.WalkHeaders(ctx, t, fn)
}

// LocateRecords implements backup.RecordLocator when the store does, and
// otherwise locates nothing.
func (x *Index) LocateRecords(ctx context.Context, refs []*proto.Ref) ([]*proto.LocatedRun, error) {
	locator, ok := storeAs[backup.RecordLocator](x.ObjectStore)
	if !ok {
		return nil, nil
	}

	return locator.LocateRecords(ctx, refs)
}

// PutEscrowedKey implements backup.KeyEscrow when the store does.
func (x *Index) PutEscrowedKey(ctx context.Context, key backup.EscrowedKey) error {
	escrow, ok := storeAs[backup.KeyEscrow](x.ObjectStore)
	if !ok {
		return backup.ErrNotImplemented
	}

	return escrow.PutEscrowedKey(ctx, key)
}

// EscrowedKeys implements backup.KeyEscrow when the store does.
func (x *Index) EscrowedKeys(ctx context.Context) ([]backup.EscrowedKey, error) {
	escrow, ok := storeAs[backup.KeyEscrow](x.ObjectStore)
	if !ok {
		return nil, backup.ErrNotImplemented
	}

	return escrow.EscrowedKeys(ctx)
}

func (x *Index) flush() error {
	if f, ok := x.ObjectStore.(interface{ Flush() error }); ok {
		return f.Flush()
	}

	return nil
}

// HasAll implements backup.HasAller as the store's Has would answer.
func (x *Index) HasAll(ctx context.Context, refs []*proto.Ref) ([]bool, error) {
	return backup.HasAll(ctx, x.ObjectStore, refs)
}

var _ backup.Batcher = (*Index)(nil)

// Missing implements backup.Batcher as HasAll would answer.
func (x *Index) Missing(ctx context.Context, refs []*proto.Ref) ([]*proto.Ref, error) {
	return backup.Missing(ctx, x, refs)
}

// checks reports whether a write under ctx checks what it references: one
// in a session of a store that checks at commit doesn't.
func (x *Index) checks(ctx context.Context) bool {
	if x.behind != nil {
		return false
	}

	if _, ok := backup.SessionFromContext(ctx); !ok {
		return true
	}

	checker, ok := storeAs[backup.CommitChecker](x.ObjectStore)

	return !ok || !checker.ChecksAtCommit()
}

// putObjectsWorkers bounds the objects one PutObjects call stores at once.
const putObjectsWorkers = 32

// PutObjects implements backup.Batcher, checking what the objects reference
// beyond each other in one lookup. A commit or pin goes through Put, which
// indexes it.
func (x *Index) PutObjects(ctx context.Context, objects []*proto.Object) error {
	for _, object := range objects {
		if object.GetCommit() != nil || object.GetPin() != nil {
			return fmt.Errorf("a %s goes through Put", object.Type())
		}
	}

	if x.checks(ctx) {
		if err := backup.CheckAllReferences(ctx, x.ObjectStore, objects); err != nil {
			return err
		}
	}

	for _, level := range backup.ByReference(objects) {
		grp, gctx := errgroup.WithContext(ctx)
		grp.SetLimit(putObjectsWorkers)

		for _, object := range level {
			grp.Go(func() error { return x.ObjectStore.Put(gctx, object) })
		}

		if err := grp.Wait(); err != nil {
			return err
		}
	}

	return nil
}

// ReadRecords implements backup.RecordReader when the store does, and
// reads nothing otherwise.
func (x *Index) ReadRecords(ctx context.Context, refs []*proto.Ref) ([]*proto.Object, error) {
	reader, ok := storeAs[backup.RecordReader](x.ObjectStore)
	if !ok {
		return make([]*proto.Object, len(refs)), nil
	}

	return reader.ReadRecords(ctx, refs)
}

// storeAs finds the first store in the wrapper chain that implements T.
func storeAs[T any](store backup.ObjectStore) (T, bool) {
	for store != nil {
		if t, ok := store.(T); ok {
			return t, true
		}

		wrapper, ok := store.(interface{ Unwrap() backup.ObjectStore })
		if !ok {
			break
		}

		store = wrapper.Unwrap()
	}

	var zero T

	return zero, false
}

// ignoreNoRows drops the error a DO NOTHING upsert returns when the row
// already exists.
func ignoreNoRows(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}

	return err
}

func ptr[T any](v T) *T {
	return &v
}

func nilIfZero(s string) *string {
	if s == "" {
		return nil
	}

	return &s
}

func nilIfZeroTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}

	return &t
}

func deref[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	}

	return *p
}
