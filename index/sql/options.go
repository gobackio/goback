package sql

import (
	"context"
	"database/sql"
	"errors"

	"entgo.io/ent/dialect"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
)

// Option configures an index before it is opened.
type Option func(*Index)

// WithDB opens the index over a database the caller connected, instead
// of the location's; d is dialect.SQLite or dialect.Postgres. Open still
// applies the migrations unless WithoutMigrations says otherwise, and
// Close leaves db open.
func WithDB(db *sql.DB, d string) Option {
	return func(x *Index) {
		x.external = &externalDB{db: db, dialect: d}
	}
}

// WithoutMigrations makes Open trust that the schema is current, for a
// caller that migrates the database itself before opening indexes over
// it.
func WithoutMigrations() Option {
	return func(x *Index) {
		x.noMigrate = true
	}
}

// WithBehind puts the index's store in front of behind, which checks what
// the store holds when it gets there: a Put's references are left
// unchecked, and where the store holds nothing itself the trees and files
// a commit names are read from behind. Everything else, collection and
// retention among it, sees the store alone.
func WithBehind(behind backup.ObjectStore) Option {
	return func(x *Index) {
		x.behind = behind
	}
}

// externalDB is a caller-connected database.
type externalDB struct {
	db      *sql.DB
	dialect string
}

func (e *externalDB) locking() bool {
	return e.dialect == dialect.Postgres
}

// reads is the store a commit's trees are read from.
func (x *Index) reads() backup.ObjectStore {
	if x.behind == nil {
		return x.ObjectStore
	}

	return inFront{ObjectStore: x.ObjectStore, behind: x.behind}
}

// inFront is a store with another behind it for what it lacks.
type inFront struct {
	backup.ObjectStore
	behind backup.ObjectStore
}

func (s inFront) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	obj, err := s.ObjectStore.Get(ctx, ref)
	if errors.Is(err, backup.ErrNotFound) {
		return s.behind.Get(ctx, ref)
	}

	return obj, err
}

func (s inFront) Has(ctx context.Context, ref *proto.Ref) (bool, error) {
	ok, err := s.ObjectStore.Has(ctx, ref)
	if err != nil || ok {
		return ok, err
	}

	return s.behind.Has(ctx, ref)
}
