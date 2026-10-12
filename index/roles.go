package index

import (
	"context"
	"time"

	"github.com/gobackio/goback/proto"
)

// LostVersion is a stored version the store cannot read, with the live
// commits that hold it.
type LostVersion struct {
	Path    string
	Ref     *proto.Ref
	Commits []*proto.Ref
}

// LostLister is an index that knows which versions a repair could not
// keep.
type LostLister interface {
	// LostVersions lists the versions of set the store cannot read, with
	// the commits each one damages.
	LostVersions(ctx context.Context, set string) ([]LostVersion, error)
}

// Unretirer is an index that brings tombstoned commits back.
type Unretirer interface {
	TombstonedCommits(ctx context.Context, set string, since time.Time) ([]*proto.Ref, error)
	UnretireCommits(ctx context.Context, refs []*proto.Ref, dryRun bool) ([]Unretired, error)
}

// CommitIndexer is an index that indexes commits the store holds.
type CommitIndexer interface {
	IndexCommits(ctx context.Context, refs []*proto.Ref, dryRun bool) ([]IndexedSet, error)
}

// OrphanFinder is an index that finds the commits the store holds and it
// has no row for.
type OrphanFinder interface {
	OrphanCommits(ctx context.Context) ([]OrphanCommit, error)
}

// SizeFiller is an index that works out the size of each commit it
// recorded without one.
type SizeFiller interface {
	FillMissingSizes(ctx context.Context) (int, error)
}
