package sql

import (
	"context"
	"fmt"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/index/sql/ent"
	"github.com/gobackio/goback/index/sql/ent/commitrow"
	"github.com/gobackio/goback/index/sql/ent/set"
	"github.com/gobackio/goback/proto"

	entsql "entgo.io/ent/dialect/sql"
	pb "google.golang.org/protobuf/proto"
)

// storePresence records a commit's filter and drops the filters of the
// set's other commits, so only the head carries one; a filter of a commit
// older than the one that carries a filter is dropped instead.
func storePresence(ctx context.Context, x *Index, setID int64, ref *proto.Ref, filter *proto.PresenceFilter) error {
	data, err := pb.Marshal(filter)
	if err != nil {
		return err
	}

	return x.tx(ctx, func(tx *ent.Tx) error {
		if _, err := x.lockSet(ctx, tx, setID); err != nil {
			return err
		}

		row, err := tx.CommitRow.Query().Where(commitrow.Ref(ref.Hash), commitrow.SetID(setID), commitrow.TombstonedAtIsNil()).Only(ctx)
		if ent.IsNotFound(err) {
			return nil
		}

		if err != nil {
			return err
		}

		newer, err := tx.CommitRow.Query().Where(commitrow.SetID(setID), commitrow.PresenceNotNil(), commitrow.ReceivedAtGT(row.ReceivedAt)).Exist(ctx)
		if err != nil || newer {
			return err
		}

		err = tx.CommitRow.Update().Where(commitrow.Ref(ref.Hash)).SetPresence(data).Exec(ctx)
		if err != nil {
			return err
		}

		return tx.CommitRow.Update().Where(commitrow.SetID(setID), commitrow.RefNEQ(ref.Hash), commitrow.PresenceNotNil()).ClearPresence().Exec(ctx)
	})
}

// loadPresence returns the stored filters of every set in the scope.
func loadPresence(ctx context.Context, c *ent.Client, scope backup.PresenceScope, name string) ([]*proto.PresenceFilter, error) {
	query := c.CommitRow.Query().Where(commitrow.PresenceNotNil())

	switch scope {
	case backup.PresenceSet:
		query.Where(commitrow.HasSetWith(set.Name(name)))
	case backup.PresenceStore:
	default:
		return nil, nil
	}

	return decodePresence(ctx, query)
}

// PresenceOf returns the stored filters of the named sets.
func (x *Index) PresenceOf(ctx context.Context, sets ...string) ([]*proto.PresenceFilter, error) {
	return decodePresence(ctx, x.client.CommitRow.Query().Where(commitrow.PresenceNotNil(), commitrow.HasSetWith(set.NameIn(sets...))))
}

func decodePresence(ctx context.Context, query *ent.CommitRowQuery) ([]*proto.PresenceFilter, error) {
	rows, err := query.Select(commitrow.FieldPresence).All(ctx)
	if err != nil {
		return nil, err
	}

	var filters []*proto.PresenceFilter
	for _, row := range rows {
		filter := &proto.PresenceFilter{}
		if err := pb.Unmarshal(row.Presence, filter); err != nil {
			return nil, fmt.Errorf("decoding presence filter: %w", err)
		}

		filters = append(filters, filter)
	}

	return filters, nil
}

// BuildPresence builds and stores the presence filter of an indexed
// commit and records its logical size.
func (x *Index) BuildPresence(ctx context.Context, commit *proto.Ref) error {
	row, err := x.client.CommitRow.Query().Where(commitrow.Ref(commit.Hash)).WithSet().Only(ctx)
	if err != nil {
		return err
	}

	return x.buildPresence(ctx, row.SetID, row.Edges.Set.Name, commit, &proto.Ref{Hash: row.Tree})
}

// PresenceEvery is how old the commit a set's filter was built for may get
// before BuildPendingPresence builds one for a newer commit. Until then an
// agent skips uploads by the older filter.
const PresenceEvery = 6 * time.Hour

// BuildPendingPresence builds the filter of every set whose newest live
// commit has none, unless the set has a filter of a commit received less
// than PresenceEvery ago, and reports how many it built.
func (x *Index) BuildPendingPresence(ctx context.Context) (int, error) {
	d := entsql.Dialect(x.dialect)
	c, s := d.Table(commitrow.Table).As("c"), d.Table(set.Table).As("s")
	query, args := d.Select(c.C(commitrow.FieldSetID), s.C(set.FieldName), c.C(commitrow.FieldRef), c.C(commitrow.FieldTree)).
		AppendSelectExpr(entsql.ExprFunc(func(b *entsql.Builder) {
			b.WriteString("CASE WHEN ").Ident(c.C(commitrow.FieldPresence)).WriteString(" IS NULL OR LENGTH(").
				Ident(c.C(commitrow.FieldPresence)).WriteString(") = 0 THEN 1 ELSE 0 END")
		})).
		From(c).Join(s).On(c.C(commitrow.FieldSetID), s.C(set.FieldID)).
		Where(liveCommitColumns(c)).
		OrderBy(c.C(commitrow.FieldSetID), entsql.Desc(c.C(commitrow.FieldReceivedAt)), entsql.Desc(c.C(commitrow.FieldID))).Query()

	type newest struct {
		set       int64
		name      string
		ref, tree []byte
	}

	var pending []newest

	err := func() error {
		rows, err := x.client.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()

		seen := false
		var last int64

		for rows.Next() {
			var n newest
			var empty int
			if err := rows.Scan(&n.set, &n.name, &n.ref, &n.tree, &empty); err != nil {
				return err
			}

			if seen && n.set == last {
				continue
			}

			seen, last = true, n.set

			if empty == 1 {
				pending = append(pending, n)
			}
		}

		return rows.Err()
	}()
	if err != nil {
		return 0, err
	}

	built := 0
	for _, n := range pending {
		recent, err := x.client.CommitRow.Query().
			Where(commitrow.SetID(n.set), commitrow.PresenceNotNil(), commitrow.ReceivedAtGT(x.now().Add(-PresenceEvery)),
				commitrow.TombstonedAtIsNil(), commitrow.RetireAtIsNil(), commitrow.DeletedAtIsNil()).Exist(ctx)
		if err != nil {
			return built, err
		}

		if recent {
			continue
		}

		err = x.buildPresence(ctx, n.set, n.name, &proto.Ref{Hash: n.ref}, &proto.Ref{Hash: n.tree})
		if err != nil {
			return built, err
		}

		built++
	}

	return built, nil
}

func (x *Index) buildPresence(ctx context.Context, setID int64, name string, commit, tree *proto.Ref) error {
	start := time.Now()

	filter, err := backup.CollectPresence(ctx, x.ObjectStore, tree)
	if err != nil {
		return fmt.Errorf("building the filter of commit %x: %w", commit.Hash, err)
	}

	filter.Commit = commit
	filter.Set = name

	err = storePresence(ctx, x, setID, commit, filter.Proto())
	if err != nil {
		return fmt.Errorf("storing the filter of commit %x: %w", commit.Hash, err)
	}

	x.logger().Info("built presence filter", "set", name, "refs", filter.Entries(), "bytes", filter.Size(), "took", time.Since(start).Round(time.Millisecond))

	return nil
}
