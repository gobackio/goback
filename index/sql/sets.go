package sql

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/index"
	"github.com/gobackio/goback/index/sql/ent"
	"github.com/gobackio/goback/index/sql/ent/commitrow"
	"github.com/gobackio/goback/index/sql/ent/pin"
	"github.com/gobackio/goback/index/sql/ent/predicate"
	"github.com/gobackio/goback/index/sql/ent/set"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/pack"

	entsql "entgo.io/ent/dialect/sql"
)

// sizeOf is the size each of ids has in sizes, NULL for any other set.
func sizeOf(ids []int64, sizes map[int64]uint64) entsql.Querier {
	return entsql.ExprFunc(func(b *entsql.Builder) {
		if len(ids) == 0 {
			b.WriteString("NULL")
			return
		}

		b.WriteString("CASE ").Ident(set.FieldID)
		for _, id := range ids {
			b.WriteString(" WHEN ").Arg(id).WriteString(" THEN CAST(").Arg(int64(sizes[id])).WriteString(" AS BIGINT)")
		}
		b.WriteString(" END")
	})
}

func anys[T any](values []T) []any {
	out := make([]any, len(values))
	for i, v := range values {
		out[i] = v
	}

	return out
}

// findSet returns the id of the named set, or backup.ErrNotFound.
func findSet(ctx context.Context, c *ent.Client, name string) (int64, error) {
	id, err := c.Set.Query().Where(set.Name(name)).OnlyID(ctx)
	if ent.IsNotFound(err) {
		return 0, fmt.Errorf("%w: set %q", backup.ErrNotFound, name)
	}

	return id, err
}

// setNamed returns the id of the named set, creating it if needed. A
// non-zero wantID finds or recreates the set under the id its commits
// carry, which a rebuild after index loss needs, so same-named sets stay
// apart; a recreated set comes up with retention paused until an operator
// sets its policy. The name decides only when that id cannot be used.
func (x *Index) setNamed(ctx context.Context, c *ent.Client, name string, wantID int64) (int64, error) {
	if wantID != 0 {
		s, err := c.Set.Get(ctx, wantID)
		if ent.IsNotFound(err) {
			s, err = c.Set.Create().SetID(wantID).SetName(name).SetRetentionPaused(true).Save(ctx)
			if err == nil {
				err = x.advanceSetIDs(ctx, c)
			}
		}

		if err == nil {
			return s.ID, nil
		}

		if !ent.IsConstraintError(err) {
			return 0, err
		}
	}

	id, err := c.Set.Query().Where(set.Name(name)).OnlyID(ctx)
	if !ent.IsNotFound(err) {
		return id, err
	}

	created, err := c.Set.Create().SetName(name).Save(ctx)
	if err == nil {
		return created.ID, nil
	}

	// another commit created the set first
	if !ent.IsConstraintError(err) {
		return 0, err
	}

	return c.Set.Query().Where(set.Name(name)).OnlyID(ctx)
}

// openSet returns the id of the named set, creating it if needed. A set
// created under a session for it that carries an initial retention
// policy starts with that policy as its own.
func (x *Index) openSet(ctx context.Context, name string) (int64, error) {
	s, ok := backup.SessionFromContext(ctx)
	if !ok || s.InitialRetention == nil || s.Set != name {
		return x.setNamed(ctx, x.client, name, 0)
	}

	id, err := findSet(ctx, x.client, name)
	if !errors.Is(err, backup.ErrNotFound) {
		return id, err
	}

	return x.createSet(ctx, name, s.InitialRetention)
}

// createSet creates the named set with its own retention policy, which
// is recorded like any change to it. A set another caller created first
// under the name is returned as it is.
func (x *Index) createSet(ctx context.Context, name string, policy *retention.Policy) (int64, error) {
	raw, err := x.encodePolicy(policy)
	if err != nil {
		return 0, err
	}

	var id int64

	err = x.policyTx(ctx, func(tx *ent.Tx) (*proto.Policy, error) {
		s, err := tx.Set.Create().SetName(name).SetRetentionPolicy(*raw).Save(ctx)
		if err != nil {
			return nil, err
		}

		id = s.ID

		return setScope(s, time.Time{}), nil
	})
	if ent.IsConstraintError(err) {
		return findSet(ctx, x.client, name)
	}

	return id, err
}

// lockSet takes the set's row lock for the transaction.
func (x *Index) lockSet(ctx context.Context, tx *ent.Tx, setID int64) (*ent.Set, error) {
	s, err := forUpdate(x, tx.Set.Query().Where(set.ID(setID))).Only(ctx)
	if ent.IsNotFound(err) {
		return nil, fmt.Errorf("%w: set %d", backup.ErrNotFound, setID)
	}

	return s, err
}

// BeginCommit implements backup.CommitGate: the set is created if needed,
// with the initial retention of the context's session, it must be active,
// and the grant carries the store policy.
func (x *Index) BeginCommit(ctx context.Context, name string) (*backup.CommitGrant, error) {
	if _, ok := backup.SetID(name); ok {
		return nil, fmt.Errorf("%w: %q", backup.ErrSetName, name)
	}

	setID, err := x.openSet(ctx, name)
	if err != nil {
		return nil, err
	}

	s, err := x.client.Set.Get(ctx, setID)
	if err != nil {
		return nil, err
	}

	if s.State != set.StateActive {
		return nil, fmt.Errorf("%w: set %q", backup.ErrSetClosed, name)
	}

	grant := &backup.CommitGrant{}

	grant.Policy, err = x.StorePolicy(ctx)
	if err != nil {
		return nil, err
	}

	grant.Rescan, grant.Damaged, err = x.damage(ctx, s)
	if err != nil {
		return nil, err
	}

	return grant, nil
}

// ListSets returns every set by name, with the sizes MeasureSets and the
// last garbage collection stored for it.
func (x *Index) ListSets(ctx context.Context) ([]index.SetInfo, error) {
	return x.QuerySets(ctx, index.SetQuery{})
}

// GetSet returns the named set as ListSets describes it, or
// backup.ErrNotFound.
func (x *Index) GetSet(ctx context.Context, name string) (index.SetInfo, error) {
	row, err := x.client.Set.Query().Where(set.Name(name)).Only(ctx)
	if ent.IsNotFound(err) {
		return index.SetInfo{}, fmt.Errorf("%w: set %q", backup.ErrNotFound, name)
	}

	if err != nil {
		return index.SetInfo{}, err
	}

	return m.Set(row), nil
}

// QuerySets returns a page of the sets q picks, described as ListSets
// describes them. The last one's Name and PhysicalSize are the After and
// AfterSize of the next page.
func (x *Index) QuerySets(ctx context.Context, q index.SetQuery) ([]index.SetInfo, error) {
	query := x.client.Set.Query().Where(setsPicked(q)...)

	switch {
	case q.BySize:
		query.Where(set.PhysicalSizeGT(0)).Order(ent.Desc(set.FieldPhysicalSize), ent.Asc(set.FieldName))
		if q.After != "" {
			query.Where(set.Or(set.PhysicalSizeLT(q.AfterSize), set.And(set.PhysicalSize(q.AfterSize), set.NameGT(q.After))))
		}
	default:
		query.Order(ent.Asc(set.FieldName))
		if q.After != "" {
			query.Where(set.NameGT(q.After))
		}
	}

	if q.Limit > 0 {
		query.Limit(q.Limit)
	}

	rows, err := query.All(ctx)
	if err != nil {
		return nil, err
	}

	return mapAll(rows, m.Set), nil
}

// CountSets counts the sets in one of the states, every set when none is
// given.
func (x *Index) CountSets(ctx context.Context, states ...string) (int, error) {
	return x.client.Set.Query().Where(setsPicked(index.SetQuery{States: states})...).Count(ctx)
}

// setsPicked is what keeps a set in q, apart from paging and order.
func setsPicked(q index.SetQuery) []predicate.Set {
	var where []predicate.Set
	if len(q.States) > 0 {
		states := make([]set.State, len(q.States))
		for i, s := range q.States {
			states[i] = set.State(s)
		}

		where = append(where, set.StateIn(states...))
	}

	if q.Match != "" {
		match := set.NameContainsFold(q.Match)
		if len(q.Named) > 0 {
			match = set.Or(match, set.NameIn(q.Named...))
		}

		where = append(where, match)
	}

	return where
}

// RootOwner returns the lookup a garbage collection attributes with: it
// answers a root, a commit or a pin holding one, with the set it belongs
// to and the group that set is collected in, and anything else with
// nothing.
func (x *Index) RootOwner(ctx context.Context) (func(root []byte) pack.Attribution, error) {
	var groups map[int64]int64
	if x.Grouping != nil {
		var err error
		if groups, err = x.Grouping(ctx); err != nil {
			return nil, err
		}
	}

	commits, err := x.client.CommitRow.Query().Select(commitrow.FieldRef, commitrow.FieldSetID).All(ctx)
	if err != nil {
		return nil, err
	}

	owners := make(map[string]int64, len(commits))
	for _, row := range commits {
		owners[string(row.Ref)] = row.SetID
	}

	pins, err := x.client.Pin.Query().Select(pin.FieldRef, pin.FieldTarget).All(ctx)
	if err != nil {
		return nil, err
	}

	for _, row := range pins {
		if owner, ok := owners[string(row.Target)]; ok {
			owners[string(row.Ref)] = owner
		}
	}

	return func(root []byte) pack.Attribution {
		set, ok := owners[string(root)]
		if !ok {
			return pack.Attribution{}
		}

		return pack.Attribution{Group: groups[set], Set: set}
	}, nil
}

// RecordSetSizes records what a garbage collection attributed to each set:
// the physical, deduplicated, alone, exclusive and deduplicated alone sizes; a set it did not
// name holds nothing of its own. What it attributed to a set the index
// does not hold is added to report.Unattributed.
func (x *Index) RecordSetSizes(ctx context.Context, report *pack.CollectReport) error {
	var unknown uint64

	ids := make([]int64, 0, len(report.SetBytes))
	for id := range report.SetBytes {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	err := x.tx(ctx, func(tx *ent.Tx) error {
		unknown = 0

		var known []int64
		err := inBatches(ids, func(batch []int64) error {
			found, err := tx.Set.Query().Where(set.IDIn(batch...)).IDs(ctx)
			known = append(known, found...)

			return err
		})
		if err != nil {
			return err
		}

		held := make(map[int64]bool, len(known))
		for _, id := range known {
			held[id] = true
		}

		for _, id := range ids {
			if !held[id] {
				x.logger().Warn("gc attributed objects to a set the index does not hold", "set", id, "bytes", report.SetBytes[id])
				unknown += report.SetBytes[id]
			}
		}

		slices.Sort(known)

		columns := map[string]map[int64]uint64{
			set.FieldPhysicalSize:          report.SetBytes,
			set.FieldDeduplicatedSize:      report.SetDeduplicated,
			set.FieldAloneSize:             report.SetAlone,
			set.FieldExclusiveSize:         report.SetExclusive,
			set.FieldDeduplicatedAloneSize: report.SetDeduplicatedAlone,
		}

		d := entsql.Dialect(x.dialect)

		// the first statement has no condition, so it also clears the
		// sizes of every set the collection did not name
		for start := 0; start == 0 || start < len(known); start += objectBatch {
			batch := known[start:min(start+objectBatch, len(known))]

			update := d.Update(set.Table)
			for column, sizes := range columns {
				update.Set(column, sizeOf(batch, sizes))
			}

			if start > 0 {
				update.Where(entsql.In(set.FieldID, anys(batch)...))
			}

			query, args := update.Query()
			if _, err := tx.ExecContext(ctx, query, args...); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	report.Unattributed += unknown

	return nil
}
