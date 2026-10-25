package sql

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/index"
	"github.com/gobackio/goback/index/sql/ent"
	"github.com/gobackio/goback/index/sql/ent/commitrow"
	"github.com/gobackio/goback/index/sql/ent/deletedref"
	"github.com/gobackio/goback/index/sql/ent/file"
	"github.com/gobackio/goback/index/sql/ent/pin"
	"github.com/gobackio/goback/index/sql/ent/predicate"
	"github.com/gobackio/goback/index/sql/ent/set"
	"github.com/gobackio/goback/index/sql/ent/setref"
	"github.com/gobackio/goback/index/sql/ent/tree"
	"github.com/gobackio/goback/index/sql/ent/walkedarchive"
	"github.com/gobackio/goback/progress"
	"github.com/gobackio/goback/proto"
	"go.opentelemetry.io/otel/attribute"

	entsql "entgo.io/ent/dialect/sql"
	"golang.org/x/sync/errgroup"
)

// setConfig is what retention needs to know about a set.
type setConfig struct {
	name   string
	state  set.State
	paused bool
	policy retention.Policy
	trash  time.Duration
}

func (x *Index) loadSetConfig(ctx context.Context, c *ent.Client, setID int64) (*setConfig, error) {
	s, err := c.Set.Query().Where(set.ID(setID)).Only(ctx)
	if ent.IsNotFound(err) {
		return nil, fmt.Errorf("%w: set %d", backup.ErrNotFound, setID)
	}

	if err != nil {
		return nil, err
	}

	defaults, err := loadSettings(ctx, c)
	if err != nil {
		return nil, err
	}

	policy, err := x.policyOf(defaults, s)
	if err != nil {
		return nil, err
	}

	return &setConfig{
		name:   s.Name,
		state:  s.State,
		paused: s.RetentionPaused,
		policy: policy,
		trash:  time.Duration(defaults.TrashDays) * 24 * time.Hour,
	}, nil
}

// policyOf is the policy set s keeps by: its own, or else the store's.
func (x *Index) policyOf(defaults *ent.Settings, s *ent.Set) (retention.Policy, error) {
	policy := retention.Default
	if x.DefaultPolicy != nil {
		policy = *x.DefaultPolicy
	}

	for _, raw := range []*string{defaults.RetentionPolicy, s.RetentionPolicy} {
		if raw == nil {
			continue
		}

		p, err := retention.Parse([]byte(*raw))
		if err != nil {
			return policy, fmt.Errorf("set %d: %w", s.ID, err)
		}

		policy = p
	}

	return policy, nil
}

// evaluateSet applies the set's policy to its un-tombstoned, un-deleted
// commits: kept ones record their reasons, the rest are retired, for the
// next retirement to tombstone. A set whose retention
// is paused keeps everything until a policy is set.
func (x *Index) evaluateSet(ctx context.Context, tx *ent.Tx, setID int64, now time.Time) error {
	c := tx.Client()

	cfg, err := x.loadSetConfig(ctx, c, setID)
	if err != nil {
		return err
	}

	if cfg.state != set.StateActive || cfg.paused {
		return nil
	}

	rows, err := c.CommitRow.Query().Where(commitrow.SetID(setID), commitrow.TombstonedAtIsNil(), commitrow.DeletedAtIsNil()).All(ctx)
	if err != nil {
		return err
	}

	if len(rows) == 0 {
		return nil
	}

	pinned, err := c.Pin.Query().Where(pin.DeletedAtIsNil(), pinsInSet(setID)).Select(pin.FieldTarget).Strings(ctx)
	if err != nil {
		return err
	}

	held := make(map[string]bool, len(pinned))
	for _, target := range pinned {
		held[target] = true
	}

	commits := make([]retention.Commit, len(rows))
	for i, row := range rows {
		commits[i] = retention.Commit{ReceivedAt: row.ReceivedAt, Partial: row.Partial, Incomplete: row.Incomplete, Pinned: held[string(row.Ref)]}
	}

	policy, err := json.Marshal(cfg.policy)
	if err != nil {
		return err
	}

	keep := make(map[string][][]byte)
	var (
		retire  [][]byte
		changed []int
	)

	for i, d := range retention.Evaluate(commits, cfg.policy, now) {
		row := rows[i]

		switch {
		case d.Keep:
			by := d.RetainedBy()
			if row.RetainedBy == by && row.RetireAt == nil && row.RetirePolicy == nil && row.ExpiresAt == nil {
				continue
			}

			keep[by] = append(keep[by], row.Ref)
		case row.RetireAt == nil:
			retire = append(retire, row.Ref)
		default:
			continue
		}

		changed = append(changed, row.ID)
	}

	// the rows change under locks taken in id order, the order
	// writeTombstones takes them in, so the two never wait on each other
	if x.locking {
		slices.Sort(changed)

		err := inBatches(changed, func(ids []int) error {
			_, err := c.CommitRow.Query().Where(commitrow.IDIn(ids...)).Order(ent.Asc(commitrow.FieldID)).ForUpdate().IDs(ctx)
			return err
		})
		if err != nil {
			return err
		}
	}

	for by, refs := range keep {
		err := inBatches(refs, func(batch [][]byte) error {
			return c.CommitRow.Update().Where(commitrow.RefIn(batch...)).SetRetainedBy(by).ClearRetireAt().ClearRetirePolicy().
				ClearExpiresAt().Exec(ctx)
		})
		if err != nil {
			return err
		}
	}

	return inBatches(retire, func(batch [][]byte) error {
		return c.CommitRow.Update().Where(commitrow.RefIn(batch...)).SetRetainedBy("").SetRetireAt(now).SetRetirePolicy(string(policy)).
			SetExpiresAt(now).Exec(ctx)
	})
}

// pinsInSet keeps the pins whose target is a commit of the set.
func pinsInSet(setID int64) predicate.Pin {
	return func(s *entsql.Selector) {
		commits := entsql.Select(commitrow.FieldRef).From(entsql.Table(commitrow.Table)).Where(entsql.EQ(commitrow.FieldSetID, setID))
		s.Where(entsql.In(s.C(pin.FieldTarget), commits))
	}
}

// inBatches calls fn with consecutive runs of at most objectBatch items.
func inBatches[T any](items []T, fn func([]T) error) error {
	for start := 0; start < len(items); start += objectBatch {
		if err := fn(items[start:min(start+objectBatch, len(items))]); err != nil {
			return err
		}
	}

	return nil
}

// reevaluateSet re-applies a set's policy, as after a policy change.
func (x *Index) reevaluateSet(ctx context.Context, setID int64) error {
	return x.reevaluateSetAt(ctx, setID, x.now())
}

func (x *Index) reevaluateSetAt(ctx context.Context, setID int64, now time.Time) error {
	return x.tx(ctx, func(tx *ent.Tx) error {
		if _, err := x.lockSet(ctx, tx, setID); err != nil {
			return err
		}

		return x.evaluateSet(ctx, tx, setID, now)
	})
}

// GetPolicy reports a set's retention.
func (x *Index) GetPolicy(ctx context.Context, name string) (index.SetRetention, error) {
	setID, err := findSet(ctx, x.client, name)
	if err != nil {
		return index.SetRetention{}, err
	}

	cfg, err := x.loadSetConfig(ctx, x.client, setID)
	if err != nil {
		return index.SetRetention{}, err
	}

	s, err := x.client.Set.Get(ctx, setID)
	if err != nil {
		return index.SetRetention{}, err
	}

	ret := index.SetRetention{Effective: cfg.policy, Paused: cfg.paused}
	if s.RetentionPolicy != nil {
		p, err := retention.Parse([]byte(*s.RetentionPolicy))
		if err != nil {
			return index.SetRetention{}, err
		}

		ret.Policy = &p
	}

	return ret, nil
}

// SetPolicy stores a set's policy, nil to inherit the store's default,
// clamped by the limits, and re-evaluates the set. Setting a policy also
// resumes retention paused by a rebuild.
func (x *Index) SetPolicy(ctx context.Context, name string, p *retention.Policy) error {
	raw, err := x.encodePolicy(p)
	if err != nil {
		return err
	}

	return x.policyTx(ctx, func(tx *ent.Tx) (*proto.Policy, error) {
		setID, _, err := x.lockOwnSet(ctx, tx, name)
		if err != nil {
			return nil, err
		}

		update := tx.Set.UpdateOneID(setID).SetRetentionPaused(false)
		if raw == nil {
			update.ClearRetentionPolicy()
		} else {
			update.SetRetentionPolicy(*raw)
		}

		s, err := update.Save(ctx)
		if err != nil {
			return nil, err
		}

		// in the change's transaction, so no retirement acts on the old
		// policy's verdicts once the new one is stored
		return setScope(s, time.Time{}), x.evaluateSet(ctx, tx, setID, x.now())
	})
}

func (x *Index) encodePolicy(p *retention.Policy) (*string, error) {
	if p == nil {
		return nil, nil
	}

	policy := *p
	if policy.KeepLast < 1 {
		policy.KeepLast = 1
	}

	if err := policy.Validate(); err != nil {
		return nil, err
	}

	raw, err := json.Marshal(policy)
	if err != nil {
		return nil, err
	}

	return ptr(string(raw)), nil
}

// lockedCommit is a commit row under its lock, with the set it belongs to.
type lockedCommit struct {
	row        *ent.CommitRow
	tombstoned bool
	cfg        *setConfig
}

func (x *Index) lockOwnCommit(ctx context.Context, tx *ent.Tx, ref *proto.Ref) (*lockedCommit, error) {
	row, err := x.lockCommitRow(ctx, tx, ref.GetHash())
	if err != nil {
		return nil, err
	}

	cfg, err := x.loadSetConfig(ctx, tx.Client(), row.SetID)
	if err != nil {
		return nil, err
	}

	tombstoned := row.TombstonedAt != nil
	if !tombstoned {
		// a tombstone written but not yet durable already names the commit
		tombstoned, err = isDeleted(ctx, tx.Client(), ref.GetHash())
		if err != nil {
			return nil, err
		}
	}

	return &lockedCommit{row: row, tombstoned: tombstoned, cfg: cfg}, nil
}

// DeleteCommit implements backup.Retention.
func (x *Index) DeleteCommit(ctx context.Context, ref *proto.Ref) error {
	return x.policyTx(ctx, func(tx *ent.Tx) (*proto.Policy, error) {
		c, err := x.lockOwnCommit(ctx, tx, ref)
		if err != nil {
			return nil, err
		}

		if c.tombstoned {
			return nil, fmt.Errorf("%w: commit %x", backup.ErrTombstoned, ref.GetHash())
		}

		if c.cfg.state == set.StateActive {
			others, err := tx.CommitRow.Query().Where(commitrow.SetID(c.row.SetID), commitrow.RefNEQ(ref.GetHash()),
				commitrow.TombstonedAtIsNil(), commitrow.DeletedAtIsNil()).Exist(ctx)
			if err != nil {
				return nil, err
			}

			if !others {
				return nil, fmt.Errorf("%w: %x", backup.ErrNewestCommit, ref.GetHash())
			}
		}

		pinned, err := tx.Pin.Query().Where(pin.Target(ref.GetHash()), pin.DeletedAtIsNil()).Exist(ctx)
		if err != nil {
			return nil, err
		}

		if pinned {
			return nil, fmt.Errorf("%w: %x", backup.ErrPinned, ref.GetHash())
		}

		now := x.now()

		if err := trashCommit(ctx, tx, c.row, c.cfg.trash, now); err != nil {
			return nil, err
		}

		// the commit the deleted one was newer than may be retired, and is
		// the latest now
		return commitScope(ref.GetHash(), now), x.evaluateSet(ctx, tx, c.row.SetID, now)
	})
}

// trashCommit moves a commit to the trash at now, expiring a trash window
// later unless retention already lets it go sooner.
func trashCommit(ctx context.Context, tx *ent.Tx, row *ent.CommitRow, trash time.Duration, now time.Time) error {
	expires := now.Add(trash)
	if row.ExpiresAt != nil && row.ExpiresAt.Before(expires) {
		expires = *row.ExpiresAt
	}

	return tx.CommitRow.Update().Where(commitrow.Ref(row.Ref)).SetDeletedAt(now).SetRetainedBy("").SetExpiresAt(expires).Exec(ctx)
}

// UndeleteCommit implements backup.Retention.
func (x *Index) UndeleteCommit(ctx context.Context, ref *proto.Ref) error {
	return x.policyTx(ctx, func(tx *ent.Tx) (*proto.Policy, error) {
		c, err := x.lockOwnCommit(ctx, tx, ref)
		if err != nil {
			return nil, err
		}

		if c.tombstoned {
			return nil, fmt.Errorf("%w: commit %x", backup.ErrTombstoned, ref.GetHash())
		}

		if c.cfg.state != set.StateActive {
			return nil, fmt.Errorf("%w: set %q", backup.ErrSetClosed, c.cfg.name)
		}

		err = untrashCommit(ctx, tx, ref.GetHash())
		if err != nil {
			return nil, err
		}

		return commitScope(ref.GetHash(), time.Time{}), x.evaluateSet(ctx, tx, c.row.SetID, x.now())
	})
}

// untrashCommit takes a commit out of the trash, for retention to decide
// on again.
func untrashCommit(ctx context.Context, tx *ent.Tx, ref []byte) error {
	return tx.CommitRow.Update().Where(commitrow.Ref(ref)).ClearDeletedAt().ClearRetireAt().ClearRetirePolicy().ClearExpiresAt().Exec(ctx)
}

func (x *Index) lockOwnSet(ctx context.Context, tx *ent.Tx, name string) (int64, *setConfig, error) {
	setID, err := findSet(ctx, tx.Client(), name)
	if err != nil {
		return 0, nil, err
	}

	if _, err := x.lockSet(ctx, tx, setID); err != nil {
		return 0, nil, err
	}

	cfg, err := x.loadSetConfig(ctx, tx.Client(), setID)

	return setID, cfg, err
}

// DeleteSet implements backup.Retention.
func (x *Index) DeleteSet(ctx context.Context, name string, erase bool) error {
	return x.policyTx(ctx, func(tx *ent.Tx) (*proto.Policy, error) {
		setID, cfg, err := x.lockOwnSet(ctx, tx, name)
		if err != nil {
			return nil, err
		}

		if cfg.state == set.StateDeleted {
			return nil, fmt.Errorf("%w: set %q", backup.ErrTombstoned, name)
		}

		now := x.now()

		s, err := closeSet(ctx, tx, setID, cfg.trash, now, erase)
		if err != nil {
			return nil, err
		}

		return setScope(s, now), nil
	})
}

// closeSet closes a set at now and moves its live commits to the trash,
// expiring a trash window later, or at once for an erasure.
func closeSet(ctx context.Context, tx *ent.Tx, setID int64, trash time.Duration, now time.Time, erase bool) (*ent.Set, error) {
	expires := now.Add(trash)
	if erase {
		expires = now
	}

	update := tx.Set.UpdateOneID(setID).SetState(set.StateClosing)
	if erase {
		update.SetErase(true)
	}

	s, err := update.Save(ctx)
	if err != nil {
		return nil, err
	}

	live := []predicate.CommitRow{commitrow.SetID(setID), commitrow.TombstonedAtIsNil()}

	err = tx.CommitRow.Update().Where(append(live, commitrow.DeletedAtIsNil())...).SetDeletedAt(now).Exec(ctx)
	if err != nil {
		return nil, err
	}

	err = tx.CommitRow.Update().Where(append(live, commitrow.Or(commitrow.ExpiresAtIsNil(), commitrow.ExpiresAtGT(expires)))...).SetExpiresAt(expires).Exec(ctx)
	if err != nil {
		return nil, err
	}

	return s, tx.CommitRow.Update().Where(live...).SetRetainedBy("").Exec(ctx)
}

// UndeleteSet implements backup.Retention.
func (x *Index) UndeleteSet(ctx context.Context, name string) error {
	return x.policyTx(ctx, func(tx *ent.Tx) (*proto.Policy, error) {
		setID, cfg, err := x.lockOwnSet(ctx, tx, name)
		if err != nil {
			return nil, err
		}

		if cfg.state == set.StateDeleted {
			return nil, fmt.Errorf("%w: set %q", backup.ErrTombstoned, name)
		}

		s, err := reopenSet(ctx, tx, setID)
		if err != nil {
			return nil, err
		}

		return setScope(s, time.Time{}), x.evaluateSet(ctx, tx, setID, x.now())
	})
}

// reopenSet makes a closing set active again with every commit it still
// holds taken out of the trash.
func reopenSet(ctx context.Context, tx *ent.Tx, setID int64) (*ent.Set, error) {
	s, err := tx.Set.UpdateOneID(setID).SetState(set.StateActive).SetErase(false).Save(ctx)
	if err != nil {
		return nil, err
	}

	return s, tx.CommitRow.Update().Where(commitrow.SetID(setID), commitrow.TombstonedAtIsNil()).ClearDeletedAt().ClearRetireAt().ClearRetirePolicy().ClearExpiresAt().Exec(ctx)
}

// inVisibleSets keeps the pins whose commit belongs to a set the query
// can see.
func inVisibleSets() predicate.Pin {
	return func(s *entsql.Selector) {
		commits := entsql.Select(commitrow.FieldRef).From(entsql.Table(commitrow.Table))
		sets := entsql.Select(set.FieldID).From(entsql.Table(set.Table))
		commits.Where(entsql.In(commits.C(commitrow.FieldSetID), sets))

		s.Where(entsql.In(s.C(pin.FieldTarget), commits))
	}
}

// Unpin implements backup.Retention.
func (x *Index) Unpin(ctx context.Context, ref *proto.Ref) error {
	row, err := x.client.Pin.Query().Where(pin.Ref(ref.GetHash()), inVisibleSets()).Only(ctx)
	if ent.IsNotFound(err) {
		return fmt.Errorf("%w: pin %x", backup.ErrNotFound, ref.GetHash())
	}

	if err != nil {
		return err
	}

	if row.DeletedAt != nil {
		return fmt.Errorf("%w: pin %x", backup.ErrTombstoned, ref.GetHash())
	}

	// the tombstone is durable before the row says so; a second unpin in
	// the meantime writes a duplicate tombstone, which archives tolerate
	err = x.ObjectStore.Delete(ctx, ref)
	if err != nil {
		return err
	}

	if err := x.flush(); err != nil {
		return err
	}

	return x.tx(ctx, func(tx *ent.Tx) error {
		err := tx.Pin.Update().Where(pin.Ref(ref.GetHash()), pin.DeletedAtIsNil()).SetDeletedAt(x.now()).Exec(ctx)
		if err != nil {
			return err
		}

		setID, tombstoned, err := x.lockCommit(ctx, tx, row.Target)
		if err == nil && !tombstoned {
			err = x.evaluateSet(ctx, tx, setID, x.now())
		}

		if err != nil && !errors.Is(err, backup.ErrNotFound) {
			return err
		}

		return nil
	})
}

// Pins implements backup.Retention.
func (x *Index) Pins(ctx context.Context) ([]*proto.PinInfo, error) {
	rows, err := x.client.Pin.Query().Where(pin.DeletedAtIsNil(), inVisibleSets()).Order(ent.Asc(pin.FieldReceivedAt)).All(ctx)
	if err != nil {
		return nil, err
	}

	return mapAll(rows, m.Pin), nil
}

// PinsOf returns the pins Pins lists that hold one of the targets.
func (x *Index) PinsOf(ctx context.Context, targets ...*proto.Ref) ([]*proto.PinInfo, error) {
	if len(targets) == 0 {
		return nil, nil
	}

	hashes := make([][]byte, len(targets))
	for i, t := range targets {
		hashes[i] = t.GetHash()
	}

	var rows []*ent.Pin

	err := inBatches(hashes, func(batch [][]byte) error {
		found, err := x.client.Pin.Query().Where(pin.TargetIn(batch...), pin.DeletedAtIsNil(), inVisibleSets()).All(ctx)
		rows = append(rows, found...)

		return err
	})
	if err != nil {
		return nil, err
	}

	slices.SortStableFunc(rows, func(a, b *ent.Pin) int {
		if c := a.ReceivedAt.Compare(b.ReceivedAt); c != 0 {
			return c
		}

		return cmp.Compare(a.ID, b.ID)
	})

	return mapAll(rows, m.Pin), nil
}

// Retire implements backup.Retirer as RetireCommits does.
func (x *Index) Retire(ctx context.Context, now time.Time) (int, error) {
	retired, err := x.RetireCommits(ctx, now)

	return len(retired), err
}

// RetireCommits gives every retired commit past its window a tombstone,
// drops the rows of its set no other commit holds, and makes a closing
// set with nothing left deleted. An active set whose retention is paused
// is skipped; a deleted set proceeds regardless. It returns the commits
// it tombstoned, those before a failure included.
func (x *Index) RetireCommits(ctx context.Context, now time.Time) ([]index.Retired, error) {
	now = now.UTC()

	ctx, span := tracer.Start(ctx, "Index.Retire")
	defer span.End()

	phases := newPhases()
	tombstoned := 0

	defer func() {
		if tombstoned > 0 {
			phases.log(x.logger(), "retired commits", "commits", tombstoned)
		}
	}()

	phase := progress.Start(ctx, progress.OpRetire, progress.PhaseDue, 0, 0)

	settings, err := loadSettings(ctx, x.client)
	if err != nil {
		return nil, err
	}

	if err := x.client.Settings.UpdateOneID(settingsID).SetRetiredAt(now).Exec(ctx); err != nil {
		return nil, err
	}

	// a clock that jumped ahead would retire what the policies keep, so a
	// run far past the last one trusts its now for nothing
	if last := settings.RetiredAt; last != nil && now.Sub(*last) > retireRunGap {
		x.logger().Warn("retirement skipped: now is implausibly far past the last run", "now", now, "last", *last)
		phase.Finish()

		return nil, nil
	}

	// commits age out of a policy without anything happening to their set,
	// so a set that stopped committing is evaluated here or never
	active, err := x.client.Set.Query().Where(set.StateEQ(set.StateActive), set.RetentionPaused(false)).IDs(ctx)
	if err != nil {
		return nil, err
	}

	for _, id := range active {
		newest, err := newestCommit(ctx, x.client, id)
		if err != nil {
			return nil, err
		}

		if newest != nil && now.Sub(newest.ReceivedAt) > retireReceiptGap {
			x.logger().Warn("retirement left a set unevaluated: now is implausibly far past its newest commit",
				"set", id, "now", now, "newest", newest.ReceivedAt)
			continue
		}

		if err := x.reevaluateSetAt(ctx, id, now); err != nil {
			return nil, err
		}
	}

	phases.done("evaluate")

	held, err := x.restoreLeases(ctx)
	if err != nil {
		return nil, err
	}

	due, err := x.client.CommitRow.Query().
		Where(commitrow.TombstonedAtIsNil(), commitrow.ExpiresAtNotNil(), commitrow.ExpiresAtLTE(now),
			commitrow.HasSetWith(set.Or(set.RetentionPaused(false), set.StateNEQ(set.StateActive)))).
		WithSet().All(ctx)
	if err != nil {
		return nil, err
	}

	phases.done("due")

	pinned, err := x.storedPins(ctx, due)
	if err != nil {
		return nil, err
	}

	phases.done("pins")

	var eligible []*ent.CommitRow

	for _, c := range due {
		if !held[string(c.Ref)] && !pinned[string(c.Ref)] {
			eligible = append(eligible, c)
		}
	}

	phase.Finish()
	phase = progress.Start(ctx, progress.OpRetire, progress.PhaseTombstones, int64(len(eligible)), 0)

	written, err := x.writeTombstones(ctx, eligible, now, phase)
	if err != nil {
		return nil, err
	}

	tombstoned = len(written)
	phases.done("tombstones")
	span.SetAttributes(attribute.Int("due", len(due)), attribute.Int("tombstoned", len(written)))

	// rows say tombstoned only once the tombstones are durable, so a crash
	// before this point leaves commits that the next run tombstones again
	if len(written) > 0 {
		if err := x.flush(); err != nil {
			return nil, err
		}
	}

	phases.done("flush")
	phase.Finish()
	phase = progress.Start(ctx, progress.OpRetire, progress.PhaseRows, int64(len(written)), 0)

	var retired []index.Retired
	sets := make(map[int64][][]byte)
	names := make(map[int64]string)

	for _, c := range written {
		sets[c.SetID] = append(sets[c.SetID], c.Ref)
		names[c.SetID] = c.Edges.Set.Name
	}

	for setID, refs := range sets {
		for start := 0; start < len(refs); start += retireBatch {
			chunk := refs[start:min(start+retireBatch, len(refs))]

			marked, err := x.markTombstoned(ctx, setID, chunk, now)
			if err != nil {
				return retired, err
			}

			phase.Add(int64(len(chunk)), 0)

			for _, row := range marked {
				r, err := retiredOf(row, names[setID])
				if err != nil {
					return retired, err
				}

				r.TombstonedAt, r.State = now, index.RetiredHeld
				retired = append(retired, r)
			}
		}
	}

	phases.done("mark")
	phase.Finish()

	// the sets this run tombstoned in, and those a failed prune left
	pending, err := x.client.Set.Query().Where(set.PrunePendingSinceNotNil(), set.StateNEQ(set.StateDeleted)).All(ctx)
	if err != nil {
		return retired, err
	}

	phase = progress.Start(ctx, progress.OpRetire, progress.PhasePrune, int64(len(pending)), 0)

	// one set's failure leaves the others to prune
	var (
		prunes    errgroup.Group
		mu        sync.Mutex
		pruneErrs []error
	)

	prunes.SetLimit(pruneWorkers)

	for _, s := range pending {
		prunes.Go(func() error {
			defer phase.Add(1, 0)

			if err := x.prunePending(ctx, s.ID, *s.PrunePendingSince); err != nil {
				mu.Lock()
				pruneErrs = append(pruneErrs, fmt.Errorf("pruning set %s: %w", s.Name, err))
				mu.Unlock()
			}

			return nil
		})
	}

	_ = prunes.Wait()

	phases.done("prune")
	phase.Finish()

	return retired, errors.Join(append(pruneErrs, x.closeEmptySets(ctx))...)
}

// RetiredCommits reports the retirement of each of the commits whose set
// the caller sees, in the order given, leaving out the others; a commit
// live now, unretired or never retired, is RetiredLive.
func (x *Index) RetiredCommits(ctx context.Context, refs []*proto.Ref) ([]index.Retired, error) {
	hashes := make([][]byte, len(refs))
	for i, ref := range refs {
		hashes[i] = ref.GetHash()
	}

	var rows []*ent.CommitRow

	err := inBatches(hashes, func(batch [][]byte) error {
		found, err := x.client.CommitRow.Query().Where(commitrow.RefIn(batch...), commitrow.HasSetWith()).WithSet().All(ctx)
		rows = append(rows, found...)

		return err
	})
	if err != nil {
		return nil, err
	}

	byRef := make(map[string]*ent.CommitRow, len(rows))
	var tombstoned []*proto.Ref

	for _, row := range rows {
		byRef[string(row.Ref)] = row

		if row.TombstonedAt != nil {
			tombstoned = append(tombstoned, &proto.Ref{Hash: row.Ref})
		}
	}

	held := make(map[string]bool, len(tombstoned))

	if reviver, ok := storeAs[backup.Reviver](x.ObjectStore); ok && len(tombstoned) > 0 {
		holds, err := reviver.Holds(ctx, tombstoned)
		if err != nil {
			return nil, err
		}

		for i, ref := range tombstoned {
			held[string(ref.Hash)] = holds[i]
		}
	}

	var retired []index.Retired

	for _, hash := range hashes {
		row := byRef[string(hash)]
		if row == nil {
			continue
		}

		r, err := retiredOf(row, row.Edges.Set.Name)
		if err != nil {
			return nil, err
		}

		switch {
		case row.TombstonedAt != nil && held[string(hash)]:
			r.State = index.RetiredHeld
		case row.TombstonedAt != nil:
			r.State = index.RetiredGone
		case row.RetireAt != nil || row.DeletedAt != nil:
			r.State = index.RetiredPending
		default:
			r.State = index.RetiredLive
		}

		retired = append(retired, r)
	}

	return retired, nil
}

// retiredOf is the retirement a commit row records.
func retiredOf(row *ent.CommitRow, setName string) (index.Retired, error) {
	r := index.Retired{
		Ref:        &proto.Ref{Hash: row.Ref},
		SetID:      row.SetID,
		Set:        setName,
		Timestamp:  row.Timestamp,
		ReceivedAt: row.ReceivedAt,
		Deleted:    row.DeletedAt != nil,
		Partial:    row.Partial,
	}

	if row.RetireAt != nil {
		r.RetiredAt = *row.RetireAt
	}

	if row.TombstonedAt != nil {
		r.TombstonedAt = *row.TombstonedAt
	}

	if row.RetirePolicy != nil {
		p, err := retention.Parse([]byte(*row.RetirePolicy))
		if err != nil {
			return index.Retired{}, fmt.Errorf("commit %x: %w", row.Ref, err)
		}

		r.Policy = &p
	}

	return r, nil
}

// storedPins indexes the pins in the store the index lacks, unpinned ones
// left out, and returns the commits of active sets due for their tombstone
// that a stored pin holds. The index may have missed a pin, restored from
// a copy older than the store, and what the store holds is what counts.
// Archives walked_archives names are not read again: their pins were
// indexed before the row was written, so a copy of the index that has the
// row has the pins too.
func (x *Index) storedPins(ctx context.Context, due []*ent.CommitRow) (map[string]bool, error) {
	targets := make(map[string]bool, len(due))
	for _, c := range due {
		if c.Edges.Set.State == set.StateActive {
			targets[string(c.Ref)] = true
		}
	}

	if len(targets) == 0 {
		return nil, nil
	}

	var found []*proto.Object

	collect := func(obj *proto.Object) error {
		found = append(found, obj)
		return nil
	}

	walker, incremental := storeAs[backup.ArchiveWalker](x.ObjectStore)

	var (
		known  map[string]bool
		walked []string
		err    error
	)

	if incremental {
		known, err = x.walkedArchives(ctx)
		if err != nil {
			return nil, err
		}

		walked, err = walker.WalkArchives(ctx, proto.ObjectType_PIN, func(name string) bool { return known[name] }, collect)
	} else {
		err = x.ObjectStore.Walk(ctx, true, proto.ObjectType_PIN, collect)
	}

	if errors.Is(err, backup.ErrNotImplemented) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	pinned, err := x.indexStoredPins(ctx, found, targets)
	if err != nil || !incremental {
		return pinned, err
	}

	return pinned, x.recordWalked(ctx, known, walked)
}

// indexStoredPins indexes those of pins the index lacks or that hold one
// of targets, unless the index or the store has them unpinned, and returns
// the targets they hold.
func (x *Index) indexStoredPins(ctx context.Context, pins []*proto.Object, targets map[string]bool) (map[string]bool, error) {
	pinned := map[string]bool{}

	for start := 0; start < len(pins); start += retireBatch {
		chunk := pins[start:min(start+retireBatch, len(pins))]

		hashes := make([][]byte, len(chunk))
		for i, obj := range chunk {
			hashes[i] = obj.Ref().Hash
		}

		rows, err := x.client.Pin.Query().Where(pin.RefIn(hashes...)).Select(pin.FieldRef, pin.FieldDeletedAt).All(ctx)
		if err != nil {
			return nil, err
		}

		indexed := make(map[string]bool, len(rows))
		unpinned := make(map[string]bool)

		for _, row := range rows {
			indexed[string(row.Ref)] = true
			unpinned[string(row.Ref)] = row.DeletedAt != nil
		}

		deleted, err := x.client.DeletedRef.Query().Where(deletedref.RefIn(hashes...)).Select(deletedref.FieldRef).All(ctx)
		if err != nil {
			return nil, err
		}

		for _, row := range deleted {
			unpinned[string(row.Ref)] = true
		}

		var (
			candidates []*proto.Object
			refs       []*proto.Ref
		)

		for _, obj := range chunk {
			ref := obj.Ref()
			target := obj.GetPin().GetTarget().GetHash()

			if unpinned[string(ref.Hash)] || (indexed[string(ref.Hash)] && !targets[string(target)]) {
				continue
			}

			candidates = append(candidates, obj)
			refs = append(refs, ref)
		}

		if len(candidates) == 0 {
			continue
		}

		// an unpin's tombstone outranks the pin wherever it is stored
		present, err := backup.HasAll(ctx, x.ObjectStore, refs)
		if err != nil {
			return nil, err
		}

		for i, obj := range candidates {
			if !present[i] {
				continue
			}

			target := obj.GetPin().GetTarget().GetHash()
			if targets[string(target)] {
				pinned[string(target)] = true
			}

			if err := x.indexPin(ctx, obj.GetPin(), refs[i], false); err != nil {
				return nil, err
			}
		}
	}

	return pinned, nil
}

// walkedArchives returns the names walked_archives holds.
func (x *Index) walkedArchives(ctx context.Context) (map[string]bool, error) {
	names, err := x.client.WalkedArchive.Query().IDs(ctx)
	if err != nil {
		return nil, err
	}

	known := make(map[string]bool, len(names))
	for _, name := range names {
		known[name] = true
	}

	return known, nil
}

// recordWalked makes walked_archives name the archives of walked, given
// known, what it named before.
func (x *Index) recordWalked(ctx context.Context, known map[string]bool, walked []string) error {
	current := make(map[string]bool, len(walked))

	var added []string

	for _, name := range walked {
		current[name] = true

		if !known[name] {
			added = append(added, name)
		}
	}

	var gone []string

	for name := range known {
		if !current[name] {
			gone = append(gone, name)
		}
	}

	for start := 0; start < len(added); start += retireBatch {
		chunk := added[start:min(start+retireBatch, len(added))]

		creates := make([]*ent.WalkedArchiveCreate, len(chunk))
		for i, name := range chunk {
			creates[i] = x.client.WalkedArchive.Create().SetID(name)
		}

		err := ignoreNoRows(x.client.WalkedArchive.CreateBulk(creates...).OnConflict().DoNothing().Exec(ctx))
		if err != nil {
			return err
		}
	}

	for start := 0; start < len(gone); start += retireBatch {
		_, err := x.client.WalkedArchive.Delete().Where(walkedarchive.IDIn(gone[start:min(start+retireBatch, len(gone))]...)).Exec(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// restoreLeases returns the refs live restore sessions hold.
func (x *Index) restoreLeases(ctx context.Context) (map[string]bool, error) {
	leaser, ok := storeAs[backup.RestoreLeaser](x.ObjectStore)
	if !ok {
		return nil, nil
	}

	refs, err := leaser.RestoreLeases(ctx)
	if err != nil {
		return nil, err
	}

	held := make(map[string]bool, len(refs))
	for _, ref := range refs {
		held[string(ref.GetHash())] = true
	}

	return held, nil
}

// retireBatch is how many commits one retiring transaction takes.
const retireBatch = 500

// writeTombstones claims retired commits in deleted_refs under their row
// locks, passing over those a pin, an undelete or another run got to
// first, then writes their tombstones, and returns the commits it wrote.
// A claimed commit whose tombstone failed is written by the next run.
func (x *Index) writeTombstones(ctx context.Context, due []*ent.CommitRow, now time.Time, phase *progress.Phase) ([]*ent.CommitRow, error) {
	_, canErase := storeAs[backup.Eraser](x.ObjectStore)
	for _, c := range due {
		if c.Edges.Set.Erase && !canErase {
			return nil, fmt.Errorf("store %T cannot erase commit %x", x.ObjectStore, c.Ref)
		}
	}

	var written []*ent.CommitRow

	for start := 0; start < len(due); start += retireBatch {
		chunk := due[start:min(start+retireBatch, len(due))]

		refs := make([][]byte, len(chunk))
		for i, c := range chunk {
			refs[i] = c.Ref
		}

		var done []*ent.CommitRow

		err := x.tx(ctx, func(tx *ent.Tx) error {
			done = nil

			// row order, so that two runs lock in the same order
			rows, err := forUpdate(x, tx.CommitRow.Query().Where(commitrow.RefIn(refs...)).Order(ent.Asc(commitrow.FieldID))).All(ctx)
			if err != nil {
				return err
			}

			current := make(map[string]*ent.CommitRow, len(rows))
			for _, row := range rows {
				current[string(row.Ref)] = row
			}

			pins, err := tx.Pin.Query().Where(pin.TargetIn(refs...), pin.DeletedAtIsNil()).Select(pin.FieldTarget).All(ctx)
			if err != nil {
				return err
			}

			pinned := make(map[string]bool, len(pins))
			for _, p := range pins {
				pinned[string(p.Target)] = true
			}

			var deleted [][]byte

			for _, c := range chunk {
				row := current[string(c.Ref)]
				if row == nil || row.TombstonedAt != nil || pinned[string(c.Ref)] || row.ExpiresAt == nil || row.ExpiresAt.After(now) {
					continue
				}

				deleted = append(deleted, c.Ref)
				done = append(done, c)
			}

			// deleted_refs names the commits from here on, so no undelete or
			// pin slips in while the tombstones are on their way to the
			// archives
			return recordDeleted(ctx, tx.Client(), deleted, now)
		})
		if err != nil {
			return nil, err
		}

		// a tombstone goes out only for a commit the database has given up
		grp, gctx := errgroup.WithContext(ctx)
		grp.SetLimit(diffWorkers)

		for _, c := range done {
			grp.Go(func() error {
				return x.tombstone(gctx, c.Ref, c.Edges.Set.Erase)
			})
		}

		if err := grp.Wait(); err != nil {
			return nil, err
		}

		written = append(written, done...)
		phase.Add(int64(len(chunk)), 0)
	}

	return written, nil
}

// tombstone writes the commit's tombstone, erasing it when its set says
// so.
func (x *Index) tombstone(ctx context.Context, ref []byte, erase bool) error {
	if !erase {
		return x.ObjectStore.Delete(ctx, &proto.Ref{Hash: ref})
	}

	eraser, ok := storeAs[backup.Eraser](x.ObjectStore)
	if !ok {
		return fmt.Errorf("store %T cannot erase commit %x", x.ObjectStore, ref)
	}

	return eraser.Erase(ctx, &proto.Ref{Hash: ref})
}

// markTombstoned records durable tombstones on the rows of the set's
// commits and drops the commit refs from what the store's sets reach.
func (x *Index) markTombstoned(ctx context.Context, setID int64, refs [][]byte, now time.Time) ([]*ent.CommitRow, error) {
	var rows []*ent.CommitRow

	err := x.tx(ctx, func(tx *ent.Tx) error {
		if _, err := x.lockSet(ctx, tx, setID); err != nil {
			return err
		}

		var err error

		rows, err = forUpdate(x, tx.CommitRow.Query().
			Where(commitrow.RefIn(refs...), commitrow.SetID(setID), commitrow.TombstonedAtIsNil()).
			Order(ent.Asc(commitrow.FieldID))).All(ctx)
		if err != nil {
			return err
		}

		if len(rows) == 0 {
			return nil
		}

		marked := make([][]byte, len(rows))
		for i, row := range rows {
			marked[i] = row.Ref
		}

		err = tx.CommitRow.Update().Where(commitrow.RefIn(marked...)).SetTombstonedAt(now).ClearPresence().Exec(ctx)
		if err != nil {
			return err
		}

		if err := recordDeleted(ctx, tx.Client(), marked, now); err != nil {
			return err
		}

		err = tx.Set.Update().Where(set.ID(setID), set.PrunePendingSinceIsNil()).SetPrunePendingSince(now).Exec(ctx)
		if err != nil {
			return err
		}

		_, err = tx.SetRef.Delete().Where(setref.RefIn(marked...)).Exec(ctx)

		return err
	})
	if err != nil {
		return nil, err
	}

	return rows, nil
}

// recordDeleted adds refs to deleted_refs, the durable set of refs a
// tombstone names.
func recordDeleted(ctx context.Context, c *ent.Client, refs [][]byte, at time.Time) error {
	if len(refs) == 0 {
		return nil
	}

	rows := make([]*ent.DeletedRefCreate, len(refs))
	for i, ref := range refs {
		rows[i] = c.DeletedRef.Create().SetRef(ref).SetTombstonedAt(at)
	}

	return ignoreNoRows(c.DeletedRef.CreateBulk(rows...).OnConflict().DoNothing().Exec(ctx))
}

// isDeleted reports whether a tombstone names the ref.
func isDeleted(ctx context.Context, c *ent.Client, ref []byte) (bool, error) {
	return c.DeletedRef.Query().Where(deletedref.Ref(ref)).Exist(ctx)
}

// prunePending prunes the refs and rows of the set's commits tombstoned
// since the prune pending since, then clears that, or moves it on to the
// oldest commit tombstoned meanwhile that the prune did not cover.
func (x *Index) prunePending(ctx context.Context, setID int64, since time.Time) error {
	rows, err := x.client.CommitRow.Query().Where(commitrow.SetID(setID), commitrow.TombstonedAtGTE(since)).
		Select(commitrow.FieldRef).All(ctx)
	if err != nil {
		return err
	}

	refs := make([][]byte, len(rows))
	covered := make(map[string]bool, len(rows))

	for i, row := range rows {
		refs[i] = row.Ref
		covered[string(row.Ref)] = true
	}

	if err := x.pruneRefs(ctx, setID, refs); err != nil {
		return err
	}

	if err := x.dropDeadRows(ctx, setID); err != nil {
		return err
	}

	return x.tx(ctx, func(tx *ent.Tx) error {
		if _, err := x.lockSet(ctx, tx, setID); err != nil {
			return err
		}

		rows, err := tx.CommitRow.Query().Where(commitrow.SetID(setID), commitrow.TombstonedAtGTE(since)).
			Select(commitrow.FieldRef, commitrow.FieldTombstonedAt).All(ctx)
		if err != nil {
			return err
		}

		var next *time.Time

		for _, row := range rows {
			if !covered[string(row.Ref)] && (next == nil || row.TombstonedAt.Before(*next)) {
				next = row.TombstonedAt
			}
		}

		if next == nil {
			return tx.Set.UpdateOneID(setID).ClearPrunePendingSince().Exec(ctx)
		}

		return tx.Set.UpdateOneID(setID).SetPrunePendingSince(*next).Exec(ctx)
	})
}

// dropDeadRows deletes the set's files and trees rows whose validity range
// holds no commit without a tombstone, under the set's lock, so that no
// unretirement reads them in between.
func (x *Index) dropDeadRows(ctx context.Context, setID int64) error {
	untombstoned := func(c *entsql.SelectTable) *entsql.Predicate {
		return entsql.IsNull(c.C(commitrow.FieldTombstonedAt))
	}

	return x.tx(ctx, func(tx *ent.Tx) error {
		if _, err := x.lockSet(ctx, tx, setID); err != nil {
			return err
		}

		_, err := tx.File.Delete().Where(file.SetID(setID), predicate.File(entsql.NotPredicates(heldByCommit(untombstoned)))).Exec(ctx)
		if err != nil {
			return err
		}

		_, err = tx.Tree.Delete().Where(tree.SetID(setID), predicate.Tree(entsql.NotPredicates(heldByCommit(untombstoned)))).Exec(ctx)

		return err
	})
}

func (x *Index) closeEmptySets(ctx context.Context) error {
	noLiveCommit := func(s *entsql.Selector) {
		c := entsql.Table(commitrow.Table)
		s.Where(entsql.Not(entsql.Exists(entsql.Select().From(c).Where(entsql.And(
			entsql.ColumnsEQ(c.C(commitrow.FieldSetID), s.C(set.FieldID)),
			entsql.IsNull(c.C(commitrow.FieldTombstonedAt)),
		)))))
	}

	ids, err := x.client.Set.Query().Where(set.StateEQ(set.StateClosing), predicate.Set(noLiveCommit)).IDs(ctx)
	if err != nil {
		return err
	}

	for _, id := range ids {
		err := x.tx(ctx, func(tx *ent.Tx) error {
			s, err := x.lockSet(ctx, tx, id)
			if err != nil {
				return err
			}

			live, err := tx.CommitRow.Query().Where(commitrow.SetID(id), commitrow.TombstonedAtIsNil()).Exist(ctx)
			if err != nil || live || s.State != set.StateClosing {
				return err
			}

			if _, err := tx.File.Delete().Where(file.SetID(id)).Exec(ctx); err != nil {
				return err
			}

			if _, err := tx.Tree.Delete().Where(tree.SetID(id)).Exec(ctx); err != nil {
				return err
			}

			if _, err := tx.SetRef.Delete().Where(setref.SetID(id)).Exec(ctx); err != nil {
				return err
			}

			return tx.Set.UpdateOneID(id).SetState(set.StateDeleted).ClearPrunePendingSince().Exec(ctx)
		})
		if err != nil {
			return err
		}

		x.logger().Info("deleted empty set", "set", id)
	}

	return nil
}

// pruneWorkers is how many sets RetireCommits prunes at once.
const pruneWorkers = 4

// retireRunGap and retireReceiptGap are how far now may lie past the last
// retirement run, and past a set's newest commit, before retirement takes
// it for a clock that jumped and leaves the store, or the set, as it is.
const (
	retireRunGap     = 30 * 24 * time.Hour
	retireReceiptGap = 400 * 24 * time.Hour
)
