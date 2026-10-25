package sql

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/index/sql/ent"
	"github.com/gobackio/goback/index/sql/ent/commitrow"
	"github.com/gobackio/goback/index/sql/ent/set"
	"github.com/gobackio/goback/index/sql/ent/settings"
	"github.com/gobackio/goback/proto"
)

// policyTx runs change, which returns the state of the scope it changed,
// and stores that state as a policy object numbered after the store's
// last. The number is taken under the settings row's lock, so policies
// replay in the order their changes committed.
func (x *Index) policyTx(ctx context.Context, change func(tx *ent.Tx) (*proto.Policy, error)) error {
	var p *proto.Policy

	err := x.tx(ctx, func(tx *ent.Tx) error {
		if _, err := loadSettings(ctx, tx.Client()); err != nil {
			return err
		}

		s, err := forUpdate(x, tx.Settings.Query().Where(settings.ID(settingsID))).Only(ctx)
		if err != nil {
			return err
		}

		p, err = change(tx)
		if err != nil {
			return err
		}

		p.Sequence = s.PolicySequence + 1
		p.WrittenAtNs = x.now().UnixNano()

		return tx.Settings.UpdateOneID(settingsID).SetPolicySequence(p.Sequence).Exec(ctx)
	})
	if err != nil {
		return err
	}

	// the object goes out once the change has committed: a SQLite index
	// holds its only writer for the whole transaction, and the store may
	// record the object in this same database
	err = x.ObjectStore.Put(backup.WithSession(ctx, nil), proto.NewObject(p))
	if err == nil {
		err = x.flush()
	}

	if err != nil {
		return fmt.Errorf("the change is made, but a rebuild would not restore it: %w", err)
	}

	return nil
}

func storeScope(s *ent.Settings) *proto.Policy {
	return &proto.Policy{Scope: &proto.Policy_Store{Store: m.StoreScope(s)}}
}

// setScope is the set's state; closedAt is when a closing set was
// deleted, zero otherwise.
func setScope(s *ent.Set, closedAt time.Time) *proto.Policy {
	scope := m.SetScope(s)
	if !closedAt.IsZero() {
		scope.ClosedAtNs = closedAt.UnixNano()
	}

	return &proto.Policy{Scope: &proto.Policy_Set{Set: scope}}
}

// commitScope is whether the commit is in the trash: since deletedAt, or
// not when it is zero.
func commitScope(ref []byte, deletedAt time.Time) *proto.Policy {
	scope := &proto.CommitScope{Commit: &proto.Ref{Hash: ref}}
	if !deletedAt.IsZero() {
		scope.DeletedAtNs = deletedAt.UnixNano()
	}

	return &proto.Policy{Scope: &proto.Policy_Commit{Commit: scope}}
}

// replayPolicies applies the policies the store holds beyond what this
// database has seen, in the order they were written. A fresh database
// sees every change an operator made to the store's settings, its sets
// and its trash again.
func (x *Index) replayPolicies(ctx context.Context) error {
	s, err := loadSettings(ctx, x.client)
	if err != nil {
		return err
	}

	var policies []*proto.Policy

	err = x.ObjectStore.Walk(ctx, true, proto.ObjectType_POLICY, func(obj *proto.Object) error {
		if p := obj.GetPolicy(); p.GetSequence() > s.PolicySequence {
			policies = append(policies, p)
		}

		return nil
	})
	if err != nil {
		return err
	}

	sort.Slice(policies, func(i, j int) bool {
		a, b := policies[i], policies[j]
		if a.Sequence != b.Sequence {
			return a.Sequence < b.Sequence
		}

		return a.WrittenAtNs < b.WrittenAtNs
	})

	for _, p := range policies {
		err := x.tx(ctx, func(tx *ent.Tx) error {
			if err := x.applyPolicy(ctx, tx, p); err != nil {
				return err
			}

			return tx.Settings.UpdateOneID(settingsID).SetPolicySequence(p.Sequence).Exec(ctx)
		})
		if err != nil {
			return fmt.Errorf("policy %d: %w", p.Sequence, err)
		}
	}

	return nil
}

func (x *Index) applyPolicy(ctx context.Context, tx *ent.Tx, p *proto.Policy) error {
	switch scope := p.Scope.(type) {
	case *proto.Policy_Store:
		return applyStoreScope(ctx, tx, scope.Store)
	case *proto.Policy_Set:
		return x.applySetScope(ctx, tx, scope.Set)
	case *proto.Policy_Commit:
		return x.applyCommitScope(ctx, tx, scope.Commit)
	}

	return nil
}

func applyStoreScope(ctx context.Context, tx *ent.Tx, s *proto.StoreScope) error {
	update := tx.Settings.UpdateOneID(settingsID).
		SetNillablePolicy(nilIfZero(s.WritePolicy)).
		SetPolicyVersion(s.WritePolicyVersion).
		SetNillableRetentionPolicy(nilIfZero(s.DefaultRetention)).
		SetTrashDays(int(s.TrashDays))

	if s.WritePolicy == "" {
		update.ClearPolicy()
	}

	if s.DefaultRetention == "" {
		update.ClearRetentionPolicy()
	}

	if s.KeyAcknowledgedAtNs == 0 {
		update.ClearKeyAcknowledgedAt()
	} else {
		update.SetKeyAcknowledgedAt(time.Unix(0, s.KeyAcknowledgedAtNs).UTC())
	}

	return update.Exec(ctx)
}

// applySetScope brings a set to the recorded state, closing or reopening
// it the way DeleteSet and UndeleteSet do when its state changes.
func (x *Index) applySetScope(ctx context.Context, tx *ent.Tx, s *proto.SetScope) error {
	setID, err := ensureSet(ctx, tx.Client(), s.Name, int64(s.SetId))
	if err != nil {
		return err
	}

	current, err := tx.Set.Get(ctx, setID)
	if err != nil {
		return err
	}

	update := tx.Set.UpdateOneID(setID).
		SetNillableRetentionPolicy(nilIfZero(s.Retention)).
		SetRetentionPaused(s.RetentionPaused)
	if s.Retention == "" {
		update.ClearRetentionPolicy()
	}

	if err := update.Exec(ctx); err != nil {
		return err
	}

	switch {
	case s.State == proto.SetState_SET_CLOSING && current.State == set.StateActive:
		cfg, err := x.loadSetConfig(ctx, tx.Client(), setID)
		if err != nil {
			return err
		}

		_, err = closeSet(ctx, tx, setID, cfg.trash, time.Unix(0, s.ClosedAtNs).UTC(), s.Erase)

		return err
	case s.State == proto.SetState_SET_ACTIVE && current.State == set.StateClosing:
		_, err := reopenSet(ctx, tx, setID)
		return err
	}

	return nil
}

// applyCommitScope moves a commit into or out of the trash; a commit the
// rebuild did not bring back is left alone.
func (x *Index) applyCommitScope(ctx context.Context, tx *ent.Tx, s *proto.CommitScope) error {
	row, err := tx.CommitRow.Query().Where(commitrow.Ref(s.GetCommit().GetHash()), commitrow.TombstonedAtIsNil()).Only(ctx)
	if ent.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return err
	}

	if s.DeletedAtNs == 0 {
		return untrashCommit(ctx, tx, row.Ref)
	}

	cfg, err := x.loadSetConfig(ctx, tx.Client(), row.SetID)
	if err != nil {
		return err
	}

	return trashCommit(ctx, tx, row, cfg.trash, time.Unix(0, s.DeletedAtNs).UTC())
}
