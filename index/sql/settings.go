package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/index"
	"github.com/gobackio/goback/index/sql/ent"
	"github.com/gobackio/goback/proto"
)

// settingsID is the id of the one settings row.
const settingsID = 1

// loadSettings returns the settings row, creating it with the defaults on
// first use.
func loadSettings(ctx context.Context, c *ent.Client) (*ent.Settings, error) {
	err := ignoreNoRows(c.Settings.Create().SetID(settingsID).OnConflict().DoNothing().Exec(ctx))
	if err != nil {
		return nil, err
	}

	return c.Settings.Get(ctx, settingsID)
}

// GetStorePolicy returns the store's policy; a store that never had one
// set reports version 0 with the default parameters.
func (x *Index) GetStorePolicy(ctx context.Context) (index.StorePolicy, error) {
	p := index.StorePolicy{Policy: storekey.DefaultPolicy()}

	s, err := loadSettings(ctx, x.client)
	if err != nil {
		return p, err
	}

	if s.Policy != nil {
		err = json.Unmarshal([]byte(*s.Policy), &p.Policy)
		if err != nil {
			return p, fmt.Errorf("store policy: %w", err)
		}
	}

	p.Policy.Version = s.PolicyVersion
	p.KeyAcknowledgedAt = s.KeyAcknowledgedAt

	return p, nil
}

// SetStorePolicy stores policy under the next version and, when
// acknowledge is set, records that the operator has saved the store key.
func (x *Index) SetStorePolicy(ctx context.Context, policy storekey.Policy, acknowledge bool, now time.Time) (index.StorePolicy, error) {
	var acknowledged *time.Time

	err := x.policyTx(ctx, func(tx *ent.Tx) (*proto.Policy, error) {
		s, err := tx.Settings.Get(ctx, settingsID)
		if err != nil {
			return nil, err
		}

		if err := chunkSizeChange(ctx, tx, s, policy); err != nil {
			return nil, err
		}

		policy.Version = s.PolicyVersion + 1

		encoded, err := json.Marshal(policy)
		if err != nil {
			return nil, err
		}

		acknowledged = s.KeyAcknowledgedAt
		if acknowledge && acknowledged == nil {
			acknowledged = ptr(now.UTC())
		}

		s, err = tx.Settings.UpdateOneID(settingsID).SetPolicy(string(encoded)).SetPolicyVersion(policy.Version).SetNillableKeyAcknowledgedAt(acknowledged).Save(ctx)
		if err != nil {
			return nil, err
		}

		return storeScope(s), nil
	})
	if err != nil {
		return index.StorePolicy{}, err
	}

	return index.StorePolicy{Policy: policy, KeyAcknowledgedAt: acknowledged}, nil
}

// chunkSizeChange refuses a policy whose chunk size differs from the
// stored one while the store holds a commit.
func chunkSizeChange(ctx context.Context, tx *ent.Tx, s *ent.Settings, policy storekey.Policy) error {
	var stored storekey.Policy
	if s.Policy != nil {
		if err := json.Unmarshal([]byte(*s.Policy), &stored); err != nil {
			return fmt.Errorf("store policy: %w", err)
		}
	}

	if stored.ChunkSize == policy.ChunkSize {
		return nil
	}

	held, err := tx.CommitRow.Query().Exist(ctx)
	if err != nil {
		return err
	}

	if held {
		return fmt.Errorf("%w: it cuts chunks of %d bytes", index.ErrChunkSizeFixed, stored.ChunkSize)
	}

	return nil
}

// StorePolicy implements backup.PolicySource: nil until an operator set a
// policy.
func (x *Index) StorePolicy(ctx context.Context) (*storekey.Policy, error) {
	p, err := x.GetStorePolicy(ctx)
	if err != nil {
		return nil, err
	}

	if p.Policy.Version == 0 {
		return nil, nil
	}

	return &p.Policy, nil
}

// GetDefaultPolicy returns the retention policy sets inherit until they
// set their own, and whether the operator stored it or it is the
// built-in default.
func (x *Index) GetDefaultPolicy(ctx context.Context) (policy retention.Policy, stored bool, err error) {
	defaults, err := loadSettings(ctx, x.client)
	if err != nil {
		return retention.Policy{}, false, err
	}

	policy = retention.Default
	if x.DefaultPolicy != nil {
		policy = *x.DefaultPolicy
	}

	if defaults.RetentionPolicy != nil {
		policy, err = retention.Parse([]byte(*defaults.RetentionPolicy))
		if err != nil {
			return retention.Policy{}, false, err
		}

		stored = true
	}

	return policy, stored, nil
}

// SetDefaultPolicy stores the retention policy sets inherit until they
// set their own, nil for the built-in default, and re-evaluates every set.
func (x *Index) SetDefaultPolicy(ctx context.Context, p *retention.Policy) error {
	raw, err := x.encodePolicy(p)
	if err != nil {
		return err
	}

	err = x.policyTx(ctx, func(tx *ent.Tx) (*proto.Policy, error) {
		update := tx.Settings.UpdateOneID(settingsID)
		if raw == nil {
			update.ClearRetentionPolicy()
		} else {
			update.SetRetentionPolicy(*raw)
		}

		s, err := update.Save(ctx)
		if err != nil {
			return nil, err
		}

		return storeScope(s), nil
	})
	if err != nil {
		return err
	}

	return x.evaluateAll(ctx)
}

// Windows returns the store's trash window.
func (x *Index) Windows(ctx context.Context) (index.Windows, error) {
	defaults, err := loadSettings(ctx, x.client)
	if err != nil {
		return index.Windows{}, err
	}

	return index.Windows{TrashDays: defaults.TrashDays}, nil
}

// SetWindows stores the trash window and re-evaluates every set; a
// negative window is refused.
func (x *Index) SetWindows(ctx context.Context, w index.Windows) error {
	if w.TrashDays < 0 {
		return fmt.Errorf("%w: a window cannot be negative", retention.ErrInvalidPolicy)
	}

	err := x.policyTx(ctx, func(tx *ent.Tx) (*proto.Policy, error) {
		s, err := tx.Settings.UpdateOneID(settingsID).SetTrashDays(w.TrashDays).Save(ctx)
		if err != nil {
			return nil, err
		}

		return storeScope(s), nil
	})
	if err != nil {
		return err
	}

	return x.evaluateAll(ctx)
}

func (x *Index) evaluateAll(ctx context.Context) error {
	ids, err := x.client.Set.Query().IDs(ctx)
	if err != nil {
		return err
	}

	for _, id := range ids {
		if err := x.reevaluateSet(ctx, id); err != nil {
			return err
		}
	}

	return nil
}
