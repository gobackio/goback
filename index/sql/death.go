package sql

import (
	"context"
	"time"

	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/index/sql/ent"
	"github.com/gobackio/goback/index/sql/ent/commitrow"
	"github.com/gobackio/goback/index/sql/ent/pin"
	"github.com/gobackio/goback/index/sql/ent/set"
	"github.com/gobackio/goback/storage/pack"
)

// RootDeath returns the lookup a garbage collection expects deaths with:
// it answers a commit that is deleted or retired with when its window
// ends, and a live commit of an active set with the first of
// pack.DeathHorizons past now by which the set's policy stops keeping it,
// were the set to go on committing at its retention.Cadence. A pin, a
// commit kept past the last horizon or one of a set that is closing or
// whose retention is paused, and anything else, answer zero: never.
func (x *Index) RootDeath(ctx context.Context, now time.Time) (func(root []byte) time.Time, error) {
	defaults, err := loadSettings(ctx, x.client)
	if err != nil {
		return nil, err
	}

	sets, err := x.client.Set.Query().Where(set.StateEQ(set.StateActive), set.RetentionPaused(false)).
		Select(set.FieldID, set.FieldRetentionPolicy).All(ctx)
	if err != nil {
		return nil, err
	}

	policies := make(map[int64]retention.Policy, len(sets))
	for _, s := range sets {
		policy, err := x.policyOf(defaults, s)
		if err != nil {
			x.logger().Warn("expecting no commit of a set whose policy is unreadable to die", "set", s.ID, "err", err)
			continue
		}

		policies[s.ID] = policy
	}

	rows, err := x.client.CommitRow.Query().Where(commitrow.TombstonedAtIsNil()).
		Select(commitrow.FieldRef, commitrow.FieldSetID, commitrow.FieldReceivedAt, commitrow.FieldPartial,
			commitrow.FieldIncomplete, commitrow.FieldDeletedAt, commitrow.FieldExpiresAt).All(ctx)
	if err != nil {
		return nil, err
	}

	pinned, err := x.client.Pin.Query().Where(pin.DeletedAtIsNil()).Select(pin.FieldTarget).Strings(ctx)
	if err != nil {
		return nil, err
	}

	held := make(map[string]bool, len(pinned))
	for _, target := range pinned {
		held[target] = true
	}

	at := make([]time.Time, len(pack.DeathHorizons))
	for i, horizon := range pack.DeathHorizons {
		at[i] = now.Add(horizon)
	}

	deaths := make(map[string]time.Time)
	bySet := make(map[int64][]*ent.CommitRow)

	for _, row := range rows {
		if row.ExpiresAt != nil {
			deaths[string(row.Ref)] = *row.ExpiresAt
		}

		if _, ok := policies[row.SetID]; ok && row.DeletedAt == nil {
			bySet[row.SetID] = append(bySet[row.SetID], row)
		}
	}

	for id, rows := range bySet {
		commits := make([]retention.Commit, len(rows))
		for i, row := range rows {
			commits[i] = retention.Commit{ReceivedAt: row.ReceivedAt, Partial: row.Partial, Incomplete: row.Incomplete, Pinned: held[string(row.Ref)]}
		}

		for i, n := range retention.Forecast(commits, policies[id], retention.Cadence(commits), at) {
			if n < len(at) && rows[i].ExpiresAt == nil {
				deaths[string(rows[i].Ref)] = at[n]
			}
		}
	}

	return func(root []byte) time.Time {
		return deaths[string(root)]
	}, nil
}
