package maintenance

import (
	"context"
	"errors"
	"fmt"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/index"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/pack"
	"github.com/gobackio/goback/storage/wrapped"
)

// Repairer is a store that rewrites the archives holding corrupt objects.
type Repairer interface {
	Repair(ctx context.Context) (*pack.RepairReport, error)
}

// SetLister is an index that names its sets.
type SetLister interface {
	ListSets(ctx context.Context) ([]index.SetInfo, error)
}

// Repair rewrites the damage out of store and records in idx where what
// it lost was held, as RecordDamage does. It fails with an error matching
// errors.ErrUnsupported when the store cannot be repaired.
func Repair(ctx context.Context, store backup.ObjectStore, idx backup.Index) (*pack.RepairReport, *backup.DamageReport, error) {
	repairer, ok := wrapped.Innermost(store).(Repairer)
	if !ok {
		return nil, nil, fmt.Errorf("store %T cannot be repaired: %w", wrapped.Innermost(store), errors.ErrUnsupported)
	}

	report, err := repairer.Repair(ctx)
	if err != nil {
		return nil, nil, err
	}

	damage, err := RecordDamage(ctx, idx, report.Lost)

	return report, damage, err
}

// RecordDamage places the lost objects at the paths of every set of idx
// that held them, so the next backup of each set reads them again; see
// backup.ReportDamage. It returns nil when idx records no damage.
func RecordDamage(ctx context.Context, idx backup.Index, lost []*proto.Ref) (*backup.DamageReport, error) {
	damage, ok := idx.(backup.DamageIndex)
	lister, listable := idx.(SetLister)

	if !ok || !listable {
		return nil, nil
	}

	sets, err := lister.ListSets(ctx)
	if err != nil {
		return nil, err
	}

	ids := make([]int64, len(sets))
	for i, s := range sets {
		ids[i] = s.ID
	}

	return backup.ReportDamage(ctx, idx, damage, ids, lost)
}
