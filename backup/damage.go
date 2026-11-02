package backup

import (
	"context"
	"fmt"

	"github.com/gobackio/goback/proto"
)

// FilePath is where a file object is stored in a set.
type FilePath struct {
	Ref   *proto.Ref
	SetID int64
	// Set is the set's name, for reporting; SetID is what identifies it.
	Set  string
	Path string
	// Open reports whether this is the version the set's newest commit
	// points at, which is the only one an agent can produce again.
	Open bool
}

// DamageIndex records what a store lost, so the next backup of a set reads
// those paths again instead of trusting that they are unchanged.
type DamageIndex interface {
	// PathsOfFiles returns where the given file objects are stored; a file
	// object no live version references any more yields nothing.
	PathsOfFiles(ctx context.Context, refs []*proto.Ref) ([]FilePath, error)
	// MarkDamaged records paths of the set a run must read again.
	MarkDamaged(ctx context.Context, setID int64, paths []string) error
	// MarkLost records versions no run can produce again, so a restore of
	// the commits holding them says so rather than failing.
	MarkLost(ctx context.Context, versions []FilePath) error
	// MarkRescan records that a whole set must be read again.
	MarkRescan(ctx context.Context, setID int64) error
}

// A DamageFinder is an index that checks the store still holds what its
// versions reference.
type DamageFinder interface {
	// FindDamage records what the indexed versions not yet marked lost
	// reference but the store no longer holds, as ReportDamage does, and
	// returns the report. It may run while the index takes writes.
	FindDamage(ctx context.Context) (*DamageReport, error)
}

// DamageReport is what ReportDamage made of a store's losses.
type DamageReport struct {
	// Paths are the paths marked for a re-read, by set id.
	Paths map[int64][]string
	// Rescan is the sets marked for a full re-read, because some loss
	// could not be placed at a path.
	Rescan []int64
	// Unplaced are the lost objects no live path holds.
	Unplaced []*proto.Ref
	// Lost are the closed versions nothing can produce again; the commits
	// holding them are damaged for that path.
	Lost []FilePath
}

// ReportDamage places a store's lost objects at the paths that held them
// and records those, so the next backup of each set reads them again. A
// loss it cannot place — a lost file or tree object, or a blob whose file
// object is gone too — marks its sets for a full re-read instead, which
// is the only way left to find it.
func ReportDamage(ctx context.Context, store ObjectStore, idx DamageIndex, sets []int64, lost []*proto.Ref) (*DamageReport, error) {
	report := &DamageReport{Paths: map[int64][]string{}}
	if len(lost) == 0 {
		return report, nil
	}

	missing := make(map[string]bool, len(lost))
	for _, ref := range lost {
		missing[string(ref.GetHash())] = true
	}

	// a lost object may be a file object itself, in which case the index
	// knows its path without any walk
	holders := append([]*proto.Ref(nil), lost...)
	covers := make(map[string][]string, len(lost))

	for _, ref := range lost {
		covers[string(ref.GetHash())] = []string{string(ref.GetHash())}
	}

	// a split file's parts are held by its split children, which no path
	// names; a loss in one is placed at the file that splits it
	splitOf := map[string]*proto.Ref{}

	err := store.Walk(ctx, true, proto.ObjectType_FILE, func(obj *proto.Object) error {
		for _, split := range obj.GetFile().GetSplits() {
			splitOf[string(split.GetHash())] = obj.Ref()
		}

		var held []string
		for _, part := range obj.GetFile().GetParts() {
			if missing[string(part.Ref.GetHash())] {
				held = append(held, string(part.Ref.GetHash()))
			}
		}

		if len(held) == 0 {
			return nil
		}

		key := string(obj.Ref().GetHash())
		holders = append(holders, obj.Ref())
		covers[key] = append(covers[key], held...)

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walking the file objects: %w", err)
	}

	for _, holder := range holders {
		held := covers[string(holder.GetHash())]

		for parent, ok := splitOf[string(holder.GetHash())]; ok; parent, ok = splitOf[string(parent.GetHash())] {
			key := string(parent.GetHash())
			if _, seen := covers[key]; !seen {
				holders = append(holders, parent)
			}

			covers[key] = append(covers[key], held...)
		}
	}

	placed, err := idx.PathsOfFiles(ctx, holders)
	if err != nil {
		return nil, err
	}

	for _, at := range placed {
		// only the version the newest commit points at is still on a
		// source somewhere; a closed one is gone for good
		if at.Open {
			report.Paths[at.SetID] = append(report.Paths[at.SetID], at.Path)
		}

		report.Lost = append(report.Lost, at)

		for _, held := range covers[string(at.Ref.GetHash())] {
			delete(missing, held)
		}
	}

	for set, paths := range report.Paths {
		err := idx.MarkDamaged(ctx, set, paths)
		if err != nil {
			return nil, err
		}
	}

	if len(report.Lost) > 0 {
		err = idx.MarkLost(ctx, report.Lost)
		if err != nil {
			return nil, err
		}
	}

	if len(missing) == 0 {
		return report, nil
	}

	for _, ref := range lost {
		if missing[string(ref.GetHash())] {
			report.Unplaced = append(report.Unplaced, ref)
		}
	}

	for _, set := range sets {
		err := idx.MarkRescan(ctx, set)
		if err != nil {
			return nil, err
		}

		report.Rescan = append(report.Rescan, set)
	}

	return report, nil
}
