package sql

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/index"
	"github.com/gobackio/goback/index/sql/ent"
	"github.com/gobackio/goback/index/sql/ent/commitrow"
	"github.com/gobackio/goback/index/sql/ent/damagedpath"
	"github.com/gobackio/goback/index/sql/ent/file"
	"github.com/gobackio/goback/index/sql/ent/predicate"
	"github.com/gobackio/goback/index/sql/ent/set"
	"github.com/gobackio/goback/proto"

	"golang.org/x/sync/errgroup"
)

// PathsOfFiles returns where the given file objects are stored, by set
// and path. A file object no live version references any more yields
// nothing.
func (x *Index) PathsOfFiles(ctx context.Context, refs []*proto.Ref) ([]backup.FilePath, error) {
	raw := make([][]byte, 0, len(refs))
	for _, ref := range refs {
		raw = append(raw, ref.GetHash())
	}

	var rows []*ent.File

	err := inBatches(raw, func(batch [][]byte) error {
		found, err := x.client.File.Query().Where(file.RefIn(batch...)).All(ctx)
		rows = append(rows, found...)

		return err
	})
	if err != nil {
		return nil, err
	}

	slices.SortStableFunc(rows, func(a, b *ent.File) int { return strings.Compare(a.Path, b.Path) })

	ids := make([]int64, 0)
	names := make(map[int64]string)
	for _, row := range rows {
		if _, ok := names[row.SetID]; !ok {
			names[row.SetID] = ""
			ids = append(ids, row.SetID)
		}
	}

	err = inBatches(ids, func(batch []int64) error {
		sets, err := x.client.Set.Query().Where(set.IDIn(batch...)).Select(set.FieldName).All(ctx)
		for _, s := range sets {
			names[s.ID] = s.Name
		}

		return err
	})
	if err != nil {
		return nil, err
	}

	seen := make(map[string]bool)

	var out []backup.FilePath
	for _, row := range rows {
		name := names[row.SetID]

		key := fmt.Sprint(row.SetID) + "\x00" + row.Path + "\x00" + string(row.Ref)
		if seen[key] {
			continue
		}

		seen[key] = true
		path := m.FilePath(row)
		path.Set = name
		out = append(out, path)
	}

	return out, nil
}

// MarkDamaged records that the paths of a set hold content the store lost,
// so the next backup reads them again. An unknown set is ignored, since a
// set that is gone has nothing to repair.
func (x *Index) MarkDamaged(ctx context.Context, setID int64, paths []string) error {
	exists, err := x.client.Set.Query().Where(set.ID(setID)).Exist(ctx)
	if err != nil || !exists {
		return err
	}

	now := x.now()

	// one statement must not name a row twice
	seen := make(map[string]bool, len(paths))
	unique := make([]string, 0, len(paths))
	for _, path := range paths {
		if !seen[path] {
			seen[path] = true
			unique = append(unique, path)
		}
	}

	return inBatches(unique, func(batch []string) error {
		builders := make([]*ent.DamagedPathCreate, len(batch))
		for i, path := range batch {
			builders[i] = x.client.DamagedPath.Create().SetSetID(setID).SetPath(path).SetFoundAt(now)
		}

		err := x.client.DamagedPath.CreateBulk(builders...).OnConflictColumns(damagedpath.FieldSetID, damagedpath.FieldPath).
			UpdateFoundAt().Exec(ctx)
		if err != nil {
			return fmt.Errorf("marking %d paths of set %d damaged: %w", len(batch), setID, err)
		}

		return nil
	})
}

// MarkRescan records that a whole set must be read again, for damage no
// path could be resolved for.
func (x *Index) MarkRescan(ctx context.Context, setID int64) error {
	n, err := x.client.Set.Update().Where(set.ID(setID)).SetRescan(true).Save(ctx)
	if err != nil {
		return err
	}

	if n == 0 {
		return fmt.Errorf("%w: set %d", backup.ErrNotFound, setID)
	}

	return nil
}

// damage returns what a run on the set must read again regardless of what
// its change detection says.
func (x *Index) damage(ctx context.Context, s *ent.Set) (rescan bool, paths []string, err error) {
	if s.Rescan {
		return true, nil, nil
	}

	rows, err := x.client.DamagedPath.Query().Where(damagedpath.SetID(s.ID)).
		Order(ent.Asc(damagedpath.FieldPath)).All(ctx)
	if err != nil {
		return false, nil, err
	}

	for _, row := range rows {
		paths = append(paths, row.Path)
	}

	return false, paths, nil
}

// clearDamage forgets what a completed run has read again. A checkpoint
// leaves it, because it did not cover the whole set.
func (x *Index) clearDamage(ctx context.Context, tx *ent.Tx, setID int64, partial bool, scanStart int64) error {
	if partial {
		return nil
	}

	s, err := tx.Set.Get(ctx, setID)
	if err != nil {
		return err
	}

	if s.Rescan {
		err = tx.Set.UpdateOneID(setID).SetRescan(false).Exec(ctx)
		if err != nil {
			return err
		}
	}

	found := x.now()
	if scanStart > 0 {
		found = time.Unix(0, scanStart).UTC()
	}

	// damage found after this run started is not covered by it
	_, err = tx.DamagedPath.Delete().
		Where(damagedpath.SetID(setID), damagedpath.FoundAtLTE(found)).Exec(ctx)
	if err != nil {
		return err
	}

	// the run stored the open versions again, so what was lost of them is
	// back; a closed version keeps its mark, since nothing can produce it
	return tx.File.Update().
		Where(file.SetID(setID), file.Lost(true), file.ValidUntilIsNil()).
		SetLost(false).Exec(ctx)
}

// MarkLost records that stored versions cannot be read any more. Every
// commit whose range covers one holds a file it cannot restore.
func (x *Index) MarkLost(ctx context.Context, versions []backup.FilePath) error {
	bySet := make(map[int64][]predicate.File)
	var sets []int64

	for _, v := range versions {
		if bySet[v.SetID] == nil {
			sets = append(sets, v.SetID)
		}

		bySet[v.SetID] = append(bySet[v.SetID], file.And(file.Path(v.Path), file.RefEQ(v.Ref.GetHash())))
	}

	for _, setID := range sets {
		versions := bySet[setID]

		for start := 0; start < len(versions); start += lostBatch {
			batch := versions[start:min(start+lostBatch, len(versions))]

			err := x.client.File.Update().Where(file.SetID(setID), file.Or(batch...)).SetLost(true).Exec(ctx)
			if err != nil {
				return fmt.Errorf("marking versions of set %d lost: %w", setID, err)
			}
		}
	}

	return nil
}

// lostBatch is how many versions MarkLost names in one statement: SQLite
// nests each OR a level deeper, and parses at most 1000 levels.
const lostBatch = 200

// LostVersions returns the versions of a set the store cannot read and the
// commits each one damages. A restore of those commits cannot produce that
// path.
func (x *Index) LostVersions(ctx context.Context, name string) ([]index.LostVersion, error) {
	setID, err := findSet(ctx, x.client, name)
	if errors.Is(err, backup.ErrNotFound) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	rows, err := x.client.File.Query().
		Where(file.SetID(setID), file.Lost(true)).
		Order(ent.Asc(file.FieldPath), ent.Asc(file.FieldValidFrom)).All(ctx)
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return []index.LostVersion{}, nil
	}

	commits, err := x.client.CommitRow.Query().Where(commitrow.SetID(setID), liveCommit()).
		Order(ent.Desc(commitrow.FieldReceivedAt)).Select(commitrow.FieldRef, commitrow.FieldReceivedAt).All(ctx)
	if err != nil {
		return nil, err
	}

	out := make([]index.LostVersion, 0, len(rows))
	for _, row := range rows {
		lost := index.LostVersion{Path: row.Path, Ref: &proto.Ref{Hash: row.Ref}}
		for _, c := range commits {
			if !c.ReceivedAt.Before(row.ValidFrom) && (row.ValidUntil == nil || c.ReceivedAt.Before(*row.ValidUntil)) {
				lost.Commits = append(lost.Commits, &proto.Ref{Hash: c.Ref})
			}
		}

		out = append(out, lost)
	}

	return out, nil
}

// damageWorkers is how many file objects findDamage checks at once.
const damageWorkers = 16

// findDamage marks what the indexed versions reference but the store no
// longer holds, as the repair that lost it did in the index a rebuild
// replaces.
func (x *Index) findDamage(ctx context.Context) error {
	var rows []struct {
		Ref []byte `json:"ref"`
	}

	err := x.client.File.Query().Where(file.RefNotNil()).Unique(true).Select(file.FieldRef).Scan(ctx, &rows)
	if err != nil {
		return err
	}

	var (
		mtx  sync.Mutex
		seen = map[string]bool{}
		lost []*proto.Ref
	)

	// claim reports whether ref is new to this pass; versions of a file
	// share most of their parts, so each is looked at once
	claim := func(ref *proto.Ref) bool {
		mtx.Lock()
		defer mtx.Unlock()

		if seen[string(ref.GetHash())] {
			return false
		}

		seen[string(ref.GetHash())] = true

		return true
	}

	gone := func(ref *proto.Ref) {
		mtx.Lock()
		lost = append(lost, ref)
		mtx.Unlock()
	}

	var check func(ctx context.Context, ref *proto.Ref) error
	check = func(ctx context.Context, ref *proto.Ref) error {
		if !claim(ref) {
			return nil
		}

		obj, err := x.ObjectStore.Get(ctx, ref)
		if errors.Is(err, backup.ErrNotFound) {
			gone(ref)
			return nil
		}

		if err != nil {
			return err
		}

		for _, part := range obj.GetFile().GetParts() {
			if !claim(part.GetRef()) {
				continue
			}

			held, err := x.ObjectStore.Has(ctx, part.GetRef())
			if err != nil {
				return err
			}

			if !held {
				gone(part.GetRef())
			}
		}

		for _, split := range obj.GetFile().GetSplits() {
			if err := check(ctx, split); err != nil {
				return err
			}
		}

		return nil
	}

	group, gctx := errgroup.WithContext(ctx)
	group.SetLimit(damageWorkers)

	began := time.Now()

	for i, row := range rows {
		if len(row.Ref) == 0 {
			continue
		}

		if i > 0 && i%10000 == 0 {
			x.logger().Info("checking that the store holds what the versions reference", "files", i, "of", len(rows), "elapsed", time.Since(began).Round(time.Second))
		}

		group.Go(func() error {
			return check(gctx, &proto.Ref{Hash: row.Ref})
		})
	}

	if err := group.Wait(); err != nil {
		return err
	}

	if len(lost) == 0 {
		return nil
	}

	sets, err := x.client.Set.Query().IDs(ctx)
	if err != nil {
		return err
	}

	report, err := backup.ReportDamage(ctx, x.ObjectStore, x, sets, lost)
	if err != nil {
		return err
	}

	x.logger().Warn("the store lost objects its versions reference", "objects", len(lost),
		"damaged sets", len(report.Paths), "unrecoverable versions", len(report.Lost), "sets to read again", len(report.Rescan))

	return nil
}
