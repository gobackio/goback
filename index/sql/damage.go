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
	"github.com/gobackio/goback/progress"
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
	// one statement must not name a row twice
	seen := make(map[string]bool, len(paths))
	unique := make([]string, 0, len(paths))
	for _, path := range paths {
		if !seen[path] {
			seen[path] = true
			unique = append(unique, path)
		}
	}

	return x.tx(ctx, func(tx *ent.Tx) error {
		s, err := forUpdate(x, tx.Set.Query().Where(set.ID(setID))).Only(ctx)
		if ent.IsNotFound(err) {
			return nil
		}

		if err != nil {
			return err
		}

		// whatever the clocks say, damage found after a grant is newer than it
		found := x.now()
		if s.GrantedAt != nil && !found.After(*s.GrantedAt) {
			found = s.GrantedAt.Add(time.Millisecond)
		}

		return inBatches(unique, func(batch []string) error {
			builders := make([]*ent.DamagedPathCreate, len(batch))
			for i, path := range batch {
				builders[i] = tx.DamagedPath.Create().SetSetID(setID).SetPath(path).SetFoundAt(found)
			}

			err := tx.DamagedPath.CreateBulk(builders...).OnConflictColumns(damagedpath.FieldSetID, damagedpath.FieldPath).
				UpdateFoundAt().Exec(ctx)
			if err != nil {
				return fmt.Errorf("marking %d paths of set %d damaged: %w", len(batch), setID, err)
			}

			return nil
		})
	})
}

// MarkRescan records that a whole set must be read again, for damage no
// path could be resolved for.
func (x *Index) MarkRescan(ctx context.Context, setID int64) error {
	n, err := x.client.Set.Update().Where(set.ID(setID)).SetRescan(true).ClearGrantedAt().Save(ctx)
	if err != nil {
		return err
	}

	if n == 0 {
		return fmt.Errorf("%w: set %d", backup.ErrNotFound, setID)
	}

	return nil
}

// grantDamage returns what a run on the set must read again regardless of
// what its change detection says, and records when it was granted.
func (x *Index) grantDamage(ctx context.Context, setID int64) (rescan bool, paths []string, err error) {
	err = x.tx(ctx, func(tx *ent.Tx) error {
		s, err := x.lockSet(ctx, tx, setID)
		if err != nil {
			return err
		}

		rescan, paths = s.Rescan, nil
		granted := x.now()

		rows, err := tx.DamagedPath.Query().Where(damagedpath.SetID(setID)).Order(ent.Asc(damagedpath.FieldPath)).All(ctx)
		if err != nil {
			return err
		}

		for _, row := range rows {
			if !rescan {
				paths = append(paths, row.Path)
			}

			if row.FoundAt.After(granted) {
				granted = row.FoundAt
			}
		}

		return tx.Set.UpdateOneID(setID).SetGrantedAt(granted).Exec(ctx)
	})

	return rescan, paths, err
}

// clearDamage forgets what a completed run has read again: what the set's
// last grant named, once. A checkpoint leaves it, because it did not cover
// the whole set.
func (x *Index) clearDamage(ctx context.Context, tx *ent.Tx, setID int64, partial bool) error {
	if partial {
		return nil
	}

	s, err := tx.Set.Get(ctx, setID)
	if err != nil {
		return err
	}

	if s.GrantedAt != nil {
		update := tx.Set.UpdateOneID(setID).ClearGrantedAt()
		if s.Rescan {
			update.SetRescan(false)
		}

		if err := update.Exec(ctx); err != nil {
			return err
		}

		_, err = tx.DamagedPath.Delete().Where(damagedpath.SetID(setID), damagedpath.FoundAtLTE(*s.GrantedAt)).Exec(ctx)
		if err != nil {
			return err
		}
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

// damageWorkers is how many file objects FindDamage checks at once.
const damageWorkers = 16

// damagePage is how many distinct file objects FindDamage reads the refs
// of and checks at a time.
var damagePage = 10000

var _ backup.DamageFinder = (*Index)(nil)

// FindDamage implements backup.DamageFinder.
func (x *Index) FindDamage(ctx context.Context) (*backup.DamageReport, error) {
	total, err := x.client.File.Query().Where(file.RefNotNil(), file.Lost(false)).Unique(true).Select(file.FieldRef).Count(ctx)
	if err != nil {
		return nil, err
	}

	var (
		lost  []*proto.Ref
		named = map[string]bool{}
	)

	phase := progress.Start(ctx, progress.OpDamage, progress.PhaseCheck, int64(total), 0)

	err = x.pageFileRefs(ctx, func(refs [][]byte) error {
		gone, err := x.lostOf(ctx, refs)
		if err != nil {
			return err
		}

		// a part the versions of two pages share is found in both
		for _, ref := range gone {
			if !named[string(ref.GetHash())] {
				named[string(ref.GetHash())] = true
				lost = append(lost, ref)
			}
		}

		phase.Add(int64(len(refs)), 0)

		return nil
	})
	phase.Finish()

	if err != nil {
		return nil, err
	}

	if len(lost) == 0 {
		return &backup.DamageReport{Paths: map[int64][]string{}}, nil
	}

	sets, err := x.client.Set.Query().IDs(ctx)
	if err != nil {
		return nil, err
	}

	phase = progress.Start(ctx, progress.OpDamage, progress.PhasePlace, int64(total), 0)
	defer phase.Finish()

	report, err := backup.ReportDamage(ctx, indexedFiles{ObjectStore: x.ObjectStore, x: x, phase: phase}, x, sets, lost)
	if err != nil {
		return nil, err
	}

	x.logger().Warn("the store lost objects its versions reference", "objects", len(lost),
		"damaged sets", len(report.Paths), "unrecoverable versions", len(report.Lost), "sets to read again", len(report.Rescan))

	return report, nil
}

// pageFileRefs calls fn with the distinct file refs of the indexed
// versions not yet marked lost, in order, damagePage at a time.
func (x *Index) pageFileRefs(ctx context.Context, fn func(refs [][]byte) error) error {
	var after []byte

	for {
		var rows []struct {
			Ref []byte `json:"ref"`
		}

		query := x.client.File.Query().Where(file.RefNotNil(), file.Lost(false))
		if after != nil {
			query.Where(file.RefGT(after))
		}

		err := query.Unique(true).Order(ent.Asc(file.FieldRef)).Limit(damagePage).Select(file.FieldRef).Scan(ctx, &rows)
		if err != nil || len(rows) == 0 {
			return err
		}

		refs := make([][]byte, len(rows))
		for i, row := range rows {
			refs[i] = row.Ref
		}

		if err := fn(refs); err != nil {
			return err
		}

		after = refs[len(refs)-1]
	}
}

// indexedFiles is the store whose file objects are only those the indexed
// versions name and their splits, read through the store's cache rather
// than walked archive by archive.
type indexedFiles struct {
	backup.ObjectStore
	x     *Index
	phase *progress.Phase
}

// Walk walks the file objects the indexed versions name and their splits,
// each once; it walks nothing of any other type.
func (s indexedFiles) Walk(ctx context.Context, _ bool, t proto.ObjectType, fn backup.ObjectReceiver) error {
	if t != proto.ObjectType_FILE {
		return nil
	}

	seen := map[string]bool{}

	return s.x.pageFileRefs(ctx, func(page [][]byte) error {
		refs := make([]*proto.Ref, len(page))
		for i, ref := range page {
			refs[i] = &proto.Ref{Hash: ref}
		}

		for len(refs) > 0 {
			objects, err := s.read(ctx, refs, seen)
			if err != nil {
				return err
			}

			refs = refs[:0:0]
			for _, obj := range objects {
				if err := fn(obj); err != nil {
					return err
				}

				refs = append(refs, obj.GetFile().GetSplits()...)
			}
		}

		s.phase.Add(int64(len(page)), 0)

		return nil
	})
}

// read gets the objects of refs not seen yet, damageWorkers at a time,
// leaving out those the store no longer holds.
func (s indexedFiles) read(ctx context.Context, refs []*proto.Ref, seen map[string]bool) ([]*proto.Object, error) {
	var fresh []*proto.Ref

	for _, ref := range refs {
		if !seen[string(ref.GetHash())] {
			seen[string(ref.GetHash())] = true
			fresh = append(fresh, ref)
		}
	}

	objects := make([]*proto.Object, len(fresh))

	group, gctx := errgroup.WithContext(ctx)
	group.SetLimit(damageWorkers)

	for i, ref := range fresh {
		group.Go(func() error {
			obj, err := s.ObjectStore.Get(gctx, ref)
			if errors.Is(err, backup.ErrNotFound) {
				return nil
			}

			objects[i] = obj

			return err
		})
	}

	if err := group.Wait(); err != nil {
		return nil, err
	}

	return slices.DeleteFunc(objects, func(obj *proto.Object) bool { return obj == nil }), nil
}

// lostOf returns what of the file objects refs name, their parts and
// their splits, the store no longer holds, each named once.
func (x *Index) lostOf(ctx context.Context, refs [][]byte) ([]*proto.Ref, error) {
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

		var parts []*proto.Ref
		for _, part := range obj.GetFile().GetParts() {
			if part.GetRef() != nil && claim(part.GetRef()) {
				parts = append(parts, part.GetRef())
			}
		}

		if len(parts) > 0 {
			held, err := backup.HasAll(ctx, x.ObjectStore, parts)
			if err != nil {
				return err
			}

			for i, part := range parts {
				if !held[i] {
					gone(part)
				}
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

	for _, ref := range refs {
		if len(ref) == 0 {
			continue
		}

		group.Go(func() error {
			return check(gctx, &proto.Ref{Hash: ref})
		})
	}

	return lost, group.Wait()
}
