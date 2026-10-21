package sql

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/index"
	"github.com/gobackio/goback/index/sql/ent"
	"github.com/gobackio/goback/index/sql/ent/commitrow"
	"github.com/gobackio/goback/index/sql/ent/deletedref"
	"github.com/gobackio/goback/index/sql/ent/file"
	"github.com/gobackio/goback/index/sql/ent/set"
	"github.com/gobackio/goback/index/sql/ent/tree"
	"github.com/gobackio/goback/proto"
)

// TombstonedCommits returns the set's tombstoned commits, oldest first:
// those tombstoned at or after since, or all when it is zero.
func (x *Index) TombstonedCommits(ctx context.Context, name string, since time.Time) ([]*proto.Ref, error) {
	setID, err := findSet(ctx, x.client, name)
	if err != nil {
		return nil, err
	}

	query := x.client.CommitRow.Query().Where(commitrow.SetID(setID), commitrow.TombstonedAtNotNil())
	if !since.IsZero() {
		query.Where(commitrow.TombstonedAtGTE(since))
	}

	rows, err := query.Order(ent.Asc(commitrow.FieldReceivedAt)).All(ctx)
	if err != nil {
		return nil, err
	}

	refs := make([]*proto.Ref, len(rows))
	for i, row := range rows {
		refs[i] = &proto.Ref{Hash: row.Ref}
	}

	return refs, nil
}

// errDryRun rolls back the transaction of a dry run.
var errDryRun = errors.New("dry run")

// UnretireCommits brings tombstoned commits of active sets back to live
// while the store still holds everything they reach, and has each set's
// policy decide on them again. A commit missing an object is reported and
// stays tombstoned. dryRun changes nothing and reports what the policies
// would keep.
func (x *Index) UnretireCommits(ctx context.Context, refs []*proto.Ref, dryRun bool) ([]index.Unretired, error) {
	reviver, ok := storeAs[backup.Reviver](x.ObjectStore)
	if !ok {
		return nil, fmt.Errorf("%w: store %T cannot revive commits", backup.ErrNotImplemented, x.ObjectStore)
	}

	rows, err := x.tombstonedRows(ctx, refs)
	if err != nil {
		return nil, err
	}

	revivals, err := reviver.Revive(ctx, refs, dryRun)
	if err != nil {
		return nil, err
	}

	results := make([]index.Unretired, len(refs))
	bySet := make(map[int64][]*ent.CommitRow)

	for i, r := range revivals {
		row := rows[string(r.Commit.Hash)]
		results[i] = index.Unretired{Revival: r, Set: row.Edges.Set.Name}

		if r.Whole() {
			bySet[row.SetID] = append(bySet[row.SetID], row)
		}
	}

	retained := make(map[string]string)

	for setID, revived := range bySet {
		sort.Slice(revived, func(i, j int) bool { return revived[i].ReceivedAt.Before(revived[j].ReceivedAt) })

		if dryRun {
			err = x.wouldRetain(ctx, setID, revived, retained)
		} else {
			err = x.unretireSet(ctx, setID, revived, retained)
		}

		if err != nil {
			return nil, err
		}
	}

	for i := range results {
		results[i].RetainedBy = retained[string(results[i].Commit.Hash)]
	}

	return results, nil
}

// tombstonedRows loads the commits' rows, refusing a commit that is not
// tombstoned or whose set is not active.
func (x *Index) tombstonedRows(ctx context.Context, refs []*proto.Ref) (map[string]*ent.CommitRow, error) {
	hashes := make([][]byte, len(refs))
	for i, ref := range refs {
		hashes[i] = ref.GetHash()
	}

	found, err := x.client.CommitRow.Query().Where(commitrow.RefIn(hashes...)).WithSet().All(ctx)
	if err != nil {
		return nil, err
	}

	rows := make(map[string]*ent.CommitRow, len(found))
	for _, row := range found {
		rows[string(row.Ref)] = row
	}

	for _, hash := range hashes {
		row := rows[string(hash)]

		switch {
		case row == nil:
			return nil, fmt.Errorf("%w: commit %x", backup.ErrNotFound, hash)
		case row.TombstonedAt == nil:
			return nil, fmt.Errorf("commit %x is not tombstoned", hash)
		case row.Edges.Set.State != set.StateActive:
			return nil, fmt.Errorf("%w: set %q", backup.ErrSetClosed, row.Edges.Set.Name)
		}
	}

	return rows, nil
}

// wouldRetain records what the set's policy would keep with the commits
// back, in a transaction it rolls back.
func (x *Index) wouldRetain(ctx context.Context, setID int64, revived []*ent.CommitRow, retained map[string]string) error {
	err := x.tx(ctx, func(tx *ent.Tx) error {
		if _, err := x.lockSet(ctx, tx, setID); err != nil {
			return err
		}

		for _, row := range revived {
			if err := liveAgain(ctx, tx, row.Ref); err != nil {
				return err
			}
		}

		if err := x.evaluateSet(ctx, tx, setID, x.now()); err != nil {
			return err
		}

		if err := readRetained(ctx, tx.Client(), revived, retained); err != nil {
			return err
		}

		return errDryRun
	})
	if errors.Is(err, errDryRun) {
		return nil
	}

	return err
}

// unretireSet makes each revived commit live with the rows its retirement
// dropped, one transaction each, then evaluates the set once all are back.
func (x *Index) unretireSet(ctx context.Context, setID int64, revived []*ent.CommitRow, retained map[string]string) error {
	visited := make(map[string]bool)

	for _, row := range revived {
		if err := x.unretire(ctx, row, visited); err != nil {
			return fmt.Errorf("unretiring commit %x: %w", row.Ref, err)
		}
	}

	if err := x.reevaluateSet(ctx, setID); err != nil {
		return err
	}

	return readRetained(ctx, x.client, revived, retained)
}

// unretire makes a revived commit live again: its versions rows, the refs
// it makes readable and its row. The store is read before the transaction,
// which the trees read then serve, so the transaction holds no lock while
// the store answers.
func (x *Index) unretire(ctx context.Context, row *ent.CommitRow, visited map[string]bool) error {
	refs := [][]byte{row.Ref}
	err := x.walkRefs(ctx, []*proto.Ref{{Hash: row.Tree}}, visited, false, func(hash []byte) { refs = append(refs, hash) })
	if err != nil {
		return err
	}

	for range planAttempts {
		read := readTrees{}
		if err := restoreVersions(ctx, x.client, recordTrees{x.ObjectStore, read}, row, false); err != nil {
			return err
		}

		err := x.policyTx(ctx, func(tx *ent.Tx) (*proto.Policy, error) {
			locked, err := x.lockCommitRow(ctx, tx, row.Ref)
			if err != nil || locked.TombstonedAt == nil {
				return commitScope(row.Ref, time.Time{}), err
			}

			if err := restoreVersions(ctx, tx.Client(), read, locked, true); err != nil {
				return nil, err
			}

			if err := addSetRefs(ctx, tx.Client(), locked.SetID, refs); err != nil {
				return nil, err
			}

			return commitScope(row.Ref, time.Time{}), liveAgain(ctx, tx, row.Ref)
		})
		if !errors.Is(err, errSetMoved) {
			return err
		}
	}

	return errSetMoved
}

// restoreVersions walks the commit's tree against the set's rows through c,
// writing the rows its retirement dropped when write is set.
func restoreVersions(ctx context.Context, c *ent.Client, store backup.Getter, row *ent.CommitRow, write bool) error {
	next, err := c.CommitRow.Query().Where(commitrow.SetID(row.SetID), commitrow.ReceivedAtGT(row.ReceivedAt)).
		Order(ent.Asc(commitrow.FieldReceivedAt)).First(ctx)
	if err != nil && !ent.IsNotFound(err) {
		return err
	}

	v := &versions{c: c, store: store, setID: row.SetID, at: row.ReceivedAt, write: write}
	if next != nil {
		v.until = &next.ReceivedAt
	}

	root, err := backup.LoadTree(ctx, store, &proto.Ref{Hash: row.Tree})
	if err != nil {
		return fmt.Errorf("reading tree %x: %w", row.Tree, err)
	}

	return v.dir(ctx, "", root)
}

// readTrees serves the trees a plan read; one it did not read means the
// set's rows moved since.
type readTrees map[string]*proto.Object

func (r readTrees) Get(_ context.Context, ref *proto.Ref) (*proto.Object, error) {
	if obj, ok := r[string(ref.Hash)]; ok {
		return obj, nil
	}

	return nil, errSetMoved
}

// recordTrees reads from the store and keeps what it read.
type recordTrees struct {
	store backup.Getter
	read  readTrees
}

// ReadRecords implements backup.RecordReader, keeping what it read.
func (r recordTrees) ReadRecords(ctx context.Context, refs []*proto.Ref) ([]*proto.Object, error) {
	objects, _ := backup.ReadObjects(ctx, r.store, refs)
	for i, obj := range objects {
		if obj != nil {
			r.read[string(refs[i].Hash)] = obj
		}
	}

	return objects, nil
}

func (r recordTrees) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	obj, err := r.store.Get(ctx, ref)
	if err == nil {
		r.read[string(ref.Hash)] = obj
	}

	return obj, err
}

// liveAgain clears every mark of the commit's retirement and its tombstone.
func liveAgain(ctx context.Context, tx *ent.Tx, ref []byte) error {
	err := tx.CommitRow.Update().Where(commitrow.Ref(ref)).ClearTombstonedAt().ClearRetireAt().ClearRetirePolicy().ClearExpiresAt().
		ClearDeletedAt().SetRetainedBy("").Exec(ctx)
	if err != nil {
		return err
	}

	_, err = tx.DeletedRef.Delete().Where(deletedref.Ref(ref)).Exec(ctx)

	return err
}

func readRetained(ctx context.Context, c *ent.Client, rows []*ent.CommitRow, retained map[string]string) error {
	refs := make([][]byte, len(rows))
	for i, row := range rows {
		refs[i] = row.Ref
	}

	current, err := c.CommitRow.Query().Where(commitrow.RefIn(refs...)).All(ctx)
	if err != nil {
		return err
	}

	for _, row := range current {
		retained[string(row.Ref)] = row.RetainedBy
	}

	return nil
}

func addSetRefs(ctx context.Context, c *ent.Client, setID int64, refs [][]byte) error {
	for start := 0; start < len(refs); start += objectBatch {
		chunk := refs[start:min(start+objectBatch, len(refs))]

		rows := make([]*ent.SetRefCreate, len(chunk))
		for i, ref := range chunk {
			rows[i] = c.SetRef.Create().SetSetID(setID).SetRef(ref)
		}

		if err := ignoreNoRows(c.SetRef.CreateBulk(rows...).OnConflict().DoNothing().Exec(ctx)); err != nil {
			return err
		}
	}

	return nil
}

// versions restores the set's versions rows at a revived commit's receipt
// time: a path no row holds then gets one up to the set's next commit,
// joined to a neighbour of the same content. A directory a row already
// holds then holds everything below it too. Without write it only reads.
type versions struct {
	c     *ent.Client
	store backup.Getter
	setID int64
	at    time.Time
	until *time.Time
	write bool
}

func (v *versions) dir(ctx context.Context, dir string, t *proto.Tree) error {
	var dirs, files []string

	for _, node := range t.GetNodes() {
		child := proto.JoinPath(dir, node.GetStat().GetName())
		if node.GetStat().GetLost() && node.GetStat().IsDir() {
			continue
		}

		if node.GetStat().IsDir() {
			dirs = append(dirs, child)
		} else {
			files = append(files, child)
		}
	}

	treeRows, fileRows, err := v.rows(ctx, dirs, files)
	if err != nil {
		return err
	}

	var below []string
	var refs []*proto.Ref

	for _, node := range t.GetNodes() {
		info := node.GetStat()
		child := proto.JoinPath(dir, info.GetName())

		if info.GetLost() && info.IsDir() {
			continue
		}

		if !info.IsDir() {
			if err := v.file(ctx, child, dir, node, fileRows[child]); err != nil {
				return err
			}

			continue
		}

		held, err := v.tree(ctx, child, dir, node.GetRef().GetHash(), treeRows[child])
		if err != nil {
			return err
		}

		if !held {
			below, refs = append(below, child), append(refs, node.GetRef())
		}
	}

	objects, errs := backup.ReadObjects(ctx, v.store, refs)
	read := readTrees{}

	for i, ref := range refs {
		if errs[i] != nil {
			return fmt.Errorf("reading tree %x: %w", ref.GetHash(), errs[i])
		}

		read[string(ref.GetHash())] = objects[i]
	}

	for i, ref := range refs {
		sub, err := backup.LoadTree(ctx, behindTrees{read, v.store}, ref)
		if err != nil {
			return fmt.Errorf("reading tree %x: %w", ref.GetHash(), err)
		}

		if err := v.dir(ctx, below[i], sub); err != nil {
			return err
		}
	}

	return nil
}

// behindTrees serves the trees read ahead and gets the rest from store.
type behindTrees struct {
	read  readTrees
	store backup.Getter
}

func (b behindTrees) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	if obj, ok := b.read[string(ref.GetHash())]; ok {
		return obj, nil
	}

	return b.store.Get(ctx, ref)
}

// rows reads the rows of the directories and files that hold the revived
// commit or border its range, by path.
func (v *versions) rows(ctx context.Context, dirs, files []string) (map[string][]*ent.Tree, map[string][]*ent.File, error) {
	trees := make(map[string][]*ent.Tree, len(dirs))
	err := inBatches(dirs, func(paths []string) error {
		rows, err := v.c.Tree.Query().Where(tree.SetID(v.setID), tree.PathIn(paths...),
			tree.Or(tree.ValidFromLTE(v.at), tree.ValidUntilEQ(v.at), tree.ValidFromEQ(v.end()))).All(ctx)
		for _, row := range rows {
			trees[row.Path] = append(trees[row.Path], row)
		}

		return err
	})
	if err != nil {
		return nil, nil, err
	}

	byPath := make(map[string][]*ent.File, len(files))
	err = inBatches(files, func(paths []string) error {
		rows, err := v.c.File.Query().Where(file.SetID(v.setID), file.PathIn(paths...),
			file.Or(file.ValidFromLTE(v.at), file.ValidUntilEQ(v.at), file.ValidFromEQ(v.end()))).All(ctx)
		for _, row := range rows {
			byPath[row.Path] = append(byPath[row.Path], row)
		}

		return err
	})

	return trees, byPath, err
}

// tree restores the row of a directory, given the rows rows read of its
// path, and reports whether one held it already.
func (v *versions) tree(ctx context.Context, p, dir string, ref []byte, rows []*ent.Tree) (bool, error) {
	var prev, next *ent.Tree

	for _, row := range rows {
		switch {
		case !row.ValidFrom.After(v.at) && (row.ValidUntil == nil || row.ValidUntil.After(v.at)):
			if !bytes.Equal(row.Ref, ref) {
				return false, fmt.Errorf("set %d holds another version of %s at %s", v.setID, p, v.at)
			}

			return true, nil
		case row.ValidUntil != nil && row.ValidUntil.Equal(v.at) && bytes.Equal(row.Ref, ref):
			prev = row
		case v.until != nil && row.ValidFrom.Equal(*v.until) && bytes.Equal(row.Ref, ref):
			next = row
		}
	}

	if !v.write {
		return false, nil
	}

	from, until := v.at, v.until
	if prev != nil {
		from = prev.ValidFrom
	}

	if next != nil {
		until = next.ValidUntil
	}

	for _, row := range []*ent.Tree{prev, next} {
		if row != nil {
			if err := v.c.Tree.DeleteOneID(row.ID).Exec(ctx); err != nil {
				return false, err
			}
		}
	}

	return false, v.c.Tree.Create().SetSetID(v.setID).SetPath(p).SetDir(dir).SetValidFrom(from).
		SetNillableValidUntil(until).SetRef(ref).Exec(ctx)
}

// file restores the row of a file, symlink or other non-directory, given
// the rows rows read of its path.
func (v *versions) file(ctx context.Context, p, dir string, node *proto.TreeNode, rows []*ent.File) error {
	var prev, next *ent.File

	for _, row := range rows {
		switch {
		case !row.ValidFrom.After(v.at) && (row.ValidUntil == nil || row.ValidUntil.After(v.at)):
			if !sameFile(row, node) {
				return fmt.Errorf("set %d holds another version of %s at %s", v.setID, p, v.at)
			}

			return nil
		case row.ValidUntil != nil && row.ValidUntil.Equal(v.at) && sameFile(row, node):
			prev = row
		case v.until != nil && row.ValidFrom.Equal(*v.until) && sameFile(row, node):
			next = row
		}
	}

	if !v.write {
		return nil
	}

	from, until := v.at, v.until
	if prev != nil {
		from = prev.ValidFrom
	}

	if next != nil {
		until = next.ValidUntil
	}

	for _, row := range []*ent.File{prev, next} {
		if row != nil {
			if err := v.c.File.DeleteOneID(row.ID).Exec(ctx); err != nil {
				return err
			}
		}
	}

	info := node.GetStat()

	return v.c.File.Create().SetSetID(v.setID).SetPath(p).SetDir(dir).SetValidFrom(from).SetNillableValidUntil(until).
		SetRef(node.GetRef().GetHash()).SetMtimeNs(info.GetMtimeNs()).SetMode(info.GetMode()).
		SetUser(proto.PathComponent(info.GetUser())).SetGroup(proto.PathComponent(info.GetGroup())).SetSize(info.GetSize()).
		SetType(uint32(info.GetType())).SetLinkTarget(info.GetLinkTarget()).Exec(ctx)
}

// end is the end of the revived commit's own range, the zero time when it
// is the set's newest.
func (v *versions) end() time.Time {
	if v.until == nil {
		return time.Time{}
	}

	return *v.until
}

// revived reports whether a revival took back the tombstone of a commit.
func (x *Index) revived(ctx context.Context, ref []byte) (bool, error) {
	reviver, ok := storeAs[backup.Reviver](x.ObjectStore)
	if !ok {
		return false, nil
	}

	return reviver.Revived(ctx, &proto.Ref{Hash: ref})
}

// stillDeleted reports whether a tombstone still stands against a commit
// deleted_refs names, and forgets the commit there when it was revived.
func (x *Index) stillDeleted(ctx context.Context, ref []byte) (bool, error) {
	revived, err := x.revived(ctx, ref)
	if err != nil || !revived {
		return true, err
	}

	_, err = x.client.DeletedRef.Delete().Where(deletedref.Ref(ref)).Exec(ctx)

	return false, err
}
