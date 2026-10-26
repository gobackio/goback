package sql

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/index/sql/ent"
	"github.com/gobackio/goback/index/sql/ent/commitrow"
	"github.com/gobackio/goback/index/sql/ent/file"
	"github.com/gobackio/goback/index/sql/ent/pin"
	"github.com/gobackio/goback/index/sql/ent/setref"
	"github.com/gobackio/goback/index/sql/ent/tree"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/pack"

	"entgo.io/ent/dialect"
)

var _ pack.ResealFollower = (*Index)(nil)

// ReplaceCommit implements pack.ResealFollower: the commit's row takes the
// replacement's ref, tree and parent and keeps the rest of its state, the
// versions valid at it and the refs its set reads take what the
// replacement names, its pins move, and its presence filter is dropped for
// maintenance to build again.
func (x *Index) ReplaceCommit(ctx context.Context, replaced pack.ResealedCommit) error {
	pins := make([]movedPin, len(replaced.Pins))
	for i, p := range replaced.Pins {
		pins[i] = movedPin{old: p.Old.GetHash(), new: p.New.GetHash(), target: replaced.New.GetHash()}
	}

	return x.followReseal(ctx,
		func() (resealPlan, error) { return x.planReplacement(ctx, replaced) },
		func(tx *ent.Tx, plan resealPlan) error {
			c := tx.Client()

			if plan.row != nil {
				_, err := c.ExecContext(ctx, x.rebind("UPDATE "+commitrow.Table+" SET "+commitrow.FieldRef+" = ? WHERE "+commitrow.FieldID+" = ?"),
					replaced.New.GetHash(), plan.row.ID)
				if err != nil {
					return err
				}

				err = c.CommitRow.UpdateOneID(plan.row.ID).SetTree(plan.commit.GetTree().GetHash()).
					SetParent(plan.commit.GetParent().GetHash()).ClearPresence().Exec(ctx)
				if err != nil {
					return err
				}

				if err := plan.diff.apply(ctx, c); err != nil {
					return err
				}
			}

			gone := [][]byte{replaced.Old.GetHash()}
			for _, p := range pins {
				if err := movePin(ctx, x, c, p); err != nil {
					return err
				}

				gone = append(gone, p.old)
			}

			return recordDeleted(ctx, c, gone, x.now())
		})
}

// MovePin implements pack.ResealFollower: the pin's row takes the moved
// pin's ref and target.
func (x *Index) MovePin(ctx context.Context, moved pack.Replacement) error {
	obj, err := x.reads().Get(ctx, moved.New)
	if err != nil {
		return err
	}

	p := movedPin{old: moved.Old.GetHash(), new: moved.New.GetHash(), target: obj.GetPin().GetTarget().GetHash()}

	return x.tx(ctx, func(tx *ent.Tx) error {
		if err := movePin(ctx, x, tx.Client(), p); err != nil {
			return err
		}

		return recordDeleted(ctx, tx.Client(), [][]byte{p.old}, x.now())
	})
}

// KeepCommit implements pack.ResealFollower: the versions valid at the
// commit, up to the set's next live commit, name what its tree holds
// again, and its set reads all of that.
func (x *Index) KeepCommit(ctx context.Context, ref *proto.Ref) error {
	return x.followReseal(ctx,
		func() (resealPlan, error) {
			row, err := x.client.CommitRow.Query().Where(commitrow.Ref(ref.GetHash())).Only(ctx)
			if ent.IsNotFound(err) {
				return resealPlan{}, nil
			}

			if err != nil {
				return resealPlan{}, err
			}

			obj, err := x.reads().Get(ctx, ref)
			if err != nil {
				return resealPlan{}, err
			}

			plan, err := x.planReseal(ctx, row, obj.GetCommit(), true)
			if err != nil {
				return resealPlan{}, err
			}

			return plan, plan.diff.walk(ctx, nil, obj.GetCommit().GetTree())
		},
		func(tx *ent.Tx, plan resealPlan) error {
			if plan.row == nil {
				return nil
			}

			return plan.diff.apply(ctx, tx.Client())
		})
}

// resealPlan is what following a commit through a reseal writes: the
// commit's row and the set's newest commit as they were when it was
// planned, the commit that stands at the row then, and the changes to
// the set's versions and refs. A plan without a row has no versions to
// change.
type resealPlan struct {
	row    *ent.CommitRow
	newest int
	commit *proto.Commit
	diff   *resealDiff
}

// followReseal plans a change outside a transaction, as the walk reads the
// store, and applies it under the set's lock, provided the commit's row and
// the set's newest commit are still the ones it was planned against.
func (x *Index) followReseal(ctx context.Context, plan func() (resealPlan, error), apply func(*ent.Tx, resealPlan) error) error {
	for attempt := 1; ; attempt++ {
		p, err := plan()
		if err != nil {
			return err
		}

		err = x.tx(ctx, func(tx *ent.Tx) error {
			if p.row != nil {
				if err := x.stillPlanned(ctx, tx, p); err != nil {
					return err
				}
			}

			return apply(tx, p)
		})
		if errors.Is(err, errSetMoved) && attempt < planAttempts {
			continue
		}

		return err
	}
}

func (x *Index) stillPlanned(ctx context.Context, tx *ent.Tx, p resealPlan) error {
	if _, err := x.lockSet(ctx, tx, p.row.SetID); err != nil {
		return err
	}

	current, err := forUpdate(x, tx.CommitRow.Query().Where(commitrow.ID(p.row.ID))).Only(ctx)
	if ent.IsNotFound(err) {
		return errSetMoved
	}

	if err != nil {
		return err
	}

	newest, err := newestCommit(ctx, tx.Client(), p.row.SetID)
	if err != nil {
		return err
	}

	if !bytes.Equal(current.Ref, p.row.Ref) || newest == nil || newest.ID != p.newest {
		return errSetMoved
	}

	return nil
}

// planReplacement walks the old commit's tree and its replacement's
// against the versions valid at the old commit. A commit the index does
// not hold, or holds replaced already, has no versions to change.
func (x *Index) planReplacement(ctx context.Context, replaced pack.ResealedCommit) (resealPlan, error) {
	row, err := x.client.CommitRow.Query().Where(commitrow.Ref(replaced.Old.GetHash())).Only(ctx)
	if ent.IsNotFound(err) {
		return resealPlan{}, nil
	}

	if err != nil {
		return resealPlan{}, err
	}

	objects, errs := backup.ReadObjects(ctx, x.reads(), []*proto.Ref{replaced.Old, replaced.New})
	if err := errors.Join(errs...); err != nil {
		return resealPlan{}, err
	}

	old, next := objects[0].GetCommit(), objects[1].GetCommit()
	if old == nil || next == nil {
		return resealPlan{}, fmt.Errorf("%w: %x or %x is not a commit", proto.ErrInvalidObject, replaced.Old.GetHash(), replaced.New.GetHash())
	}

	plan, err := x.planReseal(ctx, row, next, false)
	if err != nil {
		return resealPlan{}, err
	}

	plan.diff.drop[string(replaced.Old.GetHash())] = true
	plan.diff.add[string(replaced.New.GetHash())] = true

	return plan, plan.diff.walk(ctx, old.GetTree(), next.GetTree())
}

// planReseal starts the plan of the commit row stands for, with when the
// set's other live commits were received.
func (x *Index) planReseal(ctx context.Context, row *ent.CommitRow, commit *proto.Commit, kept bool) (resealPlan, error) {
	newest, err := newestCommit(ctx, x.client, row.SetID)
	if err != nil {
		return resealPlan{}, err
	}

	others, err := x.client.CommitRow.Query().
		Where(commitrow.SetID(row.SetID), commitrow.TombstonedAtIsNil(), commitrow.IDNEQ(row.ID)).
		Order(ent.Asc(commitrow.FieldReceivedAt)).Select(commitrow.FieldReceivedAt).All(ctx)
	if err != nil {
		return resealPlan{}, err
	}

	live := make([]time.Time, len(others))
	for i, other := range others {
		live[i] = other.ReceivedAt
	}

	diff := &resealDiff{
		read: x.client, store: x.reads(), x: x, setID: row.SetID, at: row.ReceivedAt, live: live, kept: kept,
		add: map[string]bool{}, drop: map[string]bool{},
	}

	return resealPlan{row: row, newest: newest.ID, commit: commit, diff: diff}, nil
}

// movedPin is a pin's row before and after a reseal moved the pin.
type movedPin struct {
	old, new, target []byte
}

// movePin makes the row of a moved pin name the pin it moved to, or drops
// it where that pin has a row already.
func movePin(ctx context.Context, x *Index, c *ent.Client, p movedPin) error {
	moved, err := c.Pin.Query().Where(pin.Ref(p.new)).Exist(ctx)
	if err != nil {
		return err
	}

	if moved {
		if _, err := c.Pin.Delete().Where(pin.Ref(p.old)).Exec(ctx); err != nil {
			return err
		}

		return c.Pin.Update().Where(pin.Ref(p.new)).SetTarget(p.target).Exec(ctx)
	}

	_, err = c.ExecContext(ctx, x.rebind("UPDATE "+pin.Table+" SET "+pin.FieldRef+" = ?, "+pin.FieldTarget+" = ? WHERE "+pin.FieldRef+" = ?"),
		p.new, p.target, p.old)

	return err
}

// rebind numbers the ? placeholders of a statement where Postgres needs
// them numbered.
func (x *Index) rebind(query string) string {
	if x.dialect != dialect.Postgres {
		return query
	}

	var b strings.Builder

	n := 0
	for _, r := range query {
		if r != '?' {
			b.WriteRune(r)
			continue
		}

		n++
		fmt.Fprintf(&b, "$%d", n)
	}

	return b.String()
}

// resealDiff plans making the versions rows of a set valid when one of
// its commits was received name what a tree holds, which differs from
// what they name only in refs, and the refs the set reads with them: what
// the tree reaches is added and what the tree it replaces reaches dropped.
// A directory whose row names its new tree already is passed over with
// all below it: it was followed before.
type resealDiff struct {
	read  *ent.Client
	store backup.ObjectStore
	x     *Index
	setID int64
	at    time.Time
	// live holds when the set's other live commits were received, in order
	live []time.Time
	// kept changes the rows for the commit alone rather than from it on,
	// walks the whole tree and drops nothing, for a commit the reseal left
	// as it was
	kept bool

	files     []versionFix[*ent.File]
	trees     []versionFix[*ent.Tree]
	add, drop map[string]bool
}

// versionFix is a versions row and the ref it takes from from until until,
// nil for as long as the row lasts; the row keeps its own ref outside.
type versionFix[R any] struct {
	row   R
	ref   []byte
	from  time.Time
	until *time.Time
}

// resealLevel is a directory of the tree the rows take, flattened, and of
// the tree it replaces, nil where there is none to drop.
type resealLevel struct {
	dir      string
	old, new *proto.Tree
}

// resealDescent is a directory below a level the walk reads on.
type resealDescent struct {
	path     string
	old, new *proto.Ref
}

// span is where a row valid from from until until takes the new ref: from
// the commit on, or all of it when no live commit before the commit holds
// it; for a kept commit, only up to the set's next live commit.
func (d *resealDiff) span(from time.Time, until *time.Time) (time.Time, *time.Time) {
	i, _ := slices.BinarySearchFunc(d.live, d.at, func(t, at time.Time) int { return t.Compare(at) })

	if !d.kept {
		if i > 0 && !d.live[i-1].Before(from) {
			from = d.at
		}

		return from, until
	}

	if i < len(d.live) && (until == nil || d.live[i].Before(*until)) {
		next := d.live[i]
		until = &next
	}

	return d.at, until
}

func (d *resealDiff) fixFile(row *ent.File, ref []byte) {
	from, until := d.span(row.ValidFrom, row.ValidUntil)
	d.files = append(d.files, versionFix[*ent.File]{row: row, ref: ref, from: from, until: until})
}

func (d *resealDiff) fixTree(row *ent.Tree, ref []byte) {
	from, until := d.span(row.ValidFrom, row.ValidUntil)
	d.trees = append(d.trees, versionFix[*ent.Tree]{row: row, ref: ref, from: from, until: until})
}

// walk plans the tree newRoot against the rows, a level of directories at
// a time; oldRoot, when given, is the tree it replaces, read alongside.
func (d *resealDiff) walk(ctx context.Context, oldRoot, newRoot *proto.Ref) error {
	roots, err := d.levels(ctx, []resealDescent{{old: oldRoot, new: newRoot}})
	if err != nil {
		return err
	}

	for next := roots; len(next) > 0; {
		current := next
		next = nil

		dirs := make([]string, len(current))
		for i, l := range current {
			dirs[i] = l.dir
		}

		trees, files, err := d.rowsAt(ctx, dirs)
		if err != nil {
			return err
		}

		var down []resealDescent
		var addLarge, dropLarge []*proto.Ref

		for _, l := range current {
			if l.old != nil && len(l.old.GetNodes()) != len(l.new.GetNodes()) {
				return fmt.Errorf("directory %q holds %d entries where it held %d", l.dir, len(l.new.GetNodes()), len(l.old.GetNodes()))
			}

			for i, node := range l.new.GetNodes() {
				info := node.GetStat()

				var was *proto.TreeNode
				if l.old != nil {
					was = l.old.GetNodes()[i]
					if !bytes.Equal(was.GetStat().GetName(), info.GetName()) {
						return fmt.Errorf("directory %q holds entries in another order than it did", l.dir)
					}
				}

				if info.GetLost() && info.IsDir() {
					continue
				}

				child := proto.JoinPath(l.dir, info.GetName())
				ref := node.GetRef().GetHash()

				addLarge = append(addLarge, metadataRefs(node, d.add)...)
				if was != nil {
					dropLarge = append(dropLarge, metadataRefs(was, d.drop)...)
				}

				if info.IsDir() {
					row := trees[child]
					if row != nil && bytes.Equal(row.Ref, ref) && !d.kept {
						continue
					}

					if row != nil && !bytes.Equal(row.Ref, ref) {
						d.fixTree(row, ref)
					}

					down = append(down, resealDescent{path: child, old: was.GetRef(), new: node.GetRef()})

					continue
				}

				row := files[child]
				agrees := row != nil && bytes.Equal(row.Ref, ref)

				if row != nil && !agrees {
					d.fixFile(row, ref)
				}

				if (agrees && !d.kept) || ref == nil || info.GetLost() || info.GetType() == proto.NodeType_NODE_SYMLINK {
					continue
				}

				d.add[string(ref)] = true
				if info.GetSize() >= backup.SplitFileSize {
					addLarge = append(addLarge, node.GetRef())
				}

				if old := was.GetRef(); old != nil && !old.Equal(node.GetRef()) {
					d.drop[string(old.GetHash())] = true
					if info.GetSize() >= backup.SplitFileSize {
						dropLarge = append(dropLarge, old)
					}
				}
			}
		}

		if err := d.x.walkLarge(ctx, addLarge, d.failed(), func(hash []byte) { d.add[string(hash)] = true }); err != nil {
			return err
		}

		lenient := func(err error) bool { return err != nil && !errors.Is(err, backup.ErrNotFound) }
		if err := d.x.walkLarge(ctx, dropLarge, lenient, func(hash []byte) { d.drop[string(hash)] = true }); err != nil {
			return err
		}

		if next, err = d.levels(ctx, down); err != nil {
			return err
		}
	}

	return nil
}

// failed is whether an error reading what the rows take fails the walk:
// a kept commit is walked around what the store lost.
func (d *resealDiff) failed() func(error) bool {
	return func(err error) bool {
		return err != nil && !(d.kept && errors.Is(err, backup.ErrNotFound))
	}
}

// metadataRefs adds the refs of the node's security descriptor, streams
// and extended attributes to refs, and returns those large enough to be
// split.
func metadataRefs(node *proto.TreeNode, refs map[string]bool) []*proto.Ref {
	for _, ref := range node.GetStat().MetadataRefs() {
		refs[string(ref.GetHash())] = true
	}

	var large []*proto.Ref

	for _, stream := range slices.Concat(node.GetStat().GetStreams(), node.GetStat().GetXattrs()) {
		if stream.GetSize() >= backup.SplitFileSize {
			large = append(large, stream.GetRef())
		}
	}

	return large
}

// levels reads the directories, new and old, flattened, recording the
// trees and splits read. An old directory the store lost leaves nothing
// to drop below it; a new one fails the walk, but for a kept commit,
// which leaves it out.
func (d *resealDiff) levels(ctx context.Context, down []resealDescent) ([]resealLevel, error) {
	var olds, news []*proto.Ref

	for _, dir := range down {
		news = append(news, dir.new)
		if dir.old != nil {
			olds = append(olds, dir.old)
		}
	}

	newObjects, newErrs := backup.ReadObjects(ctx, d.store, news)
	oldObjects, oldErrs := backup.ReadObjects(ctx, d.store, olds)

	var out []resealLevel

	o := 0
	for i, dir := range down {
		next, err := d.flatten(ctx, newObjects[i], newErrs[i], d.add, d.failed())
		if err != nil {
			return nil, err
		}

		if next != nil {
			d.add[string(dir.new.GetHash())] = true
		}

		var old *proto.Tree
		if dir.old != nil {
			d.drop[string(dir.old.GetHash())] = true

			old, err = d.flatten(ctx, oldObjects[o], oldErrs[o], d.drop, func(err error) bool { return err != nil && !errors.Is(err, backup.ErrNotFound) })
			if err != nil {
				return nil, err
			}

			o++
		}

		if next != nil {
			out = append(out, resealLevel{dir: dir.path, old: old, new: next})
		}
	}

	return out, nil
}

// flatten returns the nodes of a tree read with err, reading its splits and
// adding them to refs, or nil for a tree that cannot be read in full where
// failed passes over the error.
func (d *resealDiff) flatten(ctx context.Context, obj *proto.Object, err error, refs map[string]bool, failed func(error) bool) (*proto.Tree, error) {
	if err != nil {
		if failed(err) {
			return nil, err
		}

		return nil, nil
	}

	t := obj.GetTree()
	if t == nil {
		return nil, fmt.Errorf("%w: %x is not a tree", proto.ErrInvalidObject, obj.Ref().GetHash())
	}

	if len(t.GetSplits()) == 0 {
		return t, nil
	}

	objects, errs := backup.ReadObjects(ctx, d.store, t.GetSplits())
	flat := &proto.Tree{}

	for i, split := range t.GetSplits() {
		refs[string(split.GetHash())] = true

		sub, err := d.flatten(ctx, objects[i], errs[i], refs, failed)
		if err != nil || sub == nil {
			return nil, err
		}

		flat.Nodes = append(flat.Nodes, sub.Nodes...)
	}

	return flat, nil
}

// rowsAt returns the trees and files rows of the directories valid at the
// commit, by path.
func (d *resealDiff) rowsAt(ctx context.Context, dirs []string) (map[string]*ent.Tree, map[string]*ent.File, error) {
	trees := map[string]*ent.Tree{}
	files := map[string]*ent.File{}

	err := inBatches(dirs, func(batch []string) error {
		treeRows, err := d.read.Tree.Query().Where(tree.SetID(d.setID), tree.DirIn(batch...), tree.ValidFromLTE(d.at),
			tree.Or(tree.ValidUntilIsNil(), tree.ValidUntilGT(d.at))).All(ctx)
		if err != nil {
			return err
		}

		for _, row := range treeRows {
			trees[row.Path] = row
		}

		fileRows, err := d.read.File.Query().Where(file.SetID(d.setID), file.DirIn(batch...), file.ValidFromLTE(d.at),
			file.Or(file.ValidUntilIsNil(), file.ValidUntilGT(d.at))).All(ctx)
		if err != nil {
			return err
		}

		for _, row := range fileRows {
			files[row.Path] = row
		}

		return nil
	})

	return trees, files, err
}

// apply makes the planned writes through c.
func (d *resealDiff) apply(ctx context.Context, c *ent.Client) error {
	for _, fix := range d.files {
		if err := applyFileFix(ctx, c, fix); err != nil {
			return err
		}
	}

	for _, fix := range d.trees {
		if err := applyTreeFix(ctx, c, fix); err != nil {
			return err
		}
	}

	var add, drop [][]byte

	for ref := range d.add {
		add = append(add, []byte(ref))
	}

	for ref := range d.drop {
		if !d.add[ref] {
			drop = append(drop, []byte(ref))
		}
	}

	err := inBatches(drop, func(refs [][]byte) error {
		_, err := c.SetRef.Delete().Where(setref.SetID(d.setID), setref.RefIn(refs...)).Exec(ctx)
		return err
	})
	if err != nil {
		return err
	}

	return addSetRefs(ctx, c, d.setID, add)
}

func applyFileFix(ctx context.Context, c *ent.Client, fix versionFix[*ent.File]) error {
	row := fix.row

	copyRow := func(from time.Time, until *time.Time, ref []byte) error {
		return c.File.Create().SetSetID(row.SetID).SetPath(row.Path).SetDir(row.Dir).SetValidFrom(from).SetNillableValidUntil(until).
			SetRef(ref).SetMtimeNs(row.MtimeNs).SetMode(row.Mode).SetUser(row.User).SetGroup(row.Group).SetSize(row.Size).
			SetType(row.Type).SetLinkTarget(row.LinkTarget).SetLost(row.Lost).Exec(ctx)
	}

	if fix.from.After(row.ValidFrom) {
		if err := c.File.UpdateOneID(row.ID).SetValidUntil(fix.from).Exec(ctx); err != nil {
			return err
		}

		if err := copyRow(fix.from, fix.until, fix.ref); err != nil {
			return err
		}
	} else {
		update := c.File.UpdateOneID(row.ID).SetRef(fix.ref)
		if fix.until != nil {
			update.SetValidUntil(*fix.until)
		}

		if err := update.Exec(ctx); err != nil {
			return err
		}
	}

	if sameEnd(fix.until, row.ValidUntil) {
		return nil
	}

	return copyRow(*fix.until, row.ValidUntil, row.Ref)
}

func applyTreeFix(ctx context.Context, c *ent.Client, fix versionFix[*ent.Tree]) error {
	row := fix.row

	copyRow := func(from time.Time, until *time.Time, ref []byte) error {
		return c.Tree.Create().SetSetID(row.SetID).SetPath(row.Path).SetDir(row.Dir).SetValidFrom(from).SetNillableValidUntil(until).
			SetRef(ref).Exec(ctx)
	}

	if fix.from.After(row.ValidFrom) {
		if err := c.Tree.UpdateOneID(row.ID).SetValidUntil(fix.from).Exec(ctx); err != nil {
			return err
		}

		if err := copyRow(fix.from, fix.until, fix.ref); err != nil {
			return err
		}
	} else {
		update := c.Tree.UpdateOneID(row.ID).SetRef(fix.ref)
		if fix.until != nil {
			update.SetValidUntil(*fix.until)
		}

		if err := update.Exec(ctx); err != nil {
			return err
		}
	}

	if sameEnd(fix.until, row.ValidUntil) {
		return nil
	}

	return copyRow(*fix.until, row.ValidUntil, row.Ref)
}

// sameEnd reports whether two ends of a validity range, nil for open, are
// the same.
func sameEnd(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}

	return a.Equal(*b)
}
