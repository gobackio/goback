package sql

import (
	"bytes"
	"context"
	stdsql "database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/gobackio/goback/backup"
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
	"github.com/gobackio/goback/index/sql/mapping/gen"
	"github.com/gobackio/goback/progress"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/telemetry/laps"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"golang.org/x/sync/errgroup"
)

// m maps index rows to what the callers speak; one value for the package.
var m = gen.MapperImpl{}

// carriedReceipt bounds how far back a commit's carried receipt time is
// kept.
const carriedReceipt = 90 * 24 * time.Hour

// Stamp assigns the receipt time to a commit or pin and the set id to a
// commit. A commit keeps a receipt time it carries that is no later than
// now, no more than carriedReceipt before it, and later than its set's
// newest commit, as one an agent stored locally while offline does;
// anything else is replaced.
func (x *Index) Stamp(ctx context.Context, object *proto.Object) error {
	commit := object.GetCommit()
	if commit == nil && object.GetPin() == nil {
		return nil
	}

	at := x.stamp()

	var setID int64
	if commit != nil {
		var err error
		setID, err = x.ensureSet(ctx, x.client, commit, nil, true)
		if err != nil {
			return err
		}

		if carried := time.Unix(0, commit.GetReceivedAtNs()).UTC(); commit.GetReceivedAtNs() > 0 && !carried.After(at) && at.Sub(carried) <= carriedReceipt {
			newest, err := newestCommit(ctx, x.client, setID)
			if err != nil {
				return err
			}

			if newest == nil || carried.Truncate(receiptGrain).After(newest.ReceivedAt.Truncate(receiptGrain)) {
				at = carried
			}
		}
	}

	object.Stamp(uint64(setID), at)

	return nil
}

// seedStamp continues the receipt clock from the newest commit, so stamps
// stay strictly increasing across restarts and clock steps.
func (x *Index) seedStamp(ctx context.Context) error {
	newest, err := x.client.CommitRow.Query().Order(ent.Desc(commitrow.FieldReceivedAt)).First(ctx)
	if ent.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return err
	}

	x.stampMu.Lock()
	x.lastStamp = newest.ReceivedAt.UTC()
	x.stampMu.Unlock()

	return nil
}

// Put stores an object and indexes a commit or pin.
func (x *Index) Put(ctx context.Context, object *proto.Object) error {
	var err error

	var steps *laps.Laps
	if object.Type() == proto.ObjectType_COMMIT {
		steps = laps.Start()
	}

	if x.checks(ctx) {
		if err = backup.CheckReferences(ctx, x.ObjectStore, object); err != nil {
			return err
		}
	}

	if p := object.GetPin(); p != nil {
		err = x.checkPinTarget(ctx, x.client, p.GetTarget())
		if err != nil {
			return err
		}
	}

	err = x.Stamp(ctx, object)
	if err != nil {
		return err
	}

	steps.Lap("stamp")

	err = x.ObjectStore.Put(ctx, object)
	if err != nil {
		return err
	}

	steps.Lap("store")

	switch object.Type() {
	case proto.ObjectType_COMMIT:
		_, err := x.indexCommit(ctx, object.GetCommit(), object.Ref(), true, true, steps)
		return err
	case proto.ObjectType_PIN:
		return x.indexPin(ctx, object.GetPin(), object.Ref(), true)
	}

	return nil
}

func (x *Index) checkPinTarget(ctx context.Context, c *ent.Client, target *proto.Ref) error {
	deleted, err := isDeleted(ctx, c, target.GetHash())
	if err != nil {
		return err
	}

	if !deleted {
		row, err := c.CommitRow.Query().Where(commitrow.Ref(target.GetHash())).Only(ctx)
		if err != nil && !ent.IsNotFound(err) {
			return err
		}

		deleted = err == nil && row.TombstonedAt != nil
	}

	if deleted {
		return fmt.Errorf("%w: commit %x", backup.ErrTombstoned, target.GetHash())
	}

	visible, err := x.References(ctx, target)
	if err != nil {
		return err
	}

	if !visible {
		return fmt.Errorf("commit %x: %w", target.GetHash(), backup.ErrNotFound)
	}

	return nil
}

// ensureSet resolves the set a commit belongs to from the name in its
// body; strict mode refuses a set that is not active. A commit without a
// set name is refused: the rebuild names it after a placeholder set first.
func (x *Index) ensureSet(ctx context.Context, c *ent.Client, commit *proto.Commit, ref *proto.Ref, strict bool) (int64, error) {
	if commit.GetBackupSet() == "" {
		return 0, fmt.Errorf("%w: commit %x names no set", backup.ErrDanglingRef, ref.GetHash())
	}

	if !strict {
		return ensureSet(ctx, c, commit.GetBackupSet(), int64(commit.GetSetId()))
	}

	if _, ok := backup.SetID(commit.GetBackupSet()); ok {
		return 0, fmt.Errorf("%w: %q", backup.ErrSetName, commit.GetBackupSet())
	}

	setID, err := x.openSet(ctx, commit.GetBackupSet())
	if err != nil {
		return 0, err
	}

	s, err := c.Set.Get(ctx, setID)
	if err != nil {
		return 0, err
	}

	if s.State != set.StateActive {
		return 0, fmt.Errorf("%w: set %q", backup.ErrSetClosed, commit.GetBackupSet())
	}

	return setID, nil
}

// indexCommit records a commit and the versions its tree introduces. In
// strict mode a commit whose tree cannot be traversed is rejected with
// backup.ErrDanglingRef; otherwise it is indexed around the objects the
// store lost and marked incomplete, and a set with directories it can no
// longer list is read in full by its next run. Commits of a set are
// indexed one at a time, in receipt order: strict mode fails a commit
// received before the set's newest, which otherwise is left out.
func (x *Index) indexCommit(ctx context.Context, commit *proto.Commit, ref *proto.Ref, strict, evaluate bool, steps *laps.Laps) (placement, error) {
	treeObj, err := x.reads().Get(ctx, commit.Tree)
	if errors.Is(err, backup.ErrNotFound) && strict {
		return unplaced, fmt.Errorf("%w: root tree %x", backup.ErrDanglingRef, commit.Tree.GetHash())
	}

	if err != nil && !errors.Is(err, backup.ErrNotFound) {
		return unplaced, err
	}

	// the set is created outside the transaction: a unique violation
	// would abort a Postgres transaction, and a set without commits is
	// harmless
	setID, err := x.ensureSet(ctx, x.client, commit, ref, strict)
	if err != nil {
		return unplaced, err
	}

	steps.Lap("root")

	start := time.Now()

	for attempt := 1; ; attempt++ {
		plan, err := x.planCommit(ctx, commit, ref, treeObj, setID, strict, steps)
		if err != nil || plan.diff == nil {
			return plan.placement, err
		}

		indexed, err := x.applyCommit(ctx, commit, ref, setID, strict, evaluate, plan, steps)
		if errors.Is(err, errSetMoved) && attempt < planAttempts {
			continue
		}

		if err != nil || !indexed {
			return unplaced, err
		}

		x.logger().Info("indexed commit", append([]any{"set", commit.GetBackupSet(), "took", time.Since(start)}, steps.Attrs()...)...)

		return plan.placement, nil
	}
}

// placement is where indexCommit put a commit on its set's timeline.
type placement int

const (
	// unplaced is a commit left as it was: not indexed, or indexed before.
	unplaced placement = iota
	// inOrder is a commit received after the set's newest.
	inOrder
	// tied is a commit received at the same time as the set's newest,
	// indexed a microsecond after it.
	tied
	// behind is a commit received before the set's newest, not indexed.
	behind
)

// receiptGrain is the precision Postgres keeps receipt times to.
const receiptGrain = time.Microsecond

// commitPlan is what planCommit decided for a commit: the writes that
// index it, when it goes on the set's timeline and the set's newest
// commit the writes assume, 0 for none.
type commitPlan struct {
	diff      *treeDiff
	placement placement
	at        time.Time
	newest    int
}

// planAttempts bounds how often a commit is planned again because another
// commit of its set was indexed in the meantime.
const planAttempts = 3

// errSetMoved is applyCommit's answer when the set's newest commit is no
// longer the one the plan was made against.
var errSetMoved = errors.New("the set moved on while the commit was planned")

// planCommit walks the commit's tree against the set's open rows outside
// any transaction, so reading the store holds no lock and no connection,
// and returns its plan. It returns no diff for a commit there is nothing
// to do for.
func (x *Index) planCommit(ctx context.Context, commit *proto.Commit, ref *proto.Ref, treeObj *proto.Object, setID int64, strict bool, steps *laps.Laps) (commitPlan, error) {
	c := x.client
	plan := commitPlan{placement: inOrder, at: time.Unix(0, commit.ReceivedAtNs).UTC()}

	skip, err := x.skipCommit(ctx, c, commit, ref, setID, strict)
	if err != nil || skip {
		return commitPlan{}, err
	}

	newest, err := newestCommit(ctx, c, setID)
	if err != nil {
		return commitPlan{}, err
	}

	steps.Lap("plan")

	switch received := plan.at.Truncate(receiptGrain); {
	case newest == nil || received.After(newest.ReceivedAt.Truncate(receiptGrain)):
	case received.Equal(newest.ReceivedAt.Truncate(receiptGrain)):
		// versions are valid from their commit's receipt, so two commits
		// at one instant would leave the older none of its own
		x.logger().Error("commit received at the same time as the set's newest, indexing it a microsecond after",
			"ref", fmt.Sprintf("%x", ref.Hash), "received", plan.at, "newest", fmt.Sprintf("%x", newest.Ref))
		plan.placement, plan.at = tied, received.Add(receiptGrain)
	default:
		// another commit of the set, stamped later, was stored first
		if strict {
			return commitPlan{}, fmt.Errorf("commit %x received at %s, before the set's newest %x at %s, is not indexed",
				ref.Hash, plan.at.Format(time.RFC3339Nano), newest.Ref, newest.ReceivedAt.Format(time.RFC3339Nano))
		}

		x.logger().Warn("ignoring commit received before the set's newest", "ref", fmt.Sprintf("%x", ref.Hash), "received", plan.at, "newest", newest.ReceivedAt)
		return commitPlan{placement: behind}, nil
	}

	at := plan.at
	diff := &treeDiff{read: c, store: x.reads(), setID: setID, at: at, tolerant: !strict}

	root := &proto.Tree{}
	if treeObj == nil {
		diff.holes = append(diff.holes, "")
	} else {
		root, err = diff.flatten(ctx, "", commit.Tree, treeObj.GetTree())
	}

	if err == nil {
		err = diff.walk(ctx, root)
	}

	steps.Lap("walk")

	if err != nil {
		if strict {
			return commitPlan{}, fmt.Errorf("%w: %v", backup.ErrDanglingRef, err)
		}

		x.logger().Warn("ignoring commit, traversing its tree failed", "ref", fmt.Sprintf("%x", ref.Hash), "err", err)
		return commitPlan{}, nil
	}

	plan.diff = diff
	if newest != nil {
		plan.newest = newest.ID
	}

	return plan, nil
}

// skipCommit reports whether a commit is already indexed or tombstoned,
// which strict mode refuses, and refuses a strict commit to a set that no
// longer takes them. It asks the database once.
func (x *Index) skipCommit(ctx context.Context, c *ent.Client, commit *proto.Commit, ref *proto.Ref, setID int64, strict bool) (bool, error) {
	arg := func(n int) string {
		if x.dialect == dialect.Postgres {
			return fmt.Sprintf("$%d", n)
		}

		return "?"
	}

	query := fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM %s WHERE %s = %s), EXISTS (SELECT 1 FROM %s WHERE %s = %s), (SELECT %s FROM %s WHERE %s = %s)`,
		commitrow.Table, commitrow.FieldRef, arg(1), deletedref.Table, deletedref.FieldRef, arg(2), set.FieldState, set.Table, set.FieldID, arg(3))

	rows, err := c.QueryContext(ctx, query, ref.Hash, ref.Hash, setID)
	if err != nil {
		return true, err
	}
	defer rows.Close()

	var exists, deleted bool
	var state stdsql.NullString

	if !rows.Next() {
		return true, errors.Join(errors.New("asking whether to skip a commit returned nothing"), rows.Err())
	}

	if err := rows.Scan(&exists, &deleted, &state); err != nil {
		return true, err
	}

	if err := rows.Close(); err != nil {
		return true, err
	}

	if exists {
		return true, nil
	}

	if deleted {
		if strict {
			return true, fmt.Errorf("%w: commit %x", backup.ErrTombstoned, ref.Hash)
		}

		return true, nil
	}

	if !strict {
		return false, nil
	}

	if !state.Valid {
		return true, fmt.Errorf("%w: set %d", backup.ErrNotFound, setID)
	}

	if set.State(state.String) != set.StateActive {
		return true, fmt.Errorf("%w: set %q", backup.ErrSetClosed, commit.GetBackupSet())
	}

	return false, nil
}

func newestCommit(ctx context.Context, c *ent.Client, setID int64) (*ent.CommitRow, error) {
	newest, err := c.CommitRow.Query().Where(commitrow.SetID(setID)).Order(ent.Desc(commitrow.FieldReceivedAt)).First(ctx)
	if ent.IsNotFound(err) {
		return nil, nil
	}

	return newest, err
}

// applyCommit writes a planned commit under its set's lock, provided the
// set's newest commit is still the one the plan assumed, and reports
// whether it indexed it.
func (x *Index) applyCommit(ctx context.Context, commit *proto.Commit, ref *proto.Ref, setID int64, strict, evaluate bool, plan commitPlan, steps *laps.Laps) (bool, error) {
	indexed := false
	diff, at, newestID := plan.diff, plan.at, plan.newest

	err := x.tx(ctx, func(tx *ent.Tx) error {
		c := tx.Client()

		if _, err := x.lockSet(ctx, tx, setID); err != nil {
			return err
		}

		steps.Lap("lock")

		skip, err := x.skipCommit(ctx, c, commit, ref, setID, strict)
		if err != nil || skip {
			return err
		}

		newest, err := newestCommit(ctx, c, setID)
		if err != nil {
			return err
		}

		if (newest == nil && newestID != 0) || (newest != nil && newest.ID != newestID) {
			return errSetMoved
		}

		steps.Lap("recheck")

		if err := diff.apply(ctx, c); err != nil {
			return err
		}

		steps.Lap("writes")

		// the commit is the set's newest, received after every other
		size, files, err := openSize(ctx, c, setID)
		if err != nil {
			return err
		}

		steps.Lap("size")

		row := c.CommitRow.Create().SetRef(ref.Hash).SetTimestamp(time.Unix(commit.Timestamp, 0).UTC()).SetReceivedAt(at).
			SetTree(commit.Tree.Hash).SetParent(commit.GetParent().GetHash()).SetAgentID(commit.GetAgentId()).
			SetScanStartNs(commit.GetScanStartNs()).SetPolicyVersion(commit.GetPolicyVersion()).SetConsistent(commit.GetConsistent()).
			SetSetID(setID).SetPartial(commit.Partial).SetMetadata(commit.GetMetadata()).SetLogicalSize(size).SetFileCount(files).
			SetIncomplete(commit.GetIncomplete() || len(diff.holes)+len(diff.gaps) > 0)
		if commit.KeyId != nil {
			row.SetKeyID(hex.EncodeToString(commit.KeyId))
		}

		err = row.Exec(ctx)
		if err != nil {
			return err
		}

		err = setRef(ctx, c, setID, ref.Hash)
		if err != nil {
			return err
		}

		steps.Lap("row")

		err = x.clearDamage(ctx, tx, setID, commit.Partial, commit.GetScanStartNs())
		if err != nil {
			return err
		}

		steps.Lap("damage")

		if len(diff.holes)+len(diff.gaps) > 0 {
			x.logger().Warn("indexed commit around objects the store lost", "ref", fmt.Sprintf("%x", ref.Hash),
				"unlisted directories", diff.holes, "unreadable files", diff.gaps)
		}

		if len(diff.holes) > 0 {
			err = tx.Set.UpdateOneID(setID).SetRescan(true).Exec(ctx)
			if err != nil {
				return err
			}
		}

		if evaluate {
			err = x.evaluateSet(ctx, tx, setID, x.now())
			if err != nil {
				return err
			}

			steps.Lap("retention")
		}

		indexed = true

		return nil
	})

	steps.Lap("commit")

	return indexed, err
}

// treeDiff walks the changed directories of a commit against the open
// rows of its set as read reads them, a level of the tree at a time, and
// plans closing the rows of versions that changed or vanished at the
// commit's receipt time and opening rows for new versions, for apply to
// write.
type treeDiff struct {
	read  *ent.Client
	store backup.ObjectStore
	setID int64
	at    time.Time

	// closeTrees and closeFiles are the paths whose open row closes,
	// closeDirs the directories whose open rows all close
	closeTrees []string
	closeFiles []string
	closeDirs  []string
	trees      []treeRow
	files      []fileRow
	refs       [][]byte
	referenced map[string]bool

	// tolerant walks around objects the store no longer holds, recording
	// where in holes (directories whose content is unknown) and gaps
	// (files whose content is gone), instead of failing
	tolerant bool
	holes    []string
	gaps     []string
}

// treeRow and fileRow are versions a commit opens.
type treeRow struct {
	path, dir string
	ref       []byte
}

type fileRow struct {
	path, dir string
	node      *proto.TreeNode
}

// level is a directory to diff and its flattened tree.
type level struct {
	dir  string
	tree *proto.Tree
}

// descent is a node below a level the walk reads on.
type descent struct {
	path string
	node *proto.TreeNode
}

// diffWorkers bounds the objects a level of the walk reads at once.
const diffWorkers = 16

// missing reports whether err is a lost object the walk goes around.
func (d *treeDiff) missing(err error) bool {
	return d.tolerant && errors.Is(err, backup.ErrNotFound)
}

// apply makes the planned writes through c. The closes go first: each
// matches every open row of its path or directory, which must not take
// in the rows the commit opens.
func (d *treeDiff) apply(ctx context.Context, c *ent.Client) error {
	err := inBatches(d.closeTrees, func(paths []string) error {
		return c.Tree.Update().Where(tree.SetID(d.setID), tree.PathIn(paths...), tree.ValidUntilIsNil()).SetValidUntil(d.at).Exec(ctx)
	})
	if err != nil {
		return err
	}

	err = inBatches(d.closeFiles, func(paths []string) error {
		return c.File.Update().Where(file.SetID(d.setID), file.PathIn(paths...), file.ValidUntilIsNil()).SetValidUntil(d.at).Exec(ctx)
	})
	if err != nil {
		return err
	}

	err = inBatches(d.closeDirs, func(dirs []string) error {
		err := c.File.Update().Where(file.SetID(d.setID), file.DirIn(dirs...), file.ValidUntilIsNil()).SetValidUntil(d.at).Exec(ctx)
		if err != nil {
			return err
		}

		return c.Tree.Update().Where(tree.SetID(d.setID), tree.DirIn(dirs...), tree.ValidUntilIsNil()).SetValidUntil(d.at).Exec(ctx)
	})
	if err != nil {
		return err
	}

	err = inBatches(d.trees, func(rows []treeRow) error {
		builders := make([]*ent.TreeCreate, len(rows))
		for i, r := range rows {
			builders[i] = c.Tree.Create().SetSetID(d.setID).SetPath(r.path).SetDir(r.dir).SetValidFrom(d.at).SetRef(r.ref)
		}

		return c.Tree.CreateBulk(builders...).Exec(ctx)
	})
	if err != nil {
		return err
	}

	err = inBatches(d.files, func(rows []fileRow) error {
		builders := make([]*ent.FileCreate, len(rows))
		for i, r := range rows {
			info := r.node.GetStat()
			builders[i] = c.File.Create().SetSetID(d.setID).SetPath(r.path).SetDir(r.dir).SetValidFrom(d.at).SetRef(r.node.GetRef().GetHash()).
				SetMtimeNs(info.GetMtimeNs()).SetMode(info.GetMode()).SetUser(proto.PathComponent(info.GetUser())).SetGroup(proto.PathComponent(info.GetGroup())).SetSize(info.GetSize()).
				SetType(uint32(info.GetType())).SetLinkTarget(info.GetLinkTarget())
		}

		return c.File.CreateBulk(builders...).Exec(ctx)
	})
	if err != nil {
		return err
	}

	return addSetRefs(ctx, c, d.setID, d.refs)
}

// ref plans recording that the set references an object, which makes it
// readable.
func (d *treeDiff) ref(hash []byte) {
	if d.referenced == nil {
		d.referenced = make(map[string]bool)
	}

	if !d.referenced[string(hash)] {
		d.referenced[string(hash)] = true
		d.refs = append(d.refs, hash)
	}
}

func setRef(ctx context.Context, c *ent.Client, setID int64, hash []byte) error {
	return ignoreNoRows(c.SetRef.Create().SetSetID(setID).SetRef(hash).OnConflict().DoNothing().Exec(ctx))
}

// flatten records a tree and its split trees and returns the flat node
// list, like backup.LoadTree.
func (d *treeDiff) flatten(ctx context.Context, dir string, ref *proto.Ref, t *proto.Tree) (*proto.Tree, error) {
	d.ref(ref.GetHash())

	if len(t.Splits) == 0 {
		return t, nil
	}

	objects, errs := backup.ReadObjects(ctx, d.store, t.Splits)

	flat := &proto.Tree{}
	for i, split := range t.Splits {
		obj, err := objects[i], errs[i]
		if d.missing(err) {
			d.holes = append(d.holes, dir)
			continue
		}

		if err != nil {
			return nil, err
		}

		if obj.GetTree() == nil {
			return nil, fmt.Errorf("split %x is not a tree", split.GetHash())
		}

		sub, err := d.flatten(ctx, dir, split, obj.GetTree())
		if err != nil {
			return nil, err
		}

		flat.Nodes = append(flat.Nodes, sub.Nodes...)
	}

	return flat, nil
}

// refLarge records the sub-file objects the large files name, reading
// the files together and their splits in parallel.
func (d *treeDiff) refLarge(ctx context.Context, large []descent) error {
	subs := make([][][]byte, len(large))
	lost := make([]bool, len(large))

	refs := make([]*proto.Ref, len(large))
	for i, f := range large {
		refs[i] = f.node.GetRef()
	}

	objects, errs := backup.ReadObjects(ctx, d.store, refs)

	grp, gctx := errgroup.WithContext(ctx)
	grp.SetLimit(diffWorkers)

	for i := range large {
		grp.Go(func() error {
			obj, err := objects[i], errs[i]
			if d.missing(err) {
				lost[i] = true
				return nil
			}

			if err != nil {
				return err
			}

			err = backup.SubFiles(gctx, d.store, obj.GetFile(), func(split *proto.Ref) error {
				subs[i] = append(subs[i], split.GetHash())
				return nil
			})
			if d.missing(err) {
				lost[i] = true
				return nil
			}

			return err
		})
	}

	if err := grp.Wait(); err != nil {
		return err
	}

	for i, f := range large {
		for _, sub := range subs[i] {
			d.ref(sub)
		}

		if lost[i] {
			d.gaps = append(d.gaps, f.path)
		}
	}

	return nil
}

// sameFile reports whether an open row still describes the node, so its
// version can stay open. A symlink has no ref, so its target decides.
func sameFile(row *ent.File, node *proto.TreeNode) bool {
	info := node.GetStat()

	return bytes.Equal(row.Ref, node.GetRef().GetHash()) &&
		row.MtimeNs == info.GetMtimeNs() &&
		row.Type == uint32(info.GetType()) &&
		bytes.Equal(row.LinkTarget, info.GetLinkTarget())
}

// openTrees returns the refs of the open trees rows of the directories,
// by directory and path.
func (d *treeDiff) openTrees(ctx context.Context, dirs []string) (map[string]map[string][]byte, error) {
	open := make(map[string]map[string][]byte, len(dirs))

	err := inBatches(dirs, func(batch []string) error {
		rows, err := d.read.Tree.Query().Where(tree.SetID(d.setID), tree.DirIn(batch...), tree.ValidUntilIsNil()).All(ctx)
		if err != nil {
			return err
		}

		for _, row := range rows {
			if open[row.Dir] == nil {
				open[row.Dir] = make(map[string][]byte)
			}

			open[row.Dir][row.Path] = row.Ref
		}

		return nil
	})

	return open, err
}

// openFiles returns the open files rows of the directories, by directory
// and path.
func (d *treeDiff) openFiles(ctx context.Context, dirs []string) (map[string]map[string]*ent.File, error) {
	open := make(map[string]map[string]*ent.File, len(dirs))

	err := inBatches(dirs, func(batch []string) error {
		rows, err := d.read.File.Query().Where(file.SetID(d.setID), file.DirIn(batch...), file.ValidUntilIsNil()).All(ctx)
		if err != nil {
			return err
		}

		for _, row := range rows {
			if open[row.Dir] == nil {
				open[row.Dir] = make(map[string]*ent.File)
			}

			open[row.Dir][row.Path] = row
		}

		return nil
	})

	return open, err
}

// closeSubtrees plans closing every open row below the directories.
func (d *treeDiff) closeSubtrees(ctx context.Context, dirs []string) error {
	for len(dirs) > 0 {
		d.closeDirs = append(d.closeDirs, dirs...)

		children, err := d.openTrees(ctx, dirs)
		if err != nil {
			return err
		}

		dirs = nil
		for _, open := range children {
			for child := range open {
				dirs = append(dirs, child)
			}
		}
	}

	return nil
}

// walk plans the commit's flattened root tree a level of directories at
// a time: their open rows are read together and their subtrees in
// parallel.
func (d *treeDiff) walk(ctx context.Context, root *proto.Tree) error {
	next := []level{{dir: "", tree: root}}

	phase := progress.Start(ctx, progress.OpCommit, progress.PhaseIndex, 0, 0)
	defer phase.Finish()

	for len(next) > 0 {
		current := next
		next = nil

		phase.Add(int64(len(current)), 0)

		dirs := make([]string, len(current))
		for i, l := range current {
			dirs[i] = l.dir
		}

		trees, err := d.openTrees(ctx, dirs)
		if err != nil {
			return err
		}

		files, err := d.openFiles(ctx, dirs)
		if err != nil {
			return err
		}

		var descend, large []descent
		var vanished []string

		for _, l := range current {
			down, big, gone := d.dir(l, trees[l.dir], files[l.dir])
			descend = append(descend, down...)
			large = append(large, big...)
			vanished = append(vanished, gone...)
		}

		if err := d.refLarge(ctx, large); err != nil {
			return err
		}

		if err := d.closeSubtrees(ctx, vanished); err != nil {
			return err
		}

		refs := make([]*proto.Ref, len(descend))
		for i, sub := range descend {
			refs[i] = sub.node.GetRef()
		}

		subtrees, errs := backup.ReadObjects(ctx, d.store, refs)

		for i, sub := range descend {
			switch err := errs[i]; {
			case d.missing(err):
				subtrees[i] = nil
			case err != nil:
				return err
			case subtrees[i].GetTree() == nil:
				return fmt.Errorf("%x is not a tree", sub.node.GetRef().GetHash())
			}
		}

		var unlisted []string

		for i, sub := range descend {
			if subtrees[i] == nil {
				d.holes = append(d.holes, sub.path)
				unlisted = append(unlisted, sub.path)

				continue
			}

			flat, err := d.flatten(ctx, sub.path, sub.node.GetRef(), subtrees[i].GetTree())
			if err != nil {
				return err
			}

			next = append(next, level{dir: sub.path, tree: flat})
		}

		if err := d.closeSubtrees(ctx, unlisted); err != nil {
			return err
		}
	}

	return nil
}

// dir plans one directory against its open rows and returns the
// directories to descend into, the large files to read and the
// directories that vanished.
func (d *treeDiff) dir(l level, trees map[string][]byte, files map[string]*ent.File) (descend, large []descent, vanished []string) {
	seen := make(map[string]bool, len(l.tree.GetNodes()))

	for _, node := range l.tree.GetNodes() {
		info := node.GetStat()

		child := proto.JoinPath(l.dir, info.GetName())

		// a lost directory lists nothing, so what it held vanished
		if info.GetLost() && info.IsDir() {
			continue
		}

		seen[child] = true

		for _, ref := range info.MetadataRefs() {
			d.ref(ref.GetHash())
		}

		for _, stream := range info.GetStreams() {
			if stream.GetSize() >= backup.SplitFileSize {
				large = append(large, descent{path: child, node: &proto.TreeNode{Ref: stream.GetRef()}})
			}
		}

		if info.IsDir() {
			cur, open := trees[child]
			if open && bytes.Equal(cur, node.GetRef().GetHash()) {
				continue
			}

			if open {
				d.closeTrees = append(d.closeTrees, child)
			}

			d.trees = append(d.trees, treeRow{path: child, dir: l.dir, ref: node.GetRef().GetHash()})
			descend = append(descend, descent{path: child, node: node})

			continue
		}

		cur, open := files[child]
		if open && sameFile(cur, node) {
			continue
		}

		if open {
			d.closeFiles = append(d.closeFiles, child)
		}

		d.files = append(d.files, fileRow{path: child, dir: l.dir, node: node})

		if info.GetType() == proto.NodeType_NODE_SYMLINK || info.GetLost() {
			continue
		}

		d.ref(node.GetRef().GetHash())

		if info.GetSize() >= backup.SplitFileSize {
			large = append(large, descent{path: child, node: node})
		}
	}

	for p := range trees {
		if !seen[p] {
			d.closeTrees = append(d.closeTrees, p)
			vanished = append(vanished, p)
		}
	}

	for p := range files {
		if !seen[p] {
			d.closeFiles = append(d.closeFiles, p)
		}
	}

	return descend, large, vanished
}

// indexPin caches a pin and revives its target commit. A pin of a
// tombstoned commit is refused in strict mode.
func (x *Index) indexPin(ctx context.Context, p *proto.Pin, ref *proto.Ref, strict bool) error {
	return x.tx(ctx, func(tx *ent.Tx) error {
		c := tx.Client()
		target := p.GetTarget().GetHash()

		setID, tombstoned, err := x.lockCommit(ctx, tx, target)
		if err != nil && !errors.Is(err, backup.ErrNotFound) {
			return err
		}

		if !tombstoned {
			tombstoned, err = isDeleted(ctx, c, target)
			if err != nil {
				return err
			}
		}

		if tombstoned {
			if strict {
				return fmt.Errorf("%w: commit %x", backup.ErrTombstoned, target)
			}

			x.logger().Warn("pin targets a tombstoned commit", "pin", fmt.Sprintf("%x", ref.GetHash()), "commit", fmt.Sprintf("%x", target))
		}

		if setID != 0 && strict {
			err = x.checkPinScope(ctx, c, setID)
			if err != nil {
				return err
			}
		}

		err = ignoreNoRows(c.Pin.Create().SetRef(ref.Hash).SetTarget(target).SetReceivedAt(time.Unix(0, p.GetReceivedAtNs()).UTC()).
			SetMetadata(p.GetMetadata()).OnConflict().DoNothing().Exec(ctx))
		if err != nil {
			return err
		}

		if setID != 0 && !tombstoned {
			err = c.CommitRow.Update().Where(commitrow.Ref(target)).ClearRetireAt().ClearRetirePolicy().ClearDeletedAt().ClearExpiresAt().Exec(ctx)
			if err != nil {
				return err
			}

			if strict {
				err = x.evaluateSet(ctx, tx, setID, x.now())
				if err != nil {
					return err
				}
			}
		}

		return nil
	})
}

// checkPinScope verifies that a pin's target is in a set that still
// accepts changes.
func (x *Index) checkPinScope(ctx context.Context, c *ent.Client, setID int64) error {
	cfg, err := x.loadSetConfig(ctx, c, setID)
	if err != nil {
		return err
	}

	if cfg.state != set.StateActive {
		return fmt.Errorf("%w: set %q", backup.ErrSetClosed, cfg.name)
	}

	return nil
}

// lockCommit takes the commit's row lock and reports its set and whether
// it is tombstoned, or backup.ErrNotFound.
func (x *Index) lockCommit(ctx context.Context, tx *ent.Tx, ref []byte) (int64, bool, error) {
	row, err := x.lockCommitRow(ctx, tx, ref)
	if err != nil {
		return 0, false, err
	}

	return row.SetID, row.TombstonedAt != nil, nil
}

// lockCommitRow takes the commit's set lock and then its row lock, the
// order every path that goes on to evaluate the set must follow; the
// commit's absence is backup.ErrNotFound.
func (x *Index) lockCommitRow(ctx context.Context, tx *ent.Tx, ref []byte) (*ent.CommitRow, error) {
	unlocked, err := tx.CommitRow.Query().Where(commitrow.Ref(ref)).Select(commitrow.FieldSetID).Only(ctx)
	if ent.IsNotFound(err) {
		return nil, backup.ErrNotFound
	}

	if err != nil {
		return nil, err
	}

	if _, err := x.lockSet(ctx, tx, unlocked.SetID); err != nil {
		return nil, err
	}

	row, err := forUpdate(x, tx.CommitRow.Query().Where(commitrow.Ref(ref))).Only(ctx)
	if ent.IsNotFound(err) {
		return nil, backup.ErrNotFound
	}

	return row, err
}

// ReIndex rebuilds the caches from the archives in three passes:
// tombstones, then pins, then commits per set in receipt order. Sets it
// has to create come up with retention paused.
func (x *Index) ReIndex(ctx context.Context) (backup.ReIndexReport, error) {
	var report backup.ReIndexReport
	err := x.reIndex(ctx, &report)

	return report, err
}

func (x *Index) reIndex(ctx context.Context, report *backup.ReIndexReport) error {
	if _, err := x.client.WalkedArchive.Delete().Exec(ctx); err != nil {
		return err
	}

	// sets that lose a commit to a tombstone this database had not seen
	touched := map[int64][][]byte{}

	phase := progress.Start(ctx, progress.OpReIndex, progress.PhaseTombstones, 0, 0)

	if hw, ok := x.ObjectStore.(backup.HeaderWalker); ok {
		var found []foundTombstone

		apply := func() error {
			marked, err := x.applyTombstones(ctx, found)
			for setID, refs := range marked {
				touched[setID] = append(touched[setID], refs...)
			}

			found = found[:0]

			return err
		}

		err := hw.WalkHeaders(ctx, proto.ObjectType_TOMBSTONE, func(hdr *proto.ObjectHeader) error {
			at := x.now()
			if hdr.GetTimestamp() != nil {
				at = hdr.GetTimestamp().AsTime()
			}

			found = append(found, foundTombstone{ref: bytes.Clone(hdr.GetTombstoneFor().GetHash()), at: at})
			phase.Add(1, 0)
			if len(found) < objectBatch {
				return nil
			}

			return apply()
		})
		if err == nil && len(found) > 0 {
			err = apply()
		}

		if err != nil && !errors.Is(err, backup.ErrNotImplemented) {
			return err
		}
	} else {
		x.logger().Warn("store cannot walk headers, the rebuild does not honour tombstones", "store", fmt.Sprintf("%T", x.ObjectStore))
	}

	for setID, refs := range touched {
		if err := x.pruneRefs(ctx, setID, refs); err != nil {
			return err
		}

		if err := x.dropDeadRows(ctx, setID); err != nil {
			return err
		}
	}

	phase.Finish()
	phase = progress.Start(ctx, progress.OpReIndex, progress.PhasePins, 0, 0)

	err := x.ObjectStore.Walk(ctx, true, proto.ObjectType_PIN, func(obj *proto.Object) error {
		phase.Add(1, 0)
		ref := obj.Ref()

		gone, err := isDeleted(ctx, x.client, ref.Hash)
		if err != nil || gone {
			return err
		}

		return x.indexPin(ctx, obj.GetPin(), ref, false)
	})
	if err != nil {
		return err
	}

	phase.Finish()
	phase = progress.Start(ctx, progress.OpReIndex, progress.PhaseCommits, 0, 0)

	var commits []pending

	err = x.ObjectStore.Walk(ctx, true, proto.ObjectType_COMMIT, func(obj *proto.Object) error {
		phase.Add(1, 0)
		ref := obj.Ref()

		gone, err := isDeleted(ctx, x.client, ref.Hash)
		if err == nil && gone {
			gone, err = x.stillDeleted(ctx, ref.Hash)
		}

		if err != nil || gone {
			return err
		}

		if keeper, ok := storeAs[backup.HeadKeeper](x.ObjectStore); ok {
			if err := keeper.AdvanceHead(obj); err != nil {
				return err
			}
		}

		commits = append(commits, pending{commit: obj.GetCommit(), ref: ref})

		return nil
	})
	if err != nil {
		return err
	}

	phase.Finish()

	groups := groupBySet(commits)
	phase = progress.Start(ctx, progress.OpReIndex, progress.PhaseSets, int64(len(groups)), 0)

	for _, group := range groups {
		target, err := x.groupSet(ctx, group, false)
		if err != nil {
			return err
		}

		counts, err := x.indexGroup(ctx, target, group)
		if err != nil {
			return err
		}

		report.Tied += counts[tied]
		report.Behind += counts[behind]
		if target.Placeholder {
			report.Unnamed += counts[inOrder] + counts[tied]
		}

		phase.Add(1, 0)
	}

	phase.Finish()
	phase = progress.Start(ctx, progress.OpReIndex, progress.PhasePolicies, 0, 0)

	err = x.replayPolicies(ctx)
	if err != nil {
		return err
	}

	err = x.evaluateAll(ctx)
	if err != nil {
		return err
	}

	err = x.resetSetSequence(ctx)
	if err != nil {
		return err
	}

	phase.Finish()
	phase = progress.Start(ctx, progress.OpReIndex, progress.PhaseDamage, 0, 0)

	if err := x.findDamage(ctx); err != nil {
		return err
	}
	phase.Finish()

	return nil
}

// resetSetSequence moves the set id sequence past the ids a rebuild
// recreated, where the database generates ids from a sequence.
func (x *Index) resetSetSequence(ctx context.Context) error {
	if !x.locking {
		return nil
	}

	_, err := x.client.ExecContext(ctx, `SELECT setval(pg_get_serial_sequence('sets', 'id'), GREATEST((SELECT COALESCE(MAX(id), 1) FROM sets), 1))`)

	return err
}

// foundTombstone is a tombstone a rebuild found in the archives: the ref
// it names and when it was written.
type foundTombstone struct {
	ref []byte
	at  time.Time
}

// applyTombstones records tombstones found in the archives and marks the
// commit or pin rows they name when this database still has them live,
// unless a commit was revived since. Of several tombstones of one ref the
// first counts. It returns the commits it marked, by set.
func (x *Index) applyTombstones(ctx context.Context, found []foundTombstone) (map[int64][][]byte, error) {
	at := make(map[string]time.Time, len(found))
	var refs [][]byte

	for _, t := range found {
		if _, ok := at[string(t.ref)]; !ok {
			at[string(t.ref)] = t.at
			refs = append(refs, t.ref)
		}
	}

	live, err := x.client.CommitRow.Query().Where(commitrow.RefIn(refs...), commitrow.TombstonedAtIsNil()).Select(commitrow.FieldRef).All(ctx)
	if err != nil {
		return nil, err
	}

	revived := make(map[string]bool)
	for _, row := range live {
		again, err := x.revived(ctx, row.Ref)
		if err != nil {
			return nil, err
		}

		revived[string(row.Ref)] = again
	}

	applied := refs[:0:0]
	for _, ref := range refs {
		if !revived[string(ref)] {
			applied = append(applied, ref)
		}
	}

	if len(applied) == 0 {
		return nil, nil
	}

	marked := map[int64][][]byte{}

	err = x.tx(ctx, func(tx *ent.Tx) error {
		c := tx.Client()
		marked = map[int64][][]byte{}

		deleted := make([]*ent.DeletedRefCreate, len(applied))
		for i, ref := range applied {
			deleted[i] = c.DeletedRef.Create().SetRef(ref).SetTombstonedAt(at[string(ref)])
		}

		if err := ignoreNoRows(c.DeletedRef.CreateBulk(deleted...).OnConflict().DoNothing().Exec(ctx)); err != nil {
			return err
		}

		rows, err := c.CommitRow.Query().Where(commitrow.RefIn(applied...), commitrow.TombstonedAtIsNil()).All(ctx)
		if err != nil {
			return err
		}

		var gone [][]byte
		for _, row := range rows {
			if err := c.CommitRow.UpdateOneID(row.ID).SetTombstonedAt(at[string(row.Ref)]).ClearPresence().Exec(ctx); err != nil {
				return err
			}

			marked[row.SetID] = append(marked[row.SetID], row.Ref)
			gone = append(gone, row.Ref)
		}

		if len(gone) > 0 {
			if _, err := c.SetRef.Delete().Where(setref.RefIn(gone...)).Exec(ctx); err != nil {
				return err
			}
		}

		pins, err := c.Pin.Query().Where(pin.RefIn(applied...), pin.DeletedAtIsNil()).All(ctx)
		if err != nil {
			return err
		}

		for _, p := range pins {
			if err := c.Pin.UpdateOneID(p.ID).SetDeletedAt(at[string(p.Ref)]).Exec(ctx); err != nil {
				return err
			}
		}

		return nil
	})

	return marked, err
}

// References implements backup.RefScope: a commit, tree or file ref is
// readable once an indexed commit of any set the caller sees names it.
func (x *Index) References(ctx context.Context, ref *proto.Ref) (bool, error) {
	// joins sets, so a set hidden from the caller does not make it readable;
	// HasSet would only test the column
	return x.client.SetRef.Query().Where(setref.Ref(ref.GetHash()), setref.HasSetWith()).Exist(ctx)
}

// ReferencesAll implements backup.RefScope in a query per batch.
func (x *Index) ReferencesAll(ctx context.Context, refs []*proto.Ref) ([]bool, error) {
	hashes := make([][]byte, len(refs))
	for i, ref := range refs {
		hashes[i] = ref.GetHash()
	}

	found := make(map[string]bool)

	err := inBatches(hashes, func(batch [][]byte) error {
		var rows []struct {
			Ref []byte `json:"ref"`
		}

		err := x.client.SetRef.Query().Where(setref.RefIn(batch...), setref.HasSetWith()).Unique(true).Select(setref.FieldRef).Scan(ctx, &rows)
		for _, row := range rows {
			found[string(row.Ref)] = true
		}

		return err
	})
	if err != nil {
		return nil, err
	}

	referenced := make([]bool, len(refs))
	for i, hash := range hashes {
		referenced[i] = found[string(hash)]
	}

	return referenced, nil
}

// Reachable reports whether a commit of one of the named sets references
// ref, which is what a caller limited to those sets may read.
func (x *Index) Reachable(ctx context.Context, sets []string, ref *proto.Ref) (bool, error) {
	return x.client.SetRef.Query().Where(setref.Ref(ref.GetHash()), setref.HasSetWith(set.NameIn(sets...))).Exist(ctx)
}

// liveCommit is the condition under which a commits row is offered.
func liveCommit() predicate.CommitRow {
	return commitrow.And(commitrow.TombstonedAtIsNil(), commitrow.RetireAtIsNil(), commitrow.DeletedAtIsNil())
}

// heldByCommit is the condition that a versions row's validity range holds
// a commit of the set that passes filter. The files and trees tables name
// their validity columns alike, so it serves both.
func heldByCommit(filter func(t *entsql.SelectTable) *entsql.Predicate) func(s *entsql.Selector) {
	return func(s *entsql.Selector) {
		c := entsql.Table(commitrow.Table)
		s.Where(entsql.Exists(entsql.Select().From(c).Where(entsql.And(
			entsql.ColumnsEQ(c.C(commitrow.FieldSetID), s.C(file.FieldSetID)),
			filter(c),
			entsql.ColumnsGTE(c.C(commitrow.FieldReceivedAt), s.C(file.FieldValidFrom)),
			entsql.Or(entsql.IsNull(s.C(file.FieldValidUntil)), entsql.ColumnsLT(c.C(commitrow.FieldReceivedAt), s.C(file.FieldValidUntil))),
		))))
	}
}

func liveCommitColumns(c *entsql.SelectTable) *entsql.Predicate {
	return entsql.And(entsql.IsNull(c.C(commitrow.FieldTombstonedAt)), entsql.IsNull(c.C(commitrow.FieldRetireAt)), entsql.IsNull(c.C(commitrow.FieldDeletedAt)))
}

// FileInfo lists the versions of a path that a live commit of the set
// contains, newest first.
func (x *Index) FileInfo(ctx context.Context, backupSet string, name string, notAfter time.Time, count int) ([]*proto.TreeNode, error) {
	setID, err := findSet(ctx, x.client, backupSet)
	if errors.Is(err, backup.ErrNotFound) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	rows, err := x.client.File.Query().
		Where(file.SetID(setID), file.Path(name), file.ValidFromLTE(notAfter.UTC()), predicate.File(heldByCommit(liveCommitColumns))).
		Order(ent.Desc(file.FieldValidFrom)).Limit(count).All(ctx)
	if err != nil {
		return nil, err
	}

	return mapAll(rows, m.TreeNode), nil
}

// ReadDir lists what a set held directly under dir at notAfter, sorted by
// name. Directory entries carry a name and a ref only.
func (x *Index) ReadDir(ctx context.Context, backupSet string, dir string, notAfter time.Time) ([]*proto.TreeNode, error) {
	setID, err := findSet(ctx, x.client, backupSet)
	if errors.Is(err, backup.ErrNotFound) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	at := notAfter.UTC()

	fileRows, err := x.client.File.Query().Where(
		file.SetID(setID), file.Dir(dir), file.ValidFromLTE(at),
		file.Or(file.ValidUntilIsNil(), file.ValidUntilGT(at)),
		predicate.File(heldByCommit(liveCommitColumns)),
	).All(ctx)
	if err != nil {
		return nil, err
	}

	treeRows, err := x.client.Tree.Query().Where(
		tree.SetID(setID), tree.Dir(dir), tree.ValidFromLTE(at),
		tree.Or(tree.ValidUntilIsNil(), tree.ValidUntilGT(at)),
		predicate.Tree(heldByCommit(liveCommitColumns)),
	).All(ctx)
	if err != nil {
		return nil, err
	}

	entries := mapAll(fileRows, m.TreeNode)
	for _, row := range treeRows {
		entries = append(entries, directory(row))
	}

	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].GetStat().GetName(), entries[j].GetStat().GetName()) < 0
	})

	return entries, nil
}

// CommitInfo lists the live, complete commits of a set, newest first.
func (x *Index) CommitInfo(ctx context.Context, backupSet string, notAfter time.Time, count int) ([]*proto.Commit, error) {
	rows, err := x.commitInfo(ctx, backupSet, notAfter, count)
	if err != nil {
		return nil, err
	}

	return mapAll(rows, m.Commit), nil
}

// CommitSizes implements backup.CommitSizer.
func (x *Index) CommitSizes(ctx context.Context, backupSet string, notAfter time.Time, count int) ([]*proto.Commit, []*proto.CommitSize, error) {
	rows, err := x.commitInfo(ctx, backupSet, notAfter, count)
	if err != nil {
		return nil, nil, err
	}

	return mapAll(rows, m.Commit), mapAll(rows, m.CommitSize), nil
}

func (x *Index) commitInfo(ctx context.Context, backupSet string, notAfter time.Time, count int) ([]*ent.CommitRow, error) {
	setID, err := findSet(ctx, x.client, backupSet)
	if errors.Is(err, backup.ErrNotFound) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return x.client.CommitRow.Query().
		Where(commitrow.SetID(setID), commitrow.TimestampLTE(notAfter.UTC()), commitrow.Partial(false), liveCommit()).
		WithSet().Order(ent.Desc(commitrow.FieldReceivedAt)).Limit(count).All(ctx)
}

// CommitDetails is CommitInfo with what the index knows about each commit
// beyond the object, for a caller reporting on a set rather than reading
// it back.
func (x *Index) CommitDetails(ctx context.Context, backupSet string, notAfter time.Time, count int) ([]index.CommitDetail, error) {
	setID, err := findSet(ctx, x.client, backupSet)
	if errors.Is(err, backup.ErrNotFound) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	rows, err := x.client.CommitRow.Query().
		Where(commitrow.SetID(setID), commitrow.TimestampLTE(notAfter.UTC()), commitrow.Partial(false), liveCommit()).
		WithSet().Order(ent.Desc(commitrow.FieldReceivedAt)).Limit(count).All(ctx)
	if err != nil {
		return nil, err
	}

	return mapAll(rows, m.CommitDetail), nil
}

// GetCommitDetail returns the set's live, complete commit ref names, as
// CommitDetails describes it, or backup.ErrNotFound.
func (x *Index) GetCommitDetail(ctx context.Context, backupSet string, ref *proto.Ref) (index.CommitDetail, error) {
	row, err := x.commitOf(ctx, backupSet, ref, liveCommit())
	if err != nil {
		return index.CommitDetail{}, err
	}

	return m.CommitDetail(row), nil
}

// GetTrashedCommit returns the set's commit ref names while it waits in
// the trash, as TrashedCommits describes it, or backup.ErrNotFound.
func (x *Index) GetTrashedCommit(ctx context.Context, backupSet string, ref *proto.Ref) (*proto.TrashedCommit, error) {
	row, err := x.commitOf(ctx, backupSet, ref, commitrow.DeletedAtNotNil(), commitrow.TombstonedAtIsNil())
	if err != nil {
		return nil, err
	}

	return m.TrashedCommit(row), nil
}

// commitOf is the set's complete commit ref names that also passes where.
func (x *Index) commitOf(ctx context.Context, backupSet string, ref *proto.Ref, where ...predicate.CommitRow) (*ent.CommitRow, error) {
	row, err := x.client.CommitRow.Query().
		Where(commitrow.Ref(ref.GetHash()), commitrow.Partial(false), commitrow.HasSetWith(set.Name(backupSet))).
		Where(where...).WithSet().Only(ctx)
	if ent.IsNotFound(err) {
		return nil, fmt.Errorf("%w: %s holds no commit %x", backup.ErrNotFound, backupSet, ref.GetHash())
	}

	return row, err
}

// LatestCommit returns the set's newest commit that is neither tombstoned
// nor deleted, partial or not, or backup.ErrNotFound.
func (x *Index) LatestCommit(ctx context.Context, backupSet string) (*proto.Ref, error) {
	setID, err := findSet(ctx, x.client, backupSet)
	if err != nil {
		return nil, err
	}

	row, err := x.client.CommitRow.Query().Where(commitrow.SetID(setID), commitrow.TombstonedAtIsNil(), commitrow.DeletedAtIsNil()).
		Order(ent.Desc(commitrow.FieldReceivedAt), ent.Desc(commitrow.FieldTimestamp)).Select(commitrow.FieldRef).First(ctx)
	if ent.IsNotFound(err) {
		return nil, backup.ErrNotFound
	}

	if err != nil {
		return nil, err
	}

	return &proto.Ref{Hash: row.Ref}, nil
}

// Presence implements backup.PresenceIndex.
func (x *Index) Presence(ctx context.Context, scope backup.PresenceScope, backupSet string) ([]*proto.PresenceFilter, error) {
	return loadPresence(ctx, x.client, scope, backupSet)
}

func mapAll[S, T any](in []S, f func(S) T) []T {
	if len(in) == 0 {
		return nil
	}

	out := make([]T, len(in))
	for i, s := range in {
		out[i] = f(s)
	}

	return out
}

// logicalSize is what the set's files hold at a moment, and how many
// there are: the recorded size of every file version open then.
// Directories and symlinks carry no content and are left out.
func logicalSize(ctx context.Context, c *ent.Client, setID int64, at time.Time) (int64, int64, error) {
	return fileSizes(ctx, c, file.SetID(setID), file.ValidFromLTE(at), file.Or(file.ValidUntilIsNil(), file.ValidUntilGT(at)))
}

// openSize is logicalSize at a moment after every commit of the set,
// when the versions open then are the ones no commit has closed yet.
func openSize(ctx context.Context, c *ent.Client, setID int64) (int64, int64, error) {
	return fileSizes(ctx, c, file.SetID(setID), file.ValidUntilIsNil())
}

func fileSizes(ctx context.Context, c *ent.Client, where ...predicate.File) (int64, int64, error) {
	var sums []struct {
		Sum   *int64 `sql:"sum"`
		Count int64  `sql:"count"`
	}

	err := c.File.Query().Where(append(where, file.TypeEQ(uint32(proto.NodeType_NODE_FILE)))...).
		Aggregate(ent.Sum(file.FieldSize), ent.Count()).Scan(ctx, &sums)
	if err != nil {
		return 0, 0, err
	}

	if len(sums) == 0 {
		return 0, 0, nil
	}

	return deref(sums[0].Sum), sums[0].Count, nil
}

// FillMissingSizes records the logical size and file count of every
// commit missing either, and reports how many it filled. A tombstoned commit is skipped:
// the file versions it held may already be pruned, so its size can no
// longer be worked out.
func (x *Index) FillMissingSizes(ctx context.Context) (int, error) {
	rows, err := x.client.CommitRow.Query().
		Where(commitrow.Or(commitrow.LogicalSizeIsNil(), commitrow.FileCountIsNil()), commitrow.TombstonedAtIsNil()).All(ctx)
	if err != nil {
		return 0, err
	}

	var filled atomic.Int64

	grp, gctx := errgroup.WithContext(ctx)
	grp.SetLimit(diffWorkers)

	for _, row := range rows {
		grp.Go(func() error {
			size, files, err := logicalSize(gctx, x.client, row.SetID, row.ReceivedAt)
			if err != nil {
				return err
			}

			err = x.client.CommitRow.UpdateOneID(row.ID).SetLogicalSize(size).SetFileCount(files).Exec(gctx)
			if err != nil {
				return err
			}

			filled.Add(1)

			return nil
		})
	}

	err = grp.Wait()

	return int(filled.Load()), err
}
