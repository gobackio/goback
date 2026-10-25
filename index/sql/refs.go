package sql

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/index/sql/ent"
	"github.com/gobackio/goback/index/sql/ent/commitrow"
	"github.com/gobackio/goback/index/sql/ent/predicate"
	"github.com/gobackio/goback/index/sql/ent/setref"
	"github.com/gobackio/goback/index/sql/ent/tree"
	"github.com/gobackio/goback/proto"

	"golang.org/x/sync/errgroup"
)

// pruneRefs drops the set's set_refs rows that only the tombstoned
// commits reach, so their trees and files stop being readable before the
// collector erases them. A ref a live commit shares stays. What a
// directory a live commit holds at the same path reaches is never in
// question; the rest is looked for in the live commits, those next to the
// tombstoned ones first, until all of it is found or every live commit is
// walked. The live commits are walked without the set lock, and the ones
// indexed meanwhile under it, so a concurrent commit cannot lose a ref.
func (x *Index) pruneRefs(ctx context.Context, setID int64, tombstoned [][]byte) error {
	refs := make([]*proto.Ref, len(tombstoned))
	for i, ref := range tombstoned {
		refs[i] = &proto.Ref{Hash: ref}
	}

	objects, errs := x.getAll(ctx, refs)

	var trees []*proto.Ref

	for i, obj := range objects {
		if errors.Is(errs[i], backup.ErrNotFound) {
			continue
		}

		if errs[i] != nil {
			return errs[i]
		}

		if commit := obj.GetCommit(); commit != nil {
			trees = append(trees, commit.GetTree())
		}
	}

	marked := make(map[string]bool, len(tombstoned))
	for _, ref := range tombstoned {
		marked[string(ref)] = true
	}

	live, at, err := setCommits(ctx, x.client, setID, marked)
	if err != nil {
		return err
	}

	shared := make(map[string]bool, len(live))
	for _, commit := range live {
		shared[string(commit.Tree)] = true
	}

	dead := map[string]bool{}
	err = x.walkTombstoned(ctx, setID, trees, shared, func(hash []byte) { dead[string(hash)] = true })
	if err != nil || len(dead) == 0 {
		return err
	}

	visited := map[string]bool{}
	walked := map[string]bool{}
	found := func() bool { return len(dead) == 0 }

	for attempt := range planAttempts {
		if attempt > 0 {
			if live, at, err = setCommits(ctx, x.client, setID, marked); err != nil {
				return err
			}
		}

		for _, group := range nextToFirst(live, at) {
			var trees []*proto.Ref

			for _, commit := range group {
				if !walked[string(commit.Ref)] {
					walked[string(commit.Ref)] = true
					trees = append(trees, &proto.Ref{Hash: commit.Tree})
				}
			}

			// what a live commit reaches only through an object the store
			// lost cannot be read through it either
			err = x.walkRefs(ctx, trees, visited, true, func(hash []byte) { delete(dead, string(hash)) }, found)
			if err != nil {
				return fmt.Errorf("walking live commits: %w", err)
			}

			if found() {
				return nil
			}
		}

		err := x.dropRefs(ctx, setID, dead, walked)
		if !errors.Is(err, errSetMoved) {
			return err
		}
	}

	// the set keeps taking commits: the next retirement prunes what is left
	return nil
}

// setCommits returns the set's live commits in receipt order, and when each
// of its tombstoned commits that marked names was received.
func setCommits(ctx context.Context, c *ent.Client, setID int64, marked map[string]bool) ([]*ent.CommitRow, []time.Time, error) {
	rows, err := c.CommitRow.Query().Where(commitrow.SetID(setID)).
		Order(ent.Asc(commitrow.FieldReceivedAt), ent.Asc(commitrow.FieldID)).
		Select(commitrow.FieldRef, commitrow.FieldTree, commitrow.FieldReceivedAt, commitrow.FieldTombstonedAt).All(ctx)
	if err != nil {
		return nil, nil, err
	}

	var live []*ent.CommitRow
	var at []time.Time

	for _, row := range rows {
		switch {
		case row.TombstonedAt == nil:
			live = append(live, row)
		case marked[string(row.Ref)]:
			at = append(at, row.ReceivedAt)
		}
	}

	return live, at, nil
}

// nextToFirst splits the live commits, in receipt order, into those right
// before or after a time in at, and the rest.
func nextToFirst(live []*ent.CommitRow, at []time.Time) [][]*ent.CommitRow {
	next := make(map[int]bool)

	for _, t := range at {
		i, _ := slices.BinarySearchFunc(live, t, func(c *ent.CommitRow, t time.Time) int { return c.ReceivedAt.Compare(t) })
		if i > 0 {
			next[i-1] = true
		}

		if i < len(live) {
			next[i] = true
		}
	}

	var near, rest []*ent.CommitRow

	for i, commit := range live {
		if next[i] {
			near = append(near, commit)
		} else {
			rest = append(rest, commit)
		}
	}

	return [][]*ent.CommitRow{near, rest}
}

// dirAt is a directory of a tombstoned commit, by path.
type dirAt struct {
	path string
	ref  *proto.Ref
}

func (d dirAt) key() string {
	return d.path + "\x00" + string(d.ref.GetHash())
}

// walkTombstoned visits what the trees of tombstoned commits reach that
// set_refs records, as walkRefs does, but for a root in shared, the trees
// of the set's live commits, and a directory a live commit holds at the
// same path, both of which it leaves out with all below them. An object the
// store lost is passed over.
func (x *Index) walkTombstoned(ctx context.Context, setID int64, roots []*proto.Ref, shared map[string]bool, visit func([]byte)) error {
	visited := make(map[string]bool)

	var level []dirAt

	enter := func(dirs []dirAt) {
		for _, dir := range dirs {
			if !visited[string(dir.ref.GetHash())] {
				visited[string(dir.ref.GetHash())] = true
				visit(dir.ref.GetHash())
				level = append(level, dir)
			}
		}
	}

	for _, root := range roots {
		if !shared[string(root.GetHash())] {
			enter([]dirAt{{ref: root}})
		}
	}

	for len(level) > 0 {
		current := level
		level = nil

		refs := make([]*proto.Ref, len(current))
		for i, dir := range current {
			refs[i] = dir.ref
		}

		objects, errs := x.getAll(ctx, refs)

		var splits, subdirs []dirAt
		var large []*proto.Ref

		listed := make(map[string]bool)

		for i, obj := range objects {
			if errors.Is(errs[i], backup.ErrNotFound) {
				continue
			}

			if errs[i] != nil {
				return errs[i]
			}

			t := obj.GetTree()
			if t == nil {
				return fmt.Errorf("object %x is not a tree", refs[i].GetHash())
			}

			for _, split := range t.GetSplits() {
				splits = append(splits, dirAt{path: current[i].path, ref: split})
			}

			large = append(large, nodeRefs(t, visit, func(name []byte, ref *proto.Ref) {
				dir := dirAt{path: proto.JoinPath(current[i].path, name), ref: ref}
				if key := dir.key(); !visited[string(ref.GetHash())] && !listed[key] {
					listed[key] = true
					subdirs = append(subdirs, dir)
				}
			})...)
		}

		failed := func(err error) bool { return err != nil && !errors.Is(err, backup.ErrNotFound) }
		if err := x.walkLarge(ctx, large, failed, visit); err != nil {
			return err
		}

		held, err := x.liveDirs(ctx, setID, subdirs)
		if err != nil {
			return err
		}

		enter(splits)

		for _, dir := range subdirs {
			if !held[dir.key()] {
				enter([]dirAt{dir})
			}
		}
	}

	return nil
}

// liveDirs returns the keys of the directories a live commit of the set
// holds at the same path.
func (x *Index) liveDirs(ctx context.Context, setID int64, dirs []dirAt) (map[string]bool, error) {
	paths := make([]string, 0, len(dirs))
	seen := make(map[string]bool, len(dirs))

	for _, dir := range dirs {
		if !seen[dir.path] {
			seen[dir.path] = true
			paths = append(paths, dir.path)
		}
	}

	held := make(map[string]bool)

	err := inBatches(paths, func(batch []string) error {
		rows, err := x.client.Tree.Query().
			Where(tree.SetID(setID), tree.PathIn(batch...), predicate.Tree(heldByCommit(untombstonedColumns))).
			Select(tree.FieldPath, tree.FieldRef).All(ctx)
		for _, row := range rows {
			held[dirAt{path: row.Path, ref: &proto.Ref{Hash: row.Ref}}.key()] = true
		}

		return err
	})

	return held, err
}

// dropRefs deletes the dead refs of a set under its lock, provided every
// live commit of the set is among those walked to find them.
func (x *Index) dropRefs(ctx context.Context, setID int64, dead, walked map[string]bool) error {
	return x.tx(ctx, func(tx *ent.Tx) error {
		if _, err := x.lockSet(ctx, tx, setID); err != nil {
			return err
		}

		live, err := tx.CommitRow.Query().Where(commitrow.SetID(setID), commitrow.TombstonedAtIsNil()).Select(commitrow.FieldRef).Strings(ctx)
		if err != nil {
			return err
		}

		for _, ref := range live {
			if !walked[ref] {
				return errSetMoved
			}
		}

		refs := make([][]byte, 0, len(dead))
		for ref := range dead {
			refs = append(refs, []byte(ref))
		}

		for start := 0; start < len(refs); start += objectBatch {
			chunk := refs[start:min(start+objectBatch, len(refs))]

			_, err := tx.SetRef.Delete().Where(setref.SetID(setID), setref.RefIn(chunk...)).Exec(ctx)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

// walkRefs visits the refs the trees reach that set_refs records: each
// tree and its splits, every file, and the splits of a large file. It reads
// a level of the trees at a time, and stops before one once stop, when
// given, says so. Unless lenient, an object it cannot find fails the walk;
// lenient, the walk goes on without it.
func (x *Index) walkRefs(ctx context.Context, trees []*proto.Ref, visited map[string]bool, lenient bool, visit func([]byte), stop func() bool) error {
	var level []*proto.Ref

	enter := func(tree *proto.Ref) {
		if !visited[string(tree.GetHash())] {
			visited[string(tree.GetHash())] = true
			visit(tree.GetHash())
			level = append(level, tree)
		}
	}

	failed := func(err error) bool {
		return err != nil && !(lenient && errors.Is(err, backup.ErrNotFound))
	}

	for _, tree := range trees {
		enter(tree)
	}

	for len(level) > 0 && (stop == nil || !stop()) {
		refs := level
		level = nil

		objects, errs := x.getAll(ctx, refs)

		var large []*proto.Ref

		for i, obj := range objects {
			if failed(errs[i]) {
				return errs[i]
			}

			if errs[i] != nil {
				continue
			}

			t := obj.GetTree()
			if t == nil {
				return fmt.Errorf("object %x is not a tree", refs[i].GetHash())
			}

			for _, split := range t.GetSplits() {
				enter(split)
			}

			large = append(large, nodeRefs(t, visit, func(_ []byte, ref *proto.Ref) { enter(ref) })...)
		}

		if err := x.walkLarge(ctx, large, failed, visit); err != nil {
			return err
		}
	}

	return nil
}

// nodeRefs visits the refs the nodes of a tree name that set_refs records,
// hands each directory to dir, and returns the large files and streams
// whose splits are to be visited too.
func nodeRefs(t *proto.Tree, visit func([]byte), dir func(name []byte, ref *proto.Ref)) []*proto.Ref {
	var large []*proto.Ref

	for _, node := range t.GetNodes() {
		for _, ref := range node.GetStat().MetadataRefs() {
			visit(ref.GetHash())
		}

		for _, stream := range slices.Concat(node.GetStat().GetStreams(), node.GetStat().GetXattrs()) {
			if stream.GetSize() >= backup.SplitFileSize {
				large = append(large, stream.GetRef())
			}
		}

		if node.GetRef() == nil {
			continue
		}

		switch node.GetStat().GetType() {
		case proto.NodeType_NODE_DIRECTORY:
			dir(node.GetStat().GetName(), node.GetRef())
		case proto.NodeType_NODE_FILE:
			visit(node.GetRef().GetHash())

			if node.GetStat().GetSize() >= backup.SplitFileSize {
				large = append(large, node.GetRef())
			}
		}
	}

	return large
}

// walkLarge visits the splits of the large files.
func (x *Index) walkLarge(ctx context.Context, files []*proto.Ref, failed func(error) bool, visit func([]byte)) error {
	objects, errs := x.getAll(ctx, files)

	var mtx sync.Mutex
	grp, gctx := errgroup.WithContext(ctx)
	grp.SetLimit(diffWorkers)

	for i, obj := range objects {
		if failed(errs[i]) {
			return errs[i]
		}

		if errs[i] != nil {
			continue
		}

		grp.Go(func() error {
			err := backup.SubFiles(gctx, x.ObjectStore, obj.GetFile(), func(split *proto.Ref) error {
				mtx.Lock()
				visit(split.GetHash())
				mtx.Unlock()

				return nil
			})
			if failed(err) {
				return err
			}

			return nil
		})
	}

	return grp.Wait()
}

// getAll reads the objects of refs together, with the error of each that
// could not be read.
func (x *Index) getAll(ctx context.Context, refs []*proto.Ref) ([]*proto.Object, []error) {
	return backup.ReadObjects(ctx, x, refs)
}
