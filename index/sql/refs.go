package sql

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/index/sql/ent"
	"github.com/gobackio/goback/index/sql/ent/commitrow"
	"github.com/gobackio/goback/index/sql/ent/setref"
	"github.com/gobackio/goback/proto"

	"golang.org/x/sync/errgroup"
)

// pruneRefs drops the set's set_refs rows that only the tombstoned
// commits reach, so their trees and files stop being readable before the
// collector erases them. A ref a live commit shares stays. The live
// commits are walked without the set lock, and the ones indexed meanwhile
// under it, so a concurrent commit cannot lose a ref.
func (x *Index) pruneRefs(ctx context.Context, setID int64, tombstoned [][]byte) error {
	dead := map[string]bool{}
	refs := make([]*proto.Ref, len(tombstoned))

	for i, ref := range tombstoned {
		dead[string(ref)] = true
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

	err := x.walkRefs(ctx, trees, map[string]bool{}, true, func(hash []byte) { dead[string(hash)] = true })
	if err != nil {
		return err
	}

	visited := map[string]bool{}
	walked := map[string]bool{}

	subtractLive := func(c *ent.Client) error {
		commits, err := c.CommitRow.Query().Where(commitrow.SetID(setID), commitrow.TombstonedAtIsNil()).All(ctx)
		if err != nil {
			return err
		}

		var trees []*proto.Ref

		for _, commit := range commits {
			if walked[string(commit.Ref)] {
				continue
			}

			walked[string(commit.Ref)] = true
			delete(dead, string(commit.Ref))
			trees = append(trees, &proto.Ref{Hash: commit.Tree})
		}

		// what a live commit reaches only through an object the store lost
		// cannot be read through it either
		err = x.walkRefs(ctx, trees, visited, true, func(hash []byte) { delete(dead, string(hash)) })
		if err != nil {
			return fmt.Errorf("walking live commits: %w", err)
		}

		return nil
	}

	for range planAttempts {
		if err := subtractLive(x.client); err != nil {
			return err
		}

		err := x.dropRefs(ctx, setID, dead, walked)
		if !errors.Is(err, errSetMoved) {
			return err
		}
	}

	// the set keeps taking commits: the next retirement prunes what is left
	return nil
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
// a level of the trees at a time. Unless lenient, an object it cannot find
// fails the walk; lenient, the walk goes on without it.
func (x *Index) walkRefs(ctx context.Context, trees []*proto.Ref, visited map[string]bool, lenient bool, visit func([]byte)) error {
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

	for len(level) > 0 {
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
					enter(node.GetRef())
				case proto.NodeType_NODE_FILE:
					visit(node.GetRef().GetHash())

					if node.GetStat().GetSize() >= backup.SplitFileSize {
						large = append(large, node.GetRef())
					}
				}
			}
		}

		if err := x.walkLarge(ctx, large, failed, visit); err != nil {
			return err
		}
	}

	return nil
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
