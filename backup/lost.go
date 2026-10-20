package backup

import (
	"context"
	"fmt"

	"github.com/gobackio/goback/proto"
	pb "google.golang.org/protobuf/proto"
)

// MarkLost returns commit rewritten to reference none of lost: an entry
// whose content, listing or metadata reaches a lost ref is marked lost,
// a lost tree split is left out, and every tree above such a change and
// the commit are marked incomplete. The rewritten trees are put in store,
// and the returned commit is still to be put.
//
// reaches reports whether the object at a ref reaches a lost ref, so only
// those are read; nil reads every tree and file the commit reaches.
func MarkLost(ctx context.Context, store ObjectStore, commit *proto.Object, lost []*proto.Ref, reaches func(*proto.Ref) bool) (*proto.Object, error) {
	m := &marker{
		ctx: ctx, store: store, reaches: reaches,
		lost:  make(map[string]bool, len(lost)),
		trees: map[string]rewritten{}, files: map[string]bool{},
	}

	for _, ref := range lost {
		m.lost[string(ref.Hash)] = true
	}

	out := pb.Clone(commit.GetCommit()).(*proto.Commit)
	out.Incomplete = false

	root := out.GetTree()

	switch {
	case root == nil:
	case m.lost[string(root.Hash)]:
		empty := proto.NewObject(&proto.Tree{Incomplete: true})
		if err := store.Put(ctx, empty); err != nil {
			return nil, err
		}

		out.Tree, out.Incomplete = empty.Ref(), true
	default:
		r, err := m.tree(root)
		if err != nil {
			return nil, err
		}

		out.Tree, out.Incomplete = r.ref, r.incomplete
	}

	return proto.NewObject(out), nil
}

type rewritten struct {
	ref        *proto.Ref
	incomplete bool
}

type marker struct {
	ctx     context.Context
	store   ObjectStore
	reaches func(*proto.Ref) bool
	lost    map[string]bool
	trees   map[string]rewritten
	// files are whether the file object at a ref reaches a lost ref
	files map[string]bool
}

func (m *marker) skips(ref *proto.Ref) bool {
	return m.reaches != nil && !m.reaches(ref)
}

func (m *marker) tree(ref *proto.Ref) (rewritten, error) {
	if r, ok := m.trees[string(ref.Hash)]; ok {
		return r, nil
	}

	r := rewritten{ref: ref}

	if m.skips(ref) {
		return r, nil
	}

	obj, err := m.store.Get(m.ctx, ref)
	if err != nil {
		return r, fmt.Errorf("reading tree %x: %w", ref.Hash, err)
	}

	tree := pb.Clone(obj.GetTree()).(*proto.Tree)
	changed := false

	for i, node := range tree.Nodes {
		n, incomplete, err := m.node(node)
		if err != nil {
			return r, err
		}

		if n != node {
			tree.Nodes[i], changed = n, true
		}

		r.incomplete = r.incomplete || incomplete
	}

	splits := tree.Splits[:0]

	for _, split := range tree.Splits {
		if m.lost[string(split.Hash)] {
			r.incomplete, changed = true, true
			continue
		}

		s, err := m.tree(split)
		if err != nil {
			return r, err
		}

		if !s.ref.Equal(split) {
			changed = true
		}

		splits = append(splits, s.ref)
		r.incomplete = r.incomplete || s.incomplete
	}

	tree.Splits = splits

	if r.incomplete && !tree.Incomplete {
		tree.Incomplete, changed = true, true
	}

	r.incomplete = r.incomplete || tree.Incomplete

	if changed {
		out := proto.NewObject(tree)
		if err := m.store.Put(m.ctx, out); err != nil {
			return r, err
		}

		r.ref = out.Ref()
	}

	m.trees[string(ref.Hash)] = r

	return r, nil
}

// node returns node, or a copy rewritten past what it lost, and whether
// anything under it is lost.
func (m *marker) node(node *proto.TreeNode) (*proto.TreeNode, bool, error) {
	stat := node.GetStat()
	if stat.GetLost() {
		return node, true, nil
	}

	gone := node.Ref != nil && m.lost[string(node.Ref.Hash)]

	for _, ref := range stat.MetadataRefs() {
		if gone {
			break
		}

		var err error
		if gone, err = m.file(ref); err != nil {
			return nil, false, err
		}
	}

	if !gone && node.Ref != nil {
		if stat.IsDir() {
			r, err := m.tree(node.Ref)
			if err != nil {
				return nil, false, err
			}

			if r.ref.Equal(node.Ref) {
				return node, r.incomplete, nil
			}

			return &proto.TreeNode{Stat: stat, Ref: r.ref}, r.incomplete, nil
		}

		var err error
		if gone, err = m.file(node.Ref); err != nil {
			return nil, false, err
		}
	}

	if !gone {
		return node, false, nil
	}

	lost := pb.Clone(stat).(*proto.FileInfo)
	lost.Lost, lost.Security, lost.Streams = true, nil, nil

	return &proto.TreeNode{Stat: lost}, true, nil
}

// file reports whether the file object at ref reaches a lost ref.
func (m *marker) file(ref *proto.Ref) (bool, error) {
	if m.lost[string(ref.Hash)] {
		return true, nil
	}

	if gone, ok := m.files[string(ref.Hash)]; ok {
		return gone, nil
	}

	if m.skips(ref) {
		return false, nil
	}

	obj, err := m.store.Get(m.ctx, ref)
	if err != nil {
		return false, fmt.Errorf("reading file %x: %w", ref.Hash, err)
	}

	gone := false

	for _, part := range obj.GetFile().GetParts() {
		if part.Ref != nil && m.lost[string(part.Ref.Hash)] {
			gone = true
			break
		}
	}

	for _, split := range obj.GetFile().GetSplits() {
		if gone {
			break
		}

		if gone, err = m.file(split); err != nil {
			return false, err
		}
	}

	m.files[string(ref.Hash)] = gone

	return gone, nil
}
