package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"
)

const (
	// treeFanout is the node count above which a directory is split into
	// sub-tree objects.
	treeFanout = 256
	// treeSplitMin and treeSplitMax bound one split's node count; the mean is
	// treeSplitAvg, chosen by a hash of the entry name so a change rewrites
	// O(1) splits.
	treeSplitMin = 64
	treeSplitAvg = 256
	treeSplitMax = 1024
)

// TreeWriter receives the entries of one directory being backed up.
type TreeWriter interface {
	// File records a regular file whose content the callback writes;
	// ErrSkipFile from it leaves the file out.
	File(context.Context, os.FileInfo, func(io.Writer) error) error
	// Tree records a subdirectory the callback fills; ErrSkipFile from it
	// leaves the directory out.
	Tree(context.Context, os.FileInfo, func(TreeWriter) error) error
	// Node adds a finished node.
	Node(*proto.TreeNode)
}

type backupTree struct {
	store    ObjectStore
	nodes    []*proto.TreeNode
	nodesMtx sync.Mutex
}

var _ TreeWriter = (*backupTree)(nil)

func (bt *backupTree) Tree(ctx context.Context, info os.FileInfo, writer func(TreeWriter) error) error {
	node := &backupTree{
		nodes: make([]*proto.TreeNode, 0),
		store: bt.store,
	}

	err := writer(node)
	if err != nil {
		if errors.Is(err, ErrSkipFile) {
			return nil
		}
		return err
	}

	ref, err := PutTree(ctx, bt.store, node.sortedNodes(), nil, nil)
	if err != nil {
		return err
	}

	bt.Node(&proto.TreeNode{
		Stat: proto.GetFileInfo(info),
		Ref:  ref,
	})

	return nil
}

func (bt *backupTree) File(ctx context.Context, info os.FileInfo, writer func(io.Writer) error) error {
	fWriter := newFileWriter(ctx, bt.store, nil, info.Size())
	node := &proto.TreeNode{
		Stat: proto.GetFileInfo(info),
	}

	err := writer(fWriter)
	if err != nil {
		if errors.Is(err, ErrSkipFile) {
			return nil
		}

		return err
	}

	bt.Node(node)

	err = fWriter.Close()
	if err != nil {
		return err
	}

	node.Ref = fWriter.Ref()

	return nil
}

func (bt *backupTree) Node(node *proto.TreeNode) {
	bt.nodesMtx.Lock()
	bt.nodes = append(bt.nodes, node)
	bt.nodesMtx.Unlock()
}

// sortedNodes orders nodes by name, since they are appended from parallel
// workers and the tree ref must not depend on arrival order.
func (bt *backupTree) sortedNodes() []*proto.TreeNode {
	bt.nodesMtx.Lock()
	defer bt.nodesMtx.Unlock()

	return SortNodes(bt.nodes)
}

// SortNodes orders tree nodes bytewise by name, as the canonical encoding
// requires.
func SortNodes(nodes []*proto.TreeNode) []*proto.TreeNode {
	sort.Slice(nodes, func(i int, j int) bool {
		return bytes.Compare(nodes[i].Stat.Name, nodes[j].Stat.Name) < 0
	})

	return nodes
}

func newTree(store ObjectStore) *backupTree {
	return &backupTree{
		store: store,
		nodes: make([]*proto.TreeNode, 0),
	}
}

// PutTree stores a directory's nodes as one tree object, or as split trees
// under a parent when the directory is large, and returns the ref of the
// object a parent node should reference. With a key the nodes' names are
// sealed for the directory whose token is parent; the given nodes are not
// modified.
func PutTree(ctx context.Context, store ObjectStore, nodes []*proto.TreeNode, key *storekey.Key, parent []byte) (*proto.Ref, error) {
	nodes = sealNodes(key, parent, nodes)

	if len(nodes) <= treeFanout {
		return putTreeObject(ctx, store, &proto.Tree{Nodes: nodes}, key)
	}

	var splits []*proto.Ref
	for _, chunk := range splitNodes(nodes) {
		ref, err := putTreeObject(ctx, store, &proto.Tree{Nodes: chunk}, key)
		if err != nil {
			return nil, err
		}

		splits = append(splits, ref)
	}

	return putTreeObject(ctx, store, &proto.Tree{Splits: splits}, key)
}

func putTreeObject(ctx context.Context, store ObjectStore, tree *proto.Tree, key *storekey.Key) (*proto.Ref, error) {
	obj := proto.NewObject(tree)
	if key != nil {
		obj.KeyId = key.ID()
	}

	err := store.Put(ctx, obj)
	if err != nil {
		return nil, err
	}

	return obj.Ref(), nil
}

// splitNodes cuts a sorted node list at name-defined boundaries.
func splitNodes(nodes []*proto.TreeNode) [][]*proto.TreeNode {
	var (
		chunks [][]*proto.TreeNode
		start  int
	)

	for i, node := range nodes {
		size := i - start + 1
		if size < treeSplitMin {
			continue
		}

		if size >= treeSplitMax || nameBoundary(node.Stat.Name) {
			chunks = append(chunks, nodes[start:i+1])
			start = i + 1
		}
	}

	if start < len(nodes) {
		chunks = append(chunks, nodes[start:])
	}

	return chunks
}

func nameBoundary(name []byte) bool {
	h := fnv.New64a()
	h.Write(name)

	return h.Sum64()%treeSplitAvg == 0
}

// Getter is the read side of an ObjectStore.
type Getter interface {
	// Get returns the object at the ref, or ErrNotFound.
	Get(context.Context, *proto.Ref) (*proto.Object, error)
}

// CommitKey returns the key to open commit's tree with: key for a commit
// sealed under a store key, nil for one written plain. A commit that does
// not record its key is tried with key and opened plain when that fails.
func CommitKey(ctx context.Context, store Getter, commit *proto.Commit, key *storekey.Key) (*storekey.Key, error) {
	switch {
	case key == nil, commit.KeyId != nil && len(commit.KeyId) == 0:
		return nil, nil
	case bytes.Equal(commit.KeyId, key.ID()):
		return key, nil
	}

	_, err := OpenTree(ctx, store, commit.Tree, key, nil)

	switch {
	case errors.Is(err, storekey.ErrWrongKey) && commit.KeyId != nil:
		return nil, fmt.Errorf("%w: the commit is sealed under key %x, not %x", err, commit.KeyId, key.ID())
	case errors.Is(err, storekey.ErrWrongKey):
		return nil, nil
	case err != nil:
		return nil, err
	}

	return key, nil
}

// OpenTree loads a tree and opens its names for the directory whose token
// is parent. Without a key it is LoadTree.
func OpenTree(ctx context.Context, store Getter, ref *proto.Ref, key *storekey.Key, parent []byte) (*proto.Tree, error) {
	tree, err := LoadTree(ctx, store, ref)
	if err != nil {
		return nil, err
	}

	if key == nil {
		return tree, nil
	}

	nodes, err := openNodes(key, parent, tree.Nodes)
	if err != nil {
		return nil, err
	}

	return &proto.Tree{Nodes: nodes}, nil
}

// LoadTree fetches a tree and flattens its splits into one node list.
func LoadTree(ctx context.Context, store Getter, ref *proto.Ref) (*proto.Tree, error) {
	obj, err := store.Get(ctx, ref)
	if err != nil {
		return nil, err
	}

	if obj.Type() != proto.ObjectType_TREE {
		return nil, errors.New("object is not a tree")
	}

	return flatten(ctx, store, obj.GetTree())
}

// flatten returns tree with its splits, read together, in place of it.
func flatten(ctx context.Context, store Getter, tree *proto.Tree) (*proto.Tree, error) {
	if len(tree.Splits) == 0 {
		return tree, nil
	}

	objects, err := readObjects(ctx, store, tree.Splits)
	if err != nil {
		return nil, err
	}

	flat := &proto.Tree{}
	for _, obj := range objects {
		if obj.Type() != proto.ObjectType_TREE {
			return nil, errors.New("object is not a tree")
		}

		sub, err := flatten(ctx, store, obj.GetTree())
		if err != nil {
			return nil, err
		}

		flat.Nodes = append(flat.Nodes, sub.Nodes...)
	}

	return flat, nil
}
