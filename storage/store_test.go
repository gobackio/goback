package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobackio/goback/auth"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// TestStoreIsALibrary drives two stores in one process without gRPC: each
// call picks a store and runs the operation on it.
func TestStoreIsALibrary(t *testing.T) {
	stores := map[string]*Store{
		"node-1": NewStore(newMemIndex(), nil),
		"node-2": NewStore(newMemIndex(), nil),
	}
	storeOf := func(ctx context.Context) *Store {
		p, err := auth.Require(ctx)
		require.NoError(t, err)

		return stores[p.AgentID]
	}

	first := auth.WithPrincipal(context.Background(), &auth.Principal{AgentID: "node-1"})
	second := auth.WithPrincipal(context.Background(), &auth.Principal{AgentID: "node-2"})

	blob := proto.NewObject(&proto.Blob{Data: []byte("save data")})
	file := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Length: 9, Ref: blob.Ref()}}})
	tree := proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{{Stat: &proto.FileInfo{Name: []byte("a")}, Ref: file.Ref()}}})

	for _, obj := range []*proto.Object{blob, file, tree} {
		receipt, err := storeOf(first).Put(first, Upload{Object: obj, Ref: obj.Ref()})
		require.NoError(t, err)
		require.True(t, receipt.Ref.Equal(obj.Ref()))
		require.Nil(t, receipt.Object, "only commits and pins come back stamped")
	}

	_, err := storeOf(first).Put(first, Upload{Object: blob, Ref: file.Ref()})
	require.ErrorIs(t, err, proto.ErrRefMismatch)

	got, err := storeOf(first).Get(first, tree.Ref())
	require.NoError(t, err)
	require.True(t, got.Ref().Equal(tree.Ref()))

	_, err = storeOf(first).Get(first, blob.Ref())
	require.ErrorIs(t, err, backup.ErrNotFound, "blobs are never served")

	_, err = storeOf(second).Get(second, tree.Ref())
	require.ErrorIs(t, err, backup.ErrNotFound, "the second store never saw the upload")

	var trees []*proto.Ref
	require.NoError(t, storeOf(first).Tree(first, tree.Ref(), 1, func(resp *proto.GetTreeResponse) error {
		trees = append(trees, resp.Ref)
		return nil
	}))
	require.Len(t, trees, 1)

	err = storeOf(first).Tree(first, file.Ref(), 1, func(*proto.GetTreeResponse) error { return nil })
	require.ErrorIs(t, err, ErrInvalidRequest, "a file is not a tree")

	var parts []int
	require.NoError(t, storeOf(first).ReadFile(first, file.Ref(), nil, func(resp *proto.ReadFileResponse) error {
		parts = append(parts, int(resp.Index))
		require.True(t, resp.Object.Ref().Equal(blob.Ref()))
		return nil
	}))
	require.Equal(t, []int{0}, parts)

	grant, err := storeOf(first).BeginCommit(first, "world")
	require.NoError(t, err)
	require.Nil(t, grant.Policy, "an index without a gate grants without a policy")

	_, err = storeOf(first).Retention()
	require.ErrorIs(t, err, backup.ErrNotImplemented)

	_, err = storeOf(first).Presence(first, "world")
	require.NoError(t, err, "an index without filters serves none")

	// a store with sessions refuses a write outside one
	packs := testPackStore(t)
	withSessions := NewStore(&packIndex{PackStorage: packs, latest: map[string]*proto.Ref{}}, packs)
	_, err = withSessions.Put(first, Upload{Object: blob, Ref: blob.Ref()})
	require.ErrorIs(t, err, backup.ErrNoSession)

	session, err := withSessions.BeginSession(first, "world", nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, session.ID)

	_, err = withSessions.Session(second, session.ID)
	require.ErrorIs(t, err, auth.ErrForbidden, "a session belongs to its caller")

	_, err = withSessions.Session(first, "nope")
	require.ErrorIs(t, err, backup.ErrNotFound)

	_, err = withSessions.Put(backup.WithSession(first, session), Upload{Object: blob, Ref: blob.Ref()})
	require.NoError(t, err)
	require.NoError(t, withSessions.EndSession(first, session.ID))
}

// slowTrees reads its trees with jitter, failing the one named by fail.
type slowTrees struct {
	*memIndex
	fail     []byte
	inFlight atomic.Int32
	peak     atomic.Int32
}

var errTreeRead = errors.New("tree read failed")

func (s *slowTrees) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	n := s.inFlight.Add(1)
	defer s.inFlight.Add(-1)

	for peak := s.peak.Load(); n > peak && !s.peak.CompareAndSwap(peak, n); peak = s.peak.Load() {
	}

	time.Sleep(time.Duration(rand.Intn(500)) * time.Microsecond)

	if bytes.Equal(ref.Hash, s.fail) {
		return nil, errTreeRead
	}

	return s.memIndex.Get(ctx, ref)
}

// TestTreeWalksInOrderWhileReadingAhead walks 40 directories of 30
// directories, each holding one more below maxDepth, more than a read
// batch, and expects every tree under maxDepth once, at the position of a
// serial breadth-first walk.
func TestTreeWalksInOrderWhileReadingAhead(t *testing.T) {
	ctx := auth.WithPrincipal(context.Background(), &auth.Principal{AgentID: "node-1"})
	index := &slowTrees{memIndex: newMemIndex()}

	put := func(children ...*proto.Ref) *proto.Ref {
		var nodes []*proto.TreeNode
		for i, child := range children {
			nodes = append(nodes, &proto.TreeNode{Stat: &proto.FileInfo{Name: []byte(fmt.Sprintf("dir%03d", i)), Type: proto.NodeType_NODE_DIRECTORY}, Ref: child})
		}

		nodes = append(nodes, &proto.TreeNode{Stat: &proto.FileInfo{Name: []byte("link"), Type: proto.NodeType_NODE_SYMLINK, LinkTarget: []byte(fmt.Sprint(rand.Int63()))}})

		tree := proto.NewObject(&proto.Tree{Nodes: nodes})
		require.NoError(t, index.Put(ctx, tree))

		return tree.Ref()
	}

	var tops, mids []*proto.Ref
	for range 40 {
		var below []*proto.Ref
		for range 30 {
			below = append(below, put(put()))
		}

		tops = append(tops, put(below...))
		mids = append(mids, below...)
	}

	root := put(tops...)
	want := append(append([]*proto.Ref{root}, tops...), mids...)

	for range 3 {
		var got []*proto.Ref
		require.NoError(t, NewStore(index, nil).Tree(ctx, root, 2, func(resp *proto.GetTreeResponse) error {
			require.Empty(t, resp.Runs, "trees are served")
			require.True(t, resp.Object.Ref().Equal(resp.Ref))
			got = append(got, resp.Ref)

			return nil
		}))

		require.Len(t, got, len(want))
		for i := range want {
			require.True(t, want[i].Equal(got[i]), "tree %d out of place", i)
		}
	}
}

func TestAStoreRefusesASealedObjectThatIsNotABlob(t *testing.T) {
	ctx := auth.WithPrincipal(context.Background(), &auth.Principal{AgentID: "node-1"})
	store := NewStore(newMemIndex(), nil)

	for _, kind := range []proto.ObjectType{proto.ObjectType_COMMIT, proto.ObjectType_TREE, proto.ObjectType_FILE, proto.ObjectType_PIN} {
		sealed := proto.NewObject(&proto.Sealed{Ref: proto.HashPayload(kind, []byte("x")), Type: kind, Data: []byte("junk"), Encryption: proto.Encryption_SEALED})

		_, err := store.Put(ctx, Upload{Object: sealed})
		require.ErrorIs(t, err, ErrInvalidRequest, kind.String())
		require.ErrorIs(t, store.PutObjects(ctx, []*proto.Object{sealed}), ErrInvalidRequest, kind.String())
	}
}
