package goback

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage"
	"github.com/gobackio/goback/storage/cache"
	"github.com/gobackio/goback/storage/pack"

	"github.com/stretchr/testify/require"
)

// sentTrees is a store server's client that records the trees sent through
// it.
type sentTrees struct {
	*storage.Client

	mtx  sync.Mutex
	sent map[string]bool
}

func (s *sentTrees) record(objects ...*proto.Object) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for _, obj := range objects {
		if obj.Type() == proto.ObjectType_TREE {
			s.sent[string(obj.Ref().Hash)] = true
		}
	}
}

// take returns the trees sent since the last call.
func (s *sentTrees) take() map[string]bool {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	sent := s.sent
	s.sent = map[string]bool{}

	return sent
}

func (s *sentTrees) Put(ctx context.Context, obj *proto.Object) error {
	s.record(obj)
	return s.Client.Put(ctx, obj)
}

func (s *sentTrees) PutObjects(ctx context.Context, objects []*proto.Object) error {
	s.record(objects...)
	return s.Client.PutObjects(ctx, objects)
}

func (s *sentTrees) PutAssuming(ctx context.Context, obj *proto.Object, assumed []*proto.Ref) ([]*proto.Ref, error) {
	s.record(obj)
	return s.Client.PutAssuming(ctx, obj, assumed)
}

// splitStore is a served store and an agent of it whose objects go through
// a cache in memory, as a receiver's do.
type splitStore struct {
	t       *testing.T
	ctx     context.Context
	repo    *Repo
	client  *sentTrees
	objects backup.ObjectStore
}

func servedSplits(t *testing.T) *splitStore {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	repo, err := Open(ctx, Settings{Storage: t.TempDir(), Index: t.TempDir()})
	require.NoError(t, err)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	served := make(chan error, 1)
	go func() {
		served <- repo.Serve(ctx, listener, ServeOptions{Secret: "s3cret", PlaintextBehindProxy: true})
	}()

	agent, err := Open(ctx, Settings{Storage: "goback+insecure://s3cret@" + listener.Addr().String(), AgentID: "agent"})
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = agent.Close()
		cancel()
		<-served
		_ = repo.Close()
	})

	client := &sentTrees{Client: agent.Index.(*storage.Client), sent: map[string]bool{}}

	return &splitStore{t: t, ctx: context.Background(), repo: repo, client: client, objects: cache.New(cache.NewMemory(), client)}
}

// links is a directory of n symlinks, the one at changed pointing elsewhere.
func links(n, changed int) []*proto.TreeNode {
	nodes := make([]*proto.TreeNode, n)

	for i := range nodes {
		target := fmt.Sprintf("target-%d", i)
		if i == changed {
			target += "-changed"
		}

		nodes[i] = &proto.TreeNode{Stat: &proto.FileInfo{Name: []byte(fmt.Sprintf("link-%04d", i)), Type: proto.NodeType_NODE_SYMLINK, LinkTarget: []byte(target)}}
	}

	return nodes
}

func (s *splitStore) begin(set string) context.Context {
	s.t.Helper()

	sctx, err := s.client.BeginSession(s.ctx, &backup.Session{Set: set})
	require.NoError(s.t, err)

	return sctx
}

func commitOf(set string, at int64, tree *proto.Ref) *proto.Object {
	return proto.NewObject(&proto.Commit{Timestamp: at, Tree: tree, BackupSet: set, AgentId: "agent"})
}

// commit stores nodes as the tree of a commit to set in a session of its own.
func (s *splitStore) commit(set string, at int64, nodes []*proto.TreeNode) *proto.Ref {
	s.t.Helper()

	sctx := s.begin(set)

	tree, err := backup.PutTree(sctx, s.objects, nodes, nil, nil)
	require.NoError(s.t, err)
	require.NoError(s.t, s.client.Put(sctx, commitOf(set, at, tree)))

	return tree
}

func (s *splitStore) splits(tree *proto.Ref) []*proto.Ref {
	s.t.Helper()

	obj, err := s.repo.Store.Get(s.ctx, tree)
	require.NoError(s.t, err)
	require.Greater(s.t, len(obj.GetTree().GetSplits()), 2)

	return obj.GetTree().GetSplits()
}

// keepLast has set keep its newest commit only and retires the rest.
func (s *splitStore) keepLast(set string) {
	s.t.Helper()

	index := s.repo.Index.(interface {
		SetPolicy(ctx context.Context, set string, p *retention.Policy) error
		backup.Retirer
	})

	require.NoError(s.t, index.SetPolicy(s.ctx, set, &retention.Policy{KeepLast: 1}))

	n, err := index.Retire(s.ctx, time.Now())
	require.NoError(s.t, err)
	require.Positive(s.t, n)
}

// collect runs a collection as of each time ahead: the first condemns what
// no live commit reaches, a later one two days on sweeps it.
func (s *splitStore) collect(ahead ...time.Duration) {
	s.t.Helper()

	collector := s.repo.Store.(interface {
		Collect(context.Context, pack.CollectOptions) (*pack.CollectReport, error)
	})

	for _, ahead := range ahead {
		_, err := collector.Collect(s.ctx, pack.CollectOptions{Now: time.Now().Add(ahead), TempDir: s.t.TempDir(), DeadRatio: 1e-9})
		require.NoError(s.t, err)
	}
}

func (s *splitStore) holds(ref *proto.Ref) bool {
	s.t.Helper()

	has, err := s.repo.Store.Has(s.ctx, ref)
	require.NoError(s.t, err)

	return has
}

// requireWhole requires the store to hold the tree, every split of it and
// its n nodes.
func (s *splitStore) requireWhole(tree *proto.Ref, n int) {
	s.t.Helper()

	for _, split := range s.splits(tree) {
		require.True(s.t, s.holds(split), "split %x", split.Hash)
	}

	flat, err := backup.LoadTree(s.ctx, s.repo.Store, tree)
	require.NoError(s.t, err)
	require.Len(s.t, flat.Nodes, n)
}

func TestACommitKeepsTheSplitsItDidNotSendThroughCollections(t *testing.T) {
	s := servedSplits(t)

	first := s.commit("world", 1, links(1000, -1))
	before := s.splits(first)
	require.Len(t, s.client.take(), len(before)+1)

	second := s.commit("world", 2, links(1000, 999))
	after := s.splits(second)

	sent := s.client.take()
	require.Len(t, sent, 2, "the changed split and its tree")
	require.True(t, sent[string(after[len(after)-1].Hash)])
	require.Equal(t, before[:len(before)-1], after[:len(after)-1])

	s.keepLast("world")
	s.collect(0, 48*time.Hour)

	s.requireWhole(second, 1000)
	require.False(t, s.holds(before[len(before)-1]), "what only the first commit reached is collected")

	again := s.commit("world", 3, links(1000, -1))
	require.True(t, again.Equal(first))
	require.True(t, s.client.take()[string(before[len(before)-1].Hash)], "the store lacked the split the cache kept, so it went up again")

	s.requireWhole(again, 1000)
}

func TestASplitACollectionTakesBeforeItsCommitLeavesNoTreeDangling(t *testing.T) {
	for _, swept := range []bool{false, true} {
		t.Run(map[bool]string{false: "condemned", true: "swept"}[swept], func(t *testing.T) {
			s := servedSplits(t)

			s.commit("other", 1, links(1000, -1))
			s.commit("other", 2, links(1000, 999))
			s.client.take()

			sctx := s.begin("world")

			tree, err := backup.PutTree(sctx, s.objects, links(1000, -1), nil, nil)
			require.NoError(t, err)
			require.Len(t, s.client.take(), 1, "the store confirmed every split")

			splits := s.splits(tree)
			gone := splits[len(splits)-1]

			s.keepLast("other")

			if !swept {
				s.collect(0)
				require.NoError(t, s.client.Put(sctx, commitOf("world", 3, tree)), "the commit copies what the collection condemned under it")
			} else {
				s.collect(0, 48*time.Hour)

				var missing *backup.MissingError
				require.ErrorAs(t, s.client.Put(sctx, commitOf("world", 3, tree)), &missing, "the commit is refused, naming what the collection took")
				require.Len(t, missing.Refs, 1)
				require.True(t, missing.Refs[0].Equal(gone))

				again := s.commit("world", 3, links(1000, -1))
				require.True(t, again.Equal(tree))
				require.True(t, s.client.take()[string(gone.Hash)], "a session of its own sends it again")
			}

			s.collect(96*time.Hour, 144*time.Hour)
			s.requireWhole(tree, 1000)
		})
	}
}
