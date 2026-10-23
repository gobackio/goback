package goback_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobackio/goback"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/job"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/pack"
	"github.com/stretchr/testify/require"
)

// seeing is an index that also sees what the store behind it holds, as a
// spool in front of a remote store does.
type seeing struct {
	backup.Index
	behind backup.Index
}

func (s seeing) Has(ctx context.Context, ref *proto.Ref) (bool, error) {
	if ok, err := s.Index.Has(ctx, ref); err != nil || ok {
		return ok, err
	}

	return s.behind.Has(ctx, ref)
}

func (s seeing) LatestCommit(ctx context.Context, set string) (*proto.Ref, error) {
	ref, err := s.Index.LatestCommit(ctx, set)
	if errors.Is(err, backup.ErrNotFound) {
		return s.behind.LatestCommit(ctx, set)
	}

	return ref, err
}

func (s seeing) HasAll(ctx context.Context, refs []*proto.Ref) ([]bool, error) {
	present := make([]bool, len(refs))

	for i, ref := range refs {
		var err error
		if present[i], err = s.Has(ctx, ref); err != nil {
			return nil, err
		}
	}

	return present, nil
}

func (s seeing) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	obj, err := s.Index.Get(ctx, ref)
	if errors.Is(err, backup.ErrNotFound) {
		return s.behind.Get(ctx, ref)
	}

	return obj, err
}

func TestAStoreInFrontOfAnotherCommitsWhatOnlyTheOtherHolds(t *testing.T) {
	ctx := context.Background()

	open := func(s goback.Settings) *goback.Repo {
		repo, err := goback.Open(ctx, s)
		require.NoError(t, err)
		t.Cleanup(func() { _ = repo.Close() })
		return repo
	}

	src := t.TempDir()
	content := make([]byte, 3<<20)
	for i := range content {
		content[i] = byte(i * 7)
	}
	require.NoError(t, os.MkdirAll(filepath.Join(src, "sub"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "sub", "big.bin"), content, 0o644))

	back := open(goback.Settings{Storage: t.TempDir(), Index: t.TempDir(), AgentID: "pc"})
	_, err := job.Path(ctx, &job.Env{Store: back.Store, Index: back.Index, AgentID: "pc"}, job.PathSettings{Set: "pc/files", Dir: src, ProgressInterval: -1})
	require.NoError(t, err)

	run := func(front *goback.Repo) (*backup.WalkResult, error) {
		index := seeing{Index: front.Index, behind: back.Index}
		// other metadata, so the run commits the tree it finds again
		meta := map[string]string{"through": "front"}
		return job.Path(ctx, &job.Env{Store: index, Index: index, AgentID: "pc"}, job.PathSettings{Set: "pc/files", Dir: src, ProgressInterval: -1, Meta: meta})
	}

	alone := open(goback.Settings{Storage: t.TempDir(), Index: t.TempDir(), AgentID: "pc"})
	_, err = run(alone)
	require.ErrorIs(t, err, backup.ErrDanglingRef, "a store alone refuses what it doesn't hold")

	var warned atomic.Int32
	defer slog.SetDefault(slog.Default())
	slog.SetDefault(slog.New(warnCounter{slog.DiscardHandler, &warned}))

	front := open(goback.Settings{Storage: t.TempDir(), Index: t.TempDir(), AgentID: "pc", Behind: back.Index})
	result, err := run(front)
	require.NoError(t, err)
	require.Zero(t, result.Read, "every file came from the other store's commit")

	held, err := front.Store.Has(ctx, result.Commit.GetTree())
	require.NoError(t, err)
	require.False(t, held, "the front store holds none of it")

	versions, err := front.Index.FileInfo(ctx, "pc/files", "sub/big.bin", time.Now().Add(time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, versions, 1, "the front index indexed the tree only the other holds")
	require.EqualValues(t, len(content), versions[0].GetStat().GetSize())

	collector := front.Store.(interface {
		Collect(context.Context, pack.CollectOptions) (*pack.CollectReport, error)
	})
	for range 2 {
		_, err := collector.Collect(ctx, pack.CollectOptions{MinAge: time.Nanosecond, TempDir: t.TempDir(), Behind: true})
		require.NoError(t, err, "a collection passes over what only the other store holds")
	}

	require.Zero(t, warned.Load(), "and doesn't warn about it")
}

// warnCounter counts the records at warning level and above.
type warnCounter struct {
	slog.Handler
	warned *atomic.Int32
}

func (w warnCounter) Enabled(context.Context, slog.Level) bool { return true }

func (w warnCounter) Handle(_ context.Context, r slog.Record) error {
	if r.Level >= slog.LevelWarn {
		w.warned.Add(1)
	}

	return nil
}

func TestAStoreInFrontOfAnotherLeavesReferencesToIt(t *testing.T) {
	ctx := context.Background()

	open := func(s goback.Settings) *goback.Repo {
		repo, err := goback.Open(ctx, s)
		require.NoError(t, err)
		t.Cleanup(func() { _ = repo.Close() })
		return repo
	}

	missing := &proto.Ref{Hash: make([]byte, 32)}
	missing.Hash[0] = 1
	nodes := []*proto.TreeNode{{Stat: &proto.FileInfo{Name: []byte("gone.txt"), Type: proto.NodeType_NODE_FILE}, Ref: missing}}

	alone := open(goback.Settings{Storage: t.TempDir(), Index: t.TempDir(), AgentID: "pc"})
	_, err := backup.PutTree(ctx, alone.Index, nodes, nil, nil)
	require.ErrorIs(t, err, backup.ErrDanglingRef)

	back := open(goback.Settings{Storage: t.TempDir(), Index: t.TempDir(), AgentID: "pc"})
	front := open(goback.Settings{Storage: t.TempDir(), Index: t.TempDir(), AgentID: "pc", Behind: back.Index})
	_, err = backup.PutTree(ctx, front.Index, nodes, nil, nil)
	require.NoError(t, err, "the store behind checks references when what the front holds gets there")
}

func TestAStoreInFrontOfAnotherReadsBackWhatARunKeptOfIt(t *testing.T) {
	ctx := context.Background()

	open := func(s goback.Settings) *goback.Repo {
		repo, err := goback.Open(ctx, s)
		require.NoError(t, err)
		t.Cleanup(func() { _ = repo.Close() })
		return repo
	}

	back := open(goback.Settings{Storage: t.TempDir(), Index: t.TempDir(), AgentID: "pc"})
	front := open(goback.Settings{Storage: t.TempDir(), Index: t.TempDir(), AgentID: "pc", Behind: back.Index})

	sctx, err := front.Store.(backup.SessionStore).BeginSession(ctx, &backup.Session{Set: "pc/files"})
	require.NoError(t, err)

	tree := proto.NewObject(&proto.Tree{})
	require.NoError(t, front.Index.Put(sctx, tree), "a tree the run took from the store behind")

	got, err := front.Index.Get(sctx, tree.Ref())
	require.NoError(t, err, "the run reads it again while its archive is still being written")
	require.True(t, got.Ref().Equal(tree.Ref()))
}
