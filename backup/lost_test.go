package backup

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// markLost rewrites the fixture's commit past the given refs and makes it
// the set's latest.
func (f *walkerFixture) markLost(result *WalkResult, lost ...*proto.Ref) *proto.Commit {
	f.t.Helper()

	ctx := context.Background()

	obj, err := f.store.Get(ctx, result.Ref)
	require.NoError(f.t, err)

	marked, err := MarkLost(ctx, f.objects, obj, lost, nil)
	require.NoError(f.t, err)

	// as a store stamps it on receipt
	marked.GetCommit().ReceivedAtNs++
	require.NoError(f.t, f.index.Put(ctx, marked))

	latest, err := f.index.LatestCommit(ctx, "test")
	require.NoError(f.t, err)
	require.True(f.t, latest.Equal(marked.Ref()))

	return marked.GetCommit()
}

func TestAnIncompleteCommitRestoresWhatItKeptAndListsWhatItLost(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("a.txt", []byte("one"))
	f.write("sub/b.bin", f.random(64<<10))
	f.write("sub/c.txt", []byte("three"))
	f.write("other/d.txt", []byte("four"))

	result := f.run()
	sub := f.tree(result.Commit.Tree)["sub"]
	b := f.tree(sub.Ref)["b.bin"]
	file, err := f.store.Get(context.Background(), b.Ref)
	require.NoError(t, err)

	commit := f.markLost(result, file.GetFile().GetParts()[0].Ref, f.tree(result.Commit.Tree)["other"].Ref)
	require.True(t, commit.Incomplete)
	require.Equal(t, result.Commit.Timestamp, commit.Timestamp)

	top := f.tree(commit.Tree)
	require.True(t, top["other"].Stat.Lost)
	require.False(t, top["a.txt"].Stat.Lost)
	require.True(t, f.tree(top["sub"].Ref)["b.bin"].Stat.Lost)

	dst := t.TempDir()

	var lost []string

	restorer := &Restorer{Store: f.store}
	require.NoError(t, restorer.RestoreTree(context.Background(), commit, dst, TreeOptions{Done: func(r RestoredFile) error {
		if r.Outcome == OutcomeLost {
			lost = append(lost, r.Path)
		}

		return nil
	}}))

	require.ElementsMatch(t, []string{filepath.Join(dst, "other"), filepath.Join(dst, "sub", "b.bin")}, lost)
	require.EqualValues(t, 2, restorer.Stats().Lost)
	require.NoFileExists(t, filepath.Join(dst, "sub", "b.bin"))
	require.NoDirExists(t, filepath.Join(dst, "other"))

	content, err := os.ReadFile(filepath.Join(dst, "sub", "c.txt"))
	require.NoError(t, err)
	require.Equal(t, "three", string(content))

	again := f.run()
	top = f.tree(again.Commit.Tree)
	require.False(t, again.Commit.Incomplete)
	require.False(t, top["other"].Stat.Lost, "the next run reads a lost directory again")
	require.False(t, f.tree(top["sub"].Ref)["b.bin"].Stat.Lost, "and a lost file")
	require.Equal(t, "four", f.content(top["other"].Ref, "d.txt"))
}
