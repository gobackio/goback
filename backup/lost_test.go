package backup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
	pb "google.golang.org/protobuf/proto"
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

// splitLost makes a commit whose directory big lost one of its tree splits,
// and a target directory that holds the live files the commit lost.
func splitLost(t *testing.T) (f *walkerFixture, commit *proto.Commit, dst string, gone []string) {
	f = newWalkerFixture(t)
	f.write("a.txt", []byte("one"))

	for i := range 3 * treeFanout {
		f.write(fmt.Sprintf("big/%04d.txt", i), []byte(strconv.Itoa(i)))
	}

	result := f.run()
	big := f.tree(result.Commit.Tree)["big"]

	obj, err := f.store.Get(context.Background(), big.Ref)
	require.NoError(t, err)
	splits := obj.GetTree().GetSplits()
	require.Greater(t, len(splits), 1)

	for name := range f.tree(splits[0]) {
		gone = append(gone, filepath.Join("big", name))
	}

	commit = f.markLost(result, splits[0])
	require.True(t, commit.Incomplete)

	dst = t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dst, "big"), 0o755))

	for _, rel := range gone {
		require.NoError(t, os.WriteFile(filepath.Join(dst, rel), []byte("live"), 0o644))
	}

	return f, commit, dst, gone
}

func TestARestoreDeletingWhatItLacksRefusesACommitThatLostASplit(t *testing.T) {
	f, commit, dst, gone := splitLost(t)

	restorer := &Restorer{Store: f.store}
	err := restorer.RestoreTree(context.Background(), commit, dst, TreeOptions{Delete: true})
	require.ErrorIs(t, err, ErrIncomplete)
	require.NoFileExists(t, filepath.Join(dst, "a.txt"), "nothing is written")

	for _, rel := range gone {
		require.FileExists(t, filepath.Join(dst, rel))
	}

	unmarked := pb.Clone(commit).(*proto.Commit)
	unmarked.Incomplete = false
	err = restorer.RestoreTree(context.Background(), unmarked, dst, TreeOptions{Delete: true})
	require.ErrorIs(t, err, ErrIncomplete, "a tree marked incomplete is refused as well")
	require.NoFileExists(t, filepath.Join(dst, "a.txt"))

	require.NoError(t, restorer.RestoreTree(context.Background(), commit, dst, TreeOptions{Delete: true, DeleteIncomplete: true}))
	require.FileExists(t, filepath.Join(dst, "a.txt"))
	require.NoFileExists(t, filepath.Join(dst, gone[0]), "allowed, the restore removes what the commit lost")
}

func TestARestoreOfACommitThatLostASplitCountsTheDirectory(t *testing.T) {
	f, commit, dst, gone := splitLost(t)

	restorer := &Restorer{Store: f.store}
	require.NoError(t, restorer.RestoreTree(context.Background(), commit, dst, TreeOptions{}))
	require.EqualValues(t, 1, restorer.Stats().PartialDirs)
	require.Zero(t, restorer.Stats().Lost)
	require.FileExists(t, filepath.Join(dst, "a.txt"))
	require.FileExists(t, filepath.Join(dst, gone[0]), "what the commit lost is left as it is")
}

func TestARestoreOfACommitThatLostItsRootDeletesNothing(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("a.txt", []byte("one"))
	result := f.run()
	commit := f.markLost(result, result.Commit.Tree)

	dst := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dst, "a.txt"), []byte("live"), 0o644))

	restorer := &Restorer{Store: f.store}
	require.ErrorIs(t, restorer.RestoreTree(context.Background(), commit, dst, TreeOptions{Delete: true}), ErrIncomplete)
	require.FileExists(t, filepath.Join(dst, "a.txt"))

	require.NoError(t, restorer.RestoreTree(context.Background(), commit, dst, TreeOptions{}))
	require.EqualValues(t, 1, restorer.Stats().PartialDirs)
}
