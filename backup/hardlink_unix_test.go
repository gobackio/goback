//go:build !windows

package backup

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAWalkReadsAHardLinkedFileOnceAndARestoreLinksItAgain(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("a", f.random(3*maxBlobSize))
	require.NoError(t, os.Link(filepath.Join(f.root, "a"), filepath.Join(f.root, "b")))
	require.NoError(t, os.MkdirAll(filepath.Join(f.root, "sub"), 0o755))
	require.NoError(t, os.Link(filepath.Join(f.root, "a"), filepath.Join(f.root, "sub", "c")))

	first := f.run()
	require.EqualValues(t, 1, first.Read)
	require.EqualValues(t, 3, first.Files)

	root := f.tree(first.Commit.Tree)
	c := f.tree(root["sub"].Ref)["c"]
	require.Empty(t, root["a"].Stat.HardLink)
	require.Equal(t, "a", string(root["b"].Stat.HardLink))
	require.Equal(t, "a", string(c.Stat.HardLink))
	require.True(t, root["b"].Ref.Equal(root["a"].Ref))
	require.True(t, c.Ref.Equal(root["a"].Ref))

	second := f.run()
	require.Zero(t, second.Read)
	require.True(t, second.Commit.Tree.Equal(first.Commit.Tree))

	dst := t.TempDir()
	r := &Restorer{Store: f.store}
	require.NoError(t, r.RestoreTree(context.Background(), first.Commit, dst, TreeOptions{}))
	require.EqualValues(t, 2, r.Stats().Linked)
	requireSameFile(t, filepath.Join(dst, "a"), filepath.Join(dst, "b"))
	requireSameFile(t, filepath.Join(dst, "a"), filepath.Join(dst, "sub", "c"))
}

func TestALinkedFileChangedAfterItsFirstPathWasReadIsReadAgain(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("a", []byte("one"))
	require.NoError(t, os.Link(filepath.Join(f.root, "a"), filepath.Join(f.root, "b")))

	first := f.run()

	f.write("a", []byte("other"))

	second := f.run()
	root := f.tree(second.Commit.Tree)
	require.EqualValues(t, 1, second.Read)
	require.Equal(t, "other", f.content(second.Commit.Tree, "b"))
	require.Equal(t, "a", string(root["b"].Stat.HardLink))
	require.False(t, second.Commit.Tree.Equal(first.Commit.Tree))
}
