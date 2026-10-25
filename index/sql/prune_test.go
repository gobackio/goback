package sql

import (
	"fmt"
	"testing"
	"time"

	"github.com/gobackio/goback/index/sql/ent/file"
	"github.com/gobackio/goback/index/sql/ent/tree"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// withXattr gives a node an extended attribute whose value is the object.
func withXattr(node *proto.TreeNode, value *proto.Ref) *proto.TreeNode {
	node.Stat.Xattrs = []*proto.Stream{{Name: []byte("user.tag"), Ref: value, Size: 1}}

	return node
}

func TestRetiringACommitLikeItsNeighboursReadsNoTree(t *testing.T) {
	f := newFixture(t)

	sub := f.dir("world", f.file("a.txt", "one"))
	root := f.tree(sub, f.file("b.txt", "two"))

	var commits []*proto.Ref
	for range 3 {
		commits = append(commits, f.commit("world", root, false))
		f.advance(time.Hour)
	}

	require.NoError(t, f.x.DeleteCommit(f.ctx, commits[1]))

	f.store.mu.Lock()
	clear(f.store.read)
	f.store.mu.Unlock()

	require.Equal(t, 1, f.retire(15))

	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	require.Zero(t, f.store.read[string(root.Ref().Hash)])
	require.Zero(t, f.store.read[string(sub.Ref.Hash)])
}

func TestRetiringACommitKeepsWhatACommitFurtherAwayShares(t *testing.T) {
	f := newFixture(t)

	tag := f.file("value", "v")
	empty := f.dir("empty")

	first := f.tree(f.dir("cfg", withXattr(f.file("a", "a"), tag.Ref), f.dir("e2")))
	f.commit("world", first, false)
	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("other", "2")), false)
	f.advance(time.Hour)

	only := f.file("b", "only the retired commit holds this")
	retired := f.tree(withXattr(only, tag.Ref), empty, f.file("c", "a"))
	gone := f.commit("world", retired, false)
	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("other", "3")), false)

	require.NoError(t, f.x.DeleteCommit(f.ctx, gone))
	require.Equal(t, 1, f.retire(15))

	require.True(t, f.references(f.ctx, tag.Ref), "an attribute value an older commit holds")
	require.True(t, f.references(f.ctx, empty.Ref), "a directory an older commit holds elsewhere")
	require.False(t, f.references(f.ctx, only.Ref))
	require.False(t, f.references(f.ctx, retired.Ref()))
}

func TestRetiringCommitsBetweenOthersDropsEveryRowOnlyTheyHeld(t *testing.T) {
	f := newFixture(t)

	var commits []*proto.Ref
	for i := range 9 {
		commits = append(commits, f.commit("world", f.tree(
			f.dir("stable", f.file("s.txt", "same")),
			f.dir("changing", f.file("c.txt", fmt.Sprint(i/2))),
			f.file(fmt.Sprintf("only-%d.txt", i), "x"),
		), false))
		f.advance(time.Hour)
	}

	for _, i := range []int{1, 3, 4, 7} {
		require.NoError(t, f.x.DeleteCommit(f.ctx, commits[i]))
	}

	require.Equal(t, 4, f.retire(15))

	files, err := f.x.client.File.Query().Where(file.SetID(f.setID("world"))).Count(f.ctx)
	require.NoError(t, err)
	trees, err := f.x.client.Tree.Query().Where(tree.SetID(f.setID("world"))).Count(f.ctx)
	require.NoError(t, err)

	require.NoError(t, f.x.dropDeadRows(f.ctx, f.setID("world")))

	after, err := f.x.client.File.Query().Where(file.SetID(f.setID("world"))).Count(f.ctx)
	require.NoError(t, err)
	require.Equal(t, files, after, "no files row is left that no live commit holds")

	after, err = f.x.client.Tree.Query().Where(tree.SetID(f.setID("world"))).Count(f.ctx)
	require.NoError(t, err)
	require.Equal(t, trees, after, "no trees row is left that no live commit holds")

	for _, i := range []int{0, 2, 5, 6, 8} {
		versions, err := f.x.FileInfo(f.ctx, "world", fmt.Sprintf("only-%d.txt", i), f.clock, 10)
		require.NoError(t, err)
		require.Len(t, versions, 1, "commit %d keeps its file", i)
	}

	for _, i := range []int{1, 3, 4, 7} {
		versions, err := f.x.FileInfo(f.ctx, "world", fmt.Sprintf("only-%d.txt", i), f.clock, 10)
		require.NoError(t, err)
		require.Empty(t, versions, "commit %d took its file", i)
	}
}
