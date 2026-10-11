package backup

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/gobackio/goback/backup/chunker"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// growingFixture commits a file outside the walk's root as a growing file.
type growingFixture struct {
	*walkerFixture
	path    string
	content []byte
}

func newGrowingFixture(t *testing.T, size int) *growingFixture {
	f := &growingFixture{walkerFixture: newWalkerFixture(t), path: filepath.Join(t.TempDir(), "open")}
	f.content = f.random(size)
	require.NoError(t, os.WriteFile(f.path, f.content, 0o600))

	return f
}

func (f *growingFixture) grow(name, from string, size int) *WalkResult {
	f.t.Helper()

	f.walker.Growing = []Growing{{Name: name, Path: f.path, Size: int64(size), From: from}}

	return f.run()
}

func (f *growingFixture) tree(ref *proto.Ref) map[string]*proto.TreeNode {
	f.t.Helper()

	tree, err := OpenTree(context.Background(), f.store, ref, f.walker.Key, nil)
	require.NoError(f.t, err)

	nodes := make(map[string]*proto.TreeNode, len(tree.Nodes))
	for _, node := range tree.Nodes {
		nodes[string(node.Stat.Name)] = node
	}

	return nodes
}

func (f *growingFixture) parts(result *WalkResult, name string) []*proto.FilePart {
	f.t.Helper()

	node := f.tree(result.Commit.Tree)[name]
	require.NotNil(f.t, node, "the commit holds %s", name)

	obj, err := f.store.Get(context.Background(), node.Ref)
	require.NoError(f.t, err)

	parts, err := FileParts(context.Background(), f.store, obj.GetFile())
	require.NoError(f.t, err)

	return parts
}

func (f *growingFixture) restored(result *WalkResult, name string) []byte {
	f.t.Helper()

	node := f.tree(result.Commit.Tree)[name]
	dst := filepath.Join(f.t.TempDir(), name)

	_, err := (&Restorer{Store: f.store, Key: f.walker.Key}).RestoreFile(context.Background(), dst, node.Stat, node.Ref)
	require.NoError(f.t, err)

	content, err := os.ReadFile(dst)
	require.NoError(f.t, err)

	return content
}

// fullRead is the ref a walk that read size bytes of the content whole
// would give the file.
func (f *growingFixture) fullRead(size int) *proto.Ref {
	f.t.Helper()

	ref, err := PutFile(context.Background(), newMemStore(), f.walker.Key, int64(size), bytes.NewReader(f.content[:size]))
	require.NoError(f.t, err)

	return ref
}

func TestAGrowingFileCommitsOnlyItsLength(t *testing.T) {
	f := newGrowingFixture(t, 300<<10)
	f.write("whole", []byte("a whole file"))

	result := f.grow("open.partial", "", 100<<10)

	nodes := f.tree(result.Commit.Tree)
	require.Len(t, nodes, 2)
	require.EqualValues(t, 100<<10, nodes["open.partial"].Stat.Size)
	require.Equal(t, f.content[:100<<10], f.restored(result, "open.partial"))
	require.True(t, nodes["open.partial"].Ref.Equal(f.fullRead(100<<10)))
}

func TestAppendingKeepsEveryPartButTheLastAndCutsAsAFullRead(t *testing.T) {
	for _, keyed := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "sealed"}[keyed], func(t *testing.T) {
			f := newGrowingFixture(t, 8<<20)
			if keyed {
				key, err := storekey.Generate("s1")
				require.NoError(t, err)
				f.walker.Key = key
			}

			sizes := []int{1 << 20, 1<<20 + 1, 3<<20 + 12345, 8 << 20}

			var previous []*proto.FilePart
			for i, size := range sizes {
				result := f.grow("open.partial", "", size)
				parts := f.parts(result, "open.partial")

				require.True(t, f.tree(result.Commit.Tree)["open.partial"].Ref.Equal(f.fullRead(size)), "%d bytes cut as a full read of them would", size)
				require.Equal(t, f.content[:size], f.restored(result, "open.partial"))

				if i > 0 {
					kept := previous[:len(previous)-1]
					for j, part := range kept {
						require.True(t, parts[j].Ref.Equal(part.Ref), "part %d of %d moved", j, len(kept))
						require.Equal(t, part.Offset, parts[j].Offset)
					}

					fresh := uint64(size) - kept[len(kept)-1].Offset - kept[len(kept)-1].Length
					require.LessOrEqual(t, uint64(result.Uploaded), fresh, "only the bytes from the last part on are uploaded")
				}

				previous = parts
			}
		})
	}
}

func TestChunkBoundariesBeforeTheLastDoNotMoveOnAppend(t *testing.T) {
	f := newGrowingFixture(t, 4<<20)

	for _, size := range []int{chunker.MaxSize + 1, 1 << 20, 2<<20 + 7, 4 << 20} {
		whole := chunker.Split(f.content)
		prefix := chunker.Split(f.content[:size])

		for i := 0; i < len(prefix)-1; i++ {
			require.Equal(t, whole[i], prefix[i], "chunk %d of the first %d bytes", i, size)
		}
	}
}

func TestAClosedFileContinuesTheOpenVersionItWas(t *testing.T) {
	f := newGrowingFixture(t, 2<<20)

	open := f.grow("seg.partial", "", 1<<20)
	before := f.parts(open, "seg.partial")

	result := f.grow("seg", "seg.partial", 2<<20)
	nodes := f.tree(result.Commit.Tree)
	require.Contains(t, nodes, "seg")
	require.True(t, nodes["seg"].Ref.Equal(f.fullRead(2<<20)))
	require.True(t, f.parts(result, "seg")[0].Ref.Equal(before[0].Ref))
	require.Less(t, result.Uploaded, int64(2<<20)-int64(before[len(before)-2].Offset+before[len(before)-2].Length)+1)
}

func TestAGrowingFileShorterThanItsLastVersionIsReadWhole(t *testing.T) {
	f := newGrowingFixture(t, 2<<20)

	f.grow("open.partial", "", 2<<20)
	result := f.grow("open.partial", "", 1<<20)

	require.True(t, f.tree(result.Commit.Tree)["open.partial"].Ref.Equal(f.fullRead(1<<20)))
	require.Equal(t, f.content[:1<<20], f.restored(result, "open.partial"))
}

func TestAGrowingFileShorterOnDiskThanItsLengthFails(t *testing.T) {
	f := newGrowingFixture(t, 10)
	f.walker.Growing = []Growing{{Name: "open", Path: f.path, Size: 11}}

	_, err := f.walker.Run(context.Background())
	require.ErrorContains(t, err, "fewer than")
}
