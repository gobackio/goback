package storage

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/gobackio/goback/auth"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// putAll stores objects, each after those it references.
func putAll(t *testing.T, ctx context.Context, store *Store, objects ...*proto.Object) {
	t.Helper()

	for _, object := range objects {
		_, err := store.Put(ctx, Upload{Object: object})
		require.NoError(t, err)
	}
}

func repeatedRefs(ref *proto.Ref, n int) []*proto.Ref {
	refs := make([]*proto.Ref, n)
	for i := range refs {
		refs[i] = ref
	}

	return refs
}

func TestATreeReachedManyTimesIsHandedOverOnce(t *testing.T) {
	ctx := auth.WithPrincipal(context.Background(), &auth.Principal{AgentID: "node-1"})
	store := NewStore(newMemIndex(), nil)

	file := proto.NewObject(&proto.File{Inline: []byte("save")})
	tree := proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{{Stat: &proto.FileInfo{Name: []byte("f")}, Ref: file.Ref()}}})
	putAll(t, ctx, store, file, tree)

	distinct := 1
	for range 3 {
		nodes := make([]*proto.TreeNode, 30)
		for i := range nodes {
			nodes[i] = &proto.TreeNode{Stat: &proto.FileInfo{Name: fmt.Appendf(nil, "d%02d", i), Type: proto.NodeType_NODE_DIRECTORY}, Ref: tree.Ref()}
		}

		tree = proto.NewObject(&proto.Tree{Nodes: nodes})
		putAll(t, ctx, store, tree)
		distinct++
	}

	handed := 0
	require.NoError(t, store.Tree(ctx, tree.Ref(), 10, func(*proto.GetTreeResponse) error {
		handed++
		return nil
	}))
	require.Equal(t, distinct, handed)
}

// splitFixture is a file of split files that repeat some of their splits,
// at depth 2, and the blobs it holds.
func splitFixture(t *testing.T, ctx context.Context, store *Store) (*proto.Object, []*proto.Object) {
	t.Helper()

	var blobs []*proto.Object
	for i := range 4 {
		blob := proto.NewObject(&proto.Blob{Data: fmt.Appendf(nil, "blob %d", i)})
		putAll(t, ctx, store, blob)
		blobs = append(blobs, blob)
	}

	part := func(offset uint64, blob *proto.Object) *proto.FilePart {
		return &proto.FilePart{Offset: offset, Length: 6, Ref: blob.Ref()}
	}

	one := proto.NewObject(&proto.File{Parts: []*proto.FilePart{part(0, blobs[0]), part(6, blobs[1])}})
	two := proto.NewObject(&proto.File{Parts: []*proto.FilePart{part(0, blobs[2])}})
	mid := proto.NewObject(&proto.File{SplitDepth: 1, Splits: []*proto.Ref{one.Ref(), two.Ref(), one.Ref()}, SplitLengths: []uint64{12, 6, 12}})
	tail := proto.NewObject(&proto.File{Parts: []*proto.FilePart{part(0, blobs[3]), part(6, blobs[0])}})
	top := proto.NewObject(&proto.File{SplitDepth: 2, Splits: []*proto.Ref{mid.Ref(), two.Ref(), mid.Ref(), tail.Ref()}, SplitLengths: []uint64{30, 6, 30, 12}})
	putAll(t, ctx, store, one, two, mid, tail, top)

	return top, blobs
}

func TestReadFileHandsOverEveryPartOfASplitFile(t *testing.T) {
	ctx := auth.WithPrincipal(context.Background(), &auth.Principal{AgentID: "node-1"})
	store := NewStore(newMemIndex(), nil)
	top, _ := splitFixture(t, ctx, store)

	parts, err := backup.FileParts(ctx, store.Index, top.GetFile())
	require.NoError(t, err)

	want := map[uint32]string{}
	for i, part := range parts {
		if i != 1 {
			want[uint32(i)] = string(part.Ref.Hash)
		}
	}

	got := map[uint32]string{}
	require.NoError(t, store.ReadFile(ctx, top.Ref(), []uint32{1}, func(resp *proto.ReadFileResponse) error {
		got[resp.Index] = string(decoded(t, resp.Object).Ref().Hash)
		return nil
	}))
	require.Equal(t, want, got)
}

func TestReadFilesHandsOverTheFirstOfEachPart(t *testing.T) {
	ctx := auth.WithPrincipal(context.Background(), &auth.Principal{AgentID: "node-1"})
	store := NewStore(newMemIndex(), nil)
	top, blobs := splitFixture(t, ctx, store)

	other := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Length: 6, Ref: blobs[0].Ref()}, {Offset: 6, Length: 6, Ref: blobs[2].Ref()}}})
	putAll(t, ctx, store, other)

	// the first part is skipped, so the next copy of its blob comes instead
	files := []*proto.ReadFilesEntry{{Ref: top.Ref(), SkipParts: []uint32{0, 2}}, {Ref: other.Ref()}}

	parts, err := backup.FileParts(ctx, store.Index, top.GetFile())
	require.NoError(t, err)

	type at struct{ file, index uint32 }

	want := map[at]string{}
	seen := map[string]bool{}

	for i, part := range parts {
		if i != 0 && i != 2 && !seen[string(part.Ref.Hash)] {
			seen[string(part.Ref.Hash)] = true
			want[at{0, uint32(i)}] = string(part.Ref.Hash)
		}
	}

	for i, part := range other.GetFile().GetParts() {
		if !seen[string(part.Ref.Hash)] {
			seen[string(part.Ref.Hash)] = true
			want[at{1, uint32(i)}] = string(part.Ref.Hash)
		}
	}

	got := map[at]string{}
	require.NoError(t, store.ReadFiles(ctx, files, false, func(resp *proto.ReadFilesResponse) error {
		if resp.Part {
			got[at{resp.File, resp.Index}] = string(decoded(t, resp.Object).Ref().Hash)
		}

		return nil
	}))
	require.Equal(t, want, got)
}

func TestReadFilesHoldsNoPartsRepeatedSplitsExpandTo(t *testing.T) {
	ctx := auth.WithPrincipal(context.Background(), &auth.Principal{AgentID: "node-1"})
	store := NewStore(newMemIndex(), nil)

	blob := proto.NewObject(&proto.Blob{Data: []byte("x")})
	parts := make([]*proto.FilePart, 200)
	for i := range parts {
		parts[i] = &proto.FilePart{Offset: uint64(i), Length: 1, Ref: blob.Ref()}
	}

	leaf := proto.NewObject(&proto.File{Parts: parts})
	mid := proto.NewObject(&proto.File{Splits: repeatedRefs(leaf.Ref(), 200)})
	top := proto.NewObject(&proto.File{Splits: repeatedRefs(mid.Ref(), 200)})
	putAll(t, ctx, store, blob, leaf, mid, top)

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	handed := 0
	require.NoError(t, store.ReadFiles(ctx, []*proto.ReadFilesEntry{{Ref: top.Ref()}}, false, func(resp *proto.ReadFilesResponse) error {
		if resp.Part {
			handed++
		}

		return nil
	}))

	runtime.ReadMemStats(&after)

	require.Equal(t, 1, handed, "eight million parts of one blob")
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(16<<20), "the read expanded the parts")
}
