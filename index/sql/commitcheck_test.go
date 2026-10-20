package sql

import (
	"context"
	"errors"
	"testing"

	"github.com/gobackio/goback/auth"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/fileblob"
)

// sessionIndex is an index over a pack store, with a session open in ctx.
func sessionIndex(t *testing.T) (*Index, context.Context) {
	t.Helper()

	bucket, err := fileblob.OpenBucket(t.TempDir(), nil)
	require.NoError(t, err)

	packs, x := openPacks(t, bucket)
	x.DefaultPolicy = &retention.KeepAll

	ctx := auth.WithPrincipal(context.Background(), &auth.Principal{AgentID: "local"})
	sctx, err := packs.BeginSession(ctx, &backup.Session{AgentID: "local", Set: "a"})
	require.NoError(t, err)

	return x, sctx
}

func blobOf(data string) *proto.Object {
	return proto.NewObject(&proto.Blob{Data: []byte(data)})
}

func fileOf(splits []*proto.Ref, parts ...*proto.Object) *proto.Object {
	file := &proto.File{Splits: splits}
	for _, part := range parts {
		file.Parts = append(file.Parts, &proto.FilePart{Ref: part.Ref(), Length: uint64(len(part.GetBlob().GetData()))})
	}

	return proto.NewObject(file)
}

func treeOf(name string, dir bool, ref *proto.Ref) *proto.Object {
	stat := &proto.FileInfo{Name: []byte(name), Type: proto.NodeType_NODE_FILE, Mode: 0644}
	if dir {
		stat.Type, stat.Mode = proto.NodeType_NODE_DIRECTORY, 0755
	}

	return proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{{Stat: stat, Ref: ref}}})
}

// commitAt puts a commit of set a over root, answering the refs the store
// named missing.
func commitAt(t *testing.T, x *Index, ctx context.Context, timestamp int64, root *proto.Ref) []*proto.Ref {
	t.Helper()

	err := x.Put(ctx, proto.NewObject(&proto.Commit{Timestamp: timestamp, Tree: root, BackupSet: "a"}))

	var missing *backup.MissingError
	if !errors.As(err, &missing) {
		require.NoError(t, err)
		return nil
	}

	require.ErrorIs(t, err, backup.ErrDanglingRef)

	return missing.Refs
}

func hashesOf(refs ...*proto.Ref) []string {
	out := make([]string, len(refs))
	for i, ref := range refs {
		out[i] = string(ref.GetHash())
	}

	return out
}

func TestASessionStoresObjectsBeforeWhatTheyReferenceAndItsCommitNamesWhatIsMissing(t *testing.T) {
	x, ctx := sessionIndex(t)

	first, second, third := blobOf("first part"), blobOf("second part"), blobOf("third part")
	whole, split := fileOf(nil, first), fileOf(nil, second)
	file := fileOf([]*proto.Ref{split.Ref(), fileOf(nil, third).Ref()})
	sub := proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{
		treeOf("split.dat", false, file.Ref()).GetTree().Nodes[0],
		treeOf("whole.dat", false, whole.Ref()).GetTree().Nodes[0],
	}})
	root := treeOf("saves", true, sub.Ref())

	for _, obj := range []*proto.Object{root, sub, file, whole} {
		require.NoError(t, x.PutObjects(ctx, []*proto.Object{obj}))
	}

	require.ElementsMatch(t, hashesOf(first.Ref(), split.Ref(), fileOf(nil, third).Ref()), hashesOf(commitAt(t, x, ctx, 1, root.Ref())...),
		"the walk names the parts and splits it can't find")

	require.NoError(t, x.PutObjects(ctx, []*proto.Object{split, fileOf(nil, third)}))
	require.ElementsMatch(t, hashesOf(first.Ref(), second.Ref(), third.Ref()), hashesOf(commitAt(t, x, ctx, 2, root.Ref())...),
		"and goes on below the splits once they are stored")

	require.NoError(t, x.PutObjects(ctx, []*proto.Object{first, second, third}))
	require.Empty(t, commitAt(t, x, ctx, 3, root.Ref()))

	latest, err := x.LatestCommit(ctx, "a")
	require.NoError(t, err)

	obj, err := x.Get(ctx, latest)
	require.NoError(t, err)
	require.EqualValues(t, 3, obj.GetCommit().GetTimestamp())
}

func TestACommitNamesTheSubtreesAndTheRootItsSessionLacks(t *testing.T) {
	x, ctx := sessionIndex(t)

	file := fileOf(nil, blobOf("content"))
	sub := treeOf("world.dat", false, file.Ref())
	root := treeOf("saves", true, sub.Ref())

	require.ElementsMatch(t, hashesOf(root.Ref()), hashesOf(commitAt(t, x, ctx, 1, root.Ref())...))

	require.NoError(t, x.Put(ctx, root))
	require.ElementsMatch(t, hashesOf(sub.Ref()), hashesOf(commitAt(t, x, ctx, 2, root.Ref())...))

	_, err := x.LatestCommit(ctx, "a")
	require.ErrorIs(t, err, backup.ErrNotFound, "a commit missing anything is not indexed")
}

func TestAWriteOutsideASessionStillNeedsWhatItReferences(t *testing.T) {
	bucket, err := fileblob.OpenBucket(t.TempDir(), nil)
	require.NoError(t, err)

	_, x := openPacks(t, bucket)

	file := fileOf(nil, blobOf("never stored"))
	require.ErrorIs(t, x.PutObjects(context.Background(), []*proto.Object{file}), backup.ErrDanglingRef)
}
