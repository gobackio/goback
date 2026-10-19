package pack

import (
	"context"
	"testing"
	"time"

	"github.com/gobackio/goback/proto"
	"github.com/stretchr/testify/require"
)

func TestCollectKeepsTheSecurityDescriptorsAndStreamsATreeRecords(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	ctx := context.Background()

	blobs := makeTestData(t, 2)
	security := proto.NewObject(&proto.File{Inline: []byte("descriptor")})
	stream := makeGCFiles(blobs[:1])[0]
	content := makeGCFiles(blobs[1:])[0]

	tree := proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{{
		Stat: &proto.FileInfo{
			Name:     []byte("file"),
			Size:     1,
			Security: security.Ref(),
			Streams:  []*proto.Stream{{Name: []byte("Zone.Identifier"), Ref: stream.Ref(), Size: 1}},
		},
		Ref: content.Ref(),
	}}})
	commit := proto.NewObject(&proto.Commit{Tree: tree.Ref(), Timestamp: 1, BackupSet: "world"})

	reachable := append([]*proto.Object{security, stream, content, tree, commit}, blobs...)
	putAll(t, store, reachable)

	for _, ahead := range []time.Duration{0, 48 * time.Hour} {
		report, err := store.Collect(ctx, gcOptions(t, ahead))
		require.NoError(t, err)
		require.Zero(t, report.DeadObjects)
	}

	requireStored(t, store, reachable, true)
	require.NoError(t, store.Close())
}
