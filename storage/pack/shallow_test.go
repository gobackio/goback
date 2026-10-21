package pack

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gobackio/goback/proto"
)

func TestACollectionKeepsAShallowRootsTreesAndFilesButNotTheirParts(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	blobs := makeTestData(t, 20)
	only, shared := blobs[:10], blobs[10:]

	files := makeGCFiles(blobs)
	root := treeOf(files)
	shallow := proto.NewObject(&proto.Commit{Tree: root.Ref(), Timestamp: 1, BackupSet: "world"})

	sharedFiles := makeGCFiles(shared)
	sharedRoot := treeOf(sharedFiles)
	whole := proto.NewObject(&proto.Commit{Tree: sharedRoot.Ref(), Timestamp: 2, BackupSet: "other"})

	putAll(t, store, append(append(append(append([]*proto.Object{}, blobs...), files...), sharedFiles...), root, shallow, sharedRoot, whole))

	opts := func(ahead time.Duration) CollectOptions {
		o := gcOptions(t, ahead)
		o.Shallow = func(ref []byte) bool { return bytes.Equal(ref, shallow.Ref().Hash) }

		return o
	}

	_, err := store.Collect(ctx, opts(0))
	require.NoError(t, err)

	_, err = store.Collect(ctx, opts(48*time.Hour))
	require.NoError(t, err)

	requireStored(t, store, append(append([]*proto.Object{}, files...), root, shallow), true)
	requireStored(t, store, only, false)
	requireStored(t, store, shared, true)
}
