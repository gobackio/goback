package cache_test

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/index/sql"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage"
	"github.com/gobackio/goback/storage/cache"
	"github.com/gobackio/goback/storage/pack"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"
)

// readCounting counts the commits and trees read from the store, which
// streams a subtree in one call as a remote store does.
type readCounting struct {
	*sql.Index
	reads atomic.Int64
}

func (c *readCounting) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	obj, err := c.Index.Get(ctx, ref)
	if obj.Type() == proto.ObjectType_COMMIT || obj.Type() == proto.ObjectType_TREE {
		c.reads.Add(1)
	}

	return obj, err
}

func (c *readCounting) GetTree(ctx context.Context, ref *proto.Ref, _ uint32) ([]*proto.Object, error) {
	c.reads.Add(1)

	obj, err := c.Index.Get(ctx, ref)
	if err != nil {
		return nil, err
	}

	return []*proto.Object{obj}, nil
}

func TestABackupDiffsAgainstWhatTheLastOneWroteWithoutReadingIt(t *testing.T) {
	ctx := context.Background()

	store, err := pack.NewPackStorage(pack.WithArchiveStorage(storage.NewBucketStore(memblob.OpenBucket(nil))), pack.WithArchiveIndex(pack.NewInMemoryIndex()))
	require.NoError(t, err)
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })

	index := sql.NewMemory(t.Name(), store)
	require.NoError(t, index.Open())
	t.Cleanup(func() { _ = index.Close() })

	root := t.TempDir()
	for _, dir := range []string{"a", "b", "c"} {
		require.NoError(t, os.MkdirAll(filepath.Join(root, dir, "deep"), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(root, dir, "deep", "file"), []byte(dir), 0o600))
	}

	copies := cache.NewMemory()
	run := func() int64 {
		objects := &readCounting{Index: index}
		w := &backup.Walker{Index: index, Objects: cache.New(copies, objects), Set: "s", AgentID: "a", Root: root, Workers: 1, PrefetchDepth: 2}
		_, err := w.Run(ctx)
		require.NoError(t, err)

		return objects.reads.Load()
	}

	run()

	require.NoError(t, os.WriteFile(filepath.Join(root, "b", "deep", "file"), []byte("changed"), 0o600))
	require.Zero(t, run())
	require.Zero(t, run())
}

func TestRecentCopiesHoldWhatTheLastGenerationKeptOrRead(t *testing.T) {
	read := proto.NewObject(&proto.Blob{Data: []byte("read")})
	kept := proto.NewObject(&proto.Blob{Data: []byte("kept")})
	idle := proto.NewObject(&proto.Blob{Data: []byte("idle")})

	copies := cache.NewRecent()
	require.NoError(t, copies.Put(read.Ref(), read))
	require.NoError(t, copies.Put(idle.Ref(), idle))

	copies.Rotate()
	_, ok := copies.Get(read.Ref())
	require.True(t, ok)
	require.NoError(t, copies.Put(kept.Ref(), kept))

	copies.Rotate()
	_, ok = copies.Get(idle.Ref())
	require.False(t, ok, "a generation that neither kept nor read it dropped it")

	for _, obj := range []*proto.Object{read, kept} {
		_, ok = copies.Get(obj.Ref())
		require.True(t, ok)
	}
}
