package sql

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage"
	"github.com/gobackio/goback/storage/pack"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob"
	"gocloud.dev/blob/fileblob"
)

func openPacks(t *testing.T, bucket *blob.Bucket) (*pack.PackStorage, *Index) {
	t.Helper()

	index := NewMemory(t.Name()+"-"+t.TempDir(), nil)
	packs, err := pack.NewPackStorage(pack.WithArchiveStorage(storage.NewBucketStore(bucket)), pack.WithArchiveIndex(index),
		pack.WithCompaction(pack.CompactionConfig{Workers: 1}))
	require.NoError(t, err)
	index.ObjectStore = packs
	require.NoError(t, index.Open())
	require.NoError(t, packs.Open())
	t.Cleanup(func() { _ = packs.Close() })

	return packs, index
}

func TestAPinIsInTheStoreOnceItIsIndexed(t *testing.T) {
	ctx := context.Background()
	bucket, err := fileblob.OpenBucket(t.TempDir(), nil)
	require.NoError(t, err)
	_, index := openPacks(t, bucket)

	src := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(src, "level.dat"), []byte("the world"), 0o644))

	walker := &backup.Walker{Index: index, Objects: index, Set: "world", AgentID: "a", Root: src, Workers: 1}
	result, err := walker.Run(ctx)
	require.NoError(t, err)

	pin := proto.NewObject(&proto.Pin{Target: result.Ref})
	require.NoError(t, index.Put(ctx, pin))

	rebuilt, _ := openPacks(t, bucket)
	has, err := rebuilt.Has(ctx, pin.Ref())
	require.NoError(t, err)
	require.True(t, has, "a store opened after the pin holds it")
}

func TestTombstonesAreFoundThroughCompactionAndRebuild(t *testing.T) {
	ctx := context.Background()
	bucket, err := fileblob.OpenBucket(t.TempDir(), nil)
	require.NoError(t, err)
	packs, index := openPacks(t, bucket)

	kept := proto.NewObject(&proto.File{Inline: []byte("kept")})
	gone := proto.NewObject(&proto.File{Inline: []byte("gone")})
	back := proto.NewObject(&proto.File{Inline: []byte("back")})

	step := func(do func() error) {
		t.Helper()
		// an archive's version is its file's modification time, which may
		// not tick between two quick writes
		time.Sleep(20 * time.Millisecond)
		require.NoError(t, do())
		require.NoError(t, packs.Flush())
	}

	for _, obj := range []*proto.Object{kept, gone, back} {
		step(func() error { return packs.Put(ctx, obj) })
	}

	step(func() error { return packs.Delete(ctx, gone.Ref()) })
	step(func() error { return packs.Delete(ctx, back.Ref()) })
	step(func() error { return packs.Delete(ctx, proto.TombstoneRef(back.Ref())) })

	present := func(packs *pack.PackStorage, index *Index, stage string) {
		t.Helper()

		for obj, want := range map[*proto.Object]bool{kept: true, gone: false, back: true} {
			has, err := packs.Has(ctx, obj.Ref())
			require.NoError(t, err)
			require.Equal(t, want, has, "%s: %s", stage, obj.GetFile().Inline)
		}

		tombs := []*proto.Ref{proto.TombstoneRef(gone.Ref()), proto.TombstoneRef(back.Ref()), proto.TombstoneRef(proto.TombstoneRef(back.Ref()))}
		found, err := index.LocateTombstones(append(tombs, kept.Ref()), pack.Scope{})
		require.NoError(t, err)
		require.Len(t, found, len(tombs), "%s: the tombstone records alone", stage)
		for _, tomb := range tombs {
			require.Contains(t, found, string(tomb.Hash), stage)
		}
	}

	present(packs, index, "written")

	_, err = packs.Compact(context.Background())
	require.NoError(t, err)
	loc, err := index.LocateObject(proto.TombstoneRef(gone.Ref()), pack.Scope{})
	require.NoError(t, err)
	require.NotZero(t, loc.Record.CarriedTime, "the tombstone was rewritten")
	present(packs, index, "compacted")

	rebuilt, rebuiltIndex := openPacks(t, bucket)
	require.NoError(t, reindexErr(rebuiltIndex.ReIndex(ctx)))
	present(rebuilt, rebuiltIndex, "rebuilt")
}
