package pack

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/pebble"

	"github.com/stretchr/testify/require"
	pb "google.golang.org/protobuf/proto"
)

// countingStorage counts the reads of archive files, so a test can see how
// many the mark took.
type countingStorage struct {
	*localArchiveStorage

	mu    sync.Mutex
	reads int
	bytes int64
}

func (c *countingStorage) Open(name string) (File, error) {
	file, err := c.localArchiveStorage.Open(name)
	if err != nil || !strings.HasSuffix(name, ArchiveSuffix) {
		return file, err
	}

	return &countingFile{File: file, storage: c}, nil
}

func (c *countingStorage) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.reads
}

type countingFile struct {
	File
	storage *countingStorage
}

func (c *countingFile) ReadAt(p []byte, off int64) (int, error) {
	c.storage.mu.Lock()
	c.storage.reads++
	c.storage.bytes += int64(len(p))
	c.storage.mu.Unlock()

	return c.File.(io.ReaderAt).ReadAt(p, off)
}

func (c *countingFile) Read(p []byte) (int, error) {
	n, err := c.File.Read(p)

	c.storage.mu.Lock()
	c.storage.bytes += int64(n)
	c.storage.mu.Unlock()

	return n, err
}

func TestMarkReadsNeighboursTogether(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	storage := &countingStorage{localArchiveStorage: newLocal(base)}

	open := func() *PackStorage {
		store, err := NewPackStorage(WithArchiveStorage(storage), WithArchiveIndex(NewInMemoryIndex()))
		require.NoError(t, err)
		require.NoError(t, store.Open())

		return store
	}

	store := open()

	const files = 60

	children := make([]*proto.Object, 0, files)
	for i := range files {
		file := proto.NewObject(&proto.File{Inline: []byte(fmt.Sprintf("file %d", i))})
		require.NoError(t, store.Put(ctx, file))
		children = append(children, file)
	}

	tree := treeOf(children)
	require.NoError(t, store.Put(ctx, tree))

	commit := proto.NewObject(&proto.Commit{Tree: tree.Ref(), Timestamp: 1, BackupSet: "world"})
	require.NoError(t, store.Put(ctx, commit))
	require.NoError(t, store.Close())

	store = open()
	t.Cleanup(func() { _ = store.Close() })

	before := storage.count()

	report, err := store.Collect(ctx, CollectOptions{NoSweep: true, TempDir: t.TempDir()})
	require.NoError(t, err)
	require.EqualValues(t, files+2, report.Marked, "every object of the commit is reachable")

	reads := storage.count() - before
	require.Less(t, reads, files/2, "neighbouring records come back in one read, not one each")
}

func TestMarkReadsWhatTheMetadataCacheHoldsFromIt(t *testing.T) {
	ctx := context.Background()
	storage := &countingStorage{localArchiveStorage: newLocal(t.TempDir())}

	store, err := NewPackStorage(WithArchiveStorage(storage), WithArchiveIndex(NewInMemoryIndex()))
	require.NoError(t, err)
	require.NoError(t, store.Open())

	var dirs []*proto.Object
	for d := range 6 {
		var files []*proto.Object

		for i := range 8 {
			blob := proto.NewObject(&proto.Blob{Data: fmt.Appendf(nil, "blob %d/%d", d, i)})
			require.NoError(t, store.Put(ctx, blob))

			file := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Length: uint64(len(blob.GetBlob().GetData())), Ref: blob.Ref()}}})
			require.NoError(t, store.Put(ctx, file))
			files = append(files, file)
		}

		dir := treeOf(files)
		require.NoError(t, store.Put(ctx, dir))
		dirs = append(dirs, dir)
	}

	root := treeOf(dirs)
	require.NoError(t, store.Put(ctx, root))
	require.NoError(t, store.Put(ctx, proto.NewObject(&proto.Commit{Tree: root.Ref(), Timestamp: 1, BackupSet: "world"})))
	require.NoError(t, store.Close())

	const objects = 6*(8+8+1) + 2

	for _, uncached := range [][]*proto.Object{nil, {dirs[0], dirs[3], dirs[5]}} {
		cache, err := pebble.New(t.TempDir())
		require.NoError(t, err)

		leaving := &leavingOutCache{Store: cache, left: map[string]bool{}}
		for _, obj := range uncached {
			leaving.left[string(obj.Ref().Hash)] = true
		}

		store, err := NewPackStorage(WithArchiveStorage(storage), WithArchiveIndex(NewInMemoryIndex()), WithMetadataCache(leaving))
		require.NoError(t, err)
		require.NoError(t, store.Open())

		collect := func() (marked uint64, reads int) {
			before := storage.count()

			report, err := store.Collect(ctx, CollectOptions{NoSweep: true, TempDir: t.TempDir()})
			require.NoError(t, err)

			return report.Marked, storage.count() - before
		}

		marked, _ := collect()
		require.EqualValues(t, objects, marked)

		marked, reads := collect()
		require.EqualValues(t, objects, marked, "a mark through the cache marks the same")

		if len(uncached) == 0 {
			require.Zero(t, reads, "what the cache holds is not read again")
		} else {
			require.Positive(t, reads, "what the cache lacks is read")
			require.LessOrEqual(t, reads, len(uncached), "only what the cache lacks is read")
		}

		require.NoError(t, store.Close())
	}
}

// leavingOutCache is a metadata cache that never holds the objects left.
type leavingOutCache struct {
	*pebble.Store
	left map[string]bool
}

func (c *leavingOutCache) Put(ctx context.Context, obj *proto.Object) error {
	if c.left[string(obj.Ref().Hash)] {
		return nil
	}

	return c.Store.Put(ctx, obj)
}

func TestScanIndexYieldsTheStoredRecords(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()

	options := []PackOption{WithArchiveStorage(newLocal(base)), WithArchiveIndex(NewInMemoryIndex())}

	store, err := NewPackStorage(options...)
	require.NoError(t, err)
	require.NoError(t, store.Open())

	for i := range 20 {
		require.NoError(t, store.Put(ctx, proto.NewObject(&proto.Blob{Data: []byte(fmt.Sprintf("blob %d", i))})))
	}
	require.NoError(t, store.Close())

	store, err = NewPackStorage(options...)
	require.NoError(t, err)
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })

	require.NotEmpty(t, store.archives)
	archive := store.archives[0]

	loaded, err := archive.getIndex()
	require.NoError(t, err)
	require.NotEmpty(t, loaded)

	var streamed IndexFile
	require.NoError(t, scanArchive(archive, func(_ int, rec *IndexRecord) error {
		streamed = append(streamed, *rec)

		return nil
	}, nil, nil))

	require.Equal(t, loaded, streamed, "streaming an index gives what loading it does")
}

func TestReadRecordsReadsNeighboursInOneRangeRead(t *testing.T) {
	ctx := context.Background()
	storage := &countingStorage{localArchiveStorage: newLocal(t.TempDir())}

	store, err := NewPackStorage(WithArchiveStorage(storage), WithArchiveIndex(NewInMemoryIndex()), WithAtRestKey(atRestKey(t)))
	require.NoError(t, err)
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })

	objects := makeTestData(t, 6)
	for _, obj := range objects {
		require.NoError(t, store.Put(ctx, obj))
	}
	require.NoError(t, store.Flush())

	missing := proto.NewObject(&proto.Blob{Data: []byte("never stored")})
	refs := []*proto.Ref{objects[4].Ref(), missing.Ref(), objects[0].Ref(), objects[2].Ref(), objects[4].Ref()}

	before := storage.count()
	read, err := store.ReadRecords(ctx, refs)
	require.NoError(t, err)
	require.Equal(t, 1, storage.count()-before, "records side by side come back in one read")

	require.Nil(t, read[1], "an unknown ref is left to an ordinary read")
	for _, i := range []int{0, 2, 3, 4} {
		require.True(t, read[i].Ref().Equal(refs[i]))

		got, err := store.Get(ctx, refs[i])
		require.NoError(t, err)
		require.True(t, pb.Equal(got, read[i]), fmt.Sprintf("ref %d reads as Get reads it", i))
	}
}
