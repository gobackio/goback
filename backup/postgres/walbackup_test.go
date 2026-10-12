package postgres

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/blobcache"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/index/sql"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage"
	"github.com/gobackio/goback/storage/cache"
	"github.com/gobackio/goback/storage/pack"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"
)

type walFixture struct {
	t     *testing.T
	ctx   context.Context
	index *sql.Index
	spool Spool
	now   time.Time
}

func newWALFixture(t *testing.T) *walFixture {
	t.Helper()

	bucket := memblob.OpenBucket(nil)
	store, err := pack.NewPackStorage(pack.WithArchiveStorage(storage.NewBucketStore(bucket)), pack.WithArchiveIndex(pack.NewInMemoryIndex()))
	require.NoError(t, err)
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })

	index := sql.NewMemory(t.Name(), store)
	require.NoError(t, index.Open())
	t.Cleanup(func() { _ = index.Close() })

	return &walFixture{t: t, ctx: context.Background(), index: index, spool: Spool{Dir: t.TempDir()}, now: time.Now().Add(time.Minute)}
}

// spoolSegment spools segment n of timeline 1 of the cluster systemID.
func (f *walFixture) spoolSegment(n uint64, systemID uint64) string {
	f.t.Helper()

	name := walName(1, n)
	require.NoError(f.t, f.spool.put(name, append(header(1, n*segSize, systemID), byte(n))))

	return name
}

func walName(timeline uint32, n uint64) string {
	perLog := uint64(0x100000000) / segSize
	return fmt.Sprintf("%08X%08X%08X", timeline, n/perLog, n%perLog)
}

// base records a base backup that started in segment n.
func (f *walFixture) base(n uint64) {
	f.t.Helper()

	dir := f.t.TempDir()
	require.NoError(f.t, os.WriteFile(filepath.Join(dir, "base.tar"), []byte("base"), 0o600))

	w := &backup.Walker{Index: f.index, Objects: f.index, Set: "db-base", AgentID: "a", Root: dir, Workers: 1,
		Metadata: map[string]string{MetaStartWALFile: walName(1, n)}}
	_, err := w.Run(f.ctx)
	require.NoError(f.t, err)
}

func (f *walFixture) run() (*backup.WalkResult, error) {
	return f.commit(f.index, nil, nil)
}

// commit commits what the spool holds, and open, as the receiver does.
func (f *walFixture) commit(objects backup.ObjectStore, key *storekey.Key, open *OpenSegment) (*backup.WalkResult, error) {
	f.t.Helper()

	files, err := f.spool.Files()
	require.NoError(f.t, err)

	b := &WALBackup{
		Walker:  &backup.Walker{Index: f.index, Objects: objects, Set: "db-wal", AgentID: "a", Root: f.spool.Dir, Workers: 2, Key: key},
		Spool:   f.spool,
		BaseSet: "db-base",
		Window:  14 * 24 * time.Hour,
		Now:     func() time.Time { return f.now },
		Files:   files,
		Open:    open,
	}

	return b.Run(f.ctx)
}

func (f *walFixture) held(result *backup.WalkResult) []string {
	f.t.Helper()

	tree, err := backup.LoadTree(f.ctx, f.index, result.Commit.Tree)
	require.NoError(f.t, err)

	var names []string
	for _, node := range tree.Nodes {
		names = append(names, string(node.Stat.Name))
	}

	return names
}

func (f *walFixture) spooled() []string {
	f.t.Helper()

	files, err := f.spool.Files()
	require.NoError(f.t, err)

	return files
}

func TestEachWALCommitHoldsEverythingBackToTheOldestBase(t *testing.T) {
	f := newWALFixture(t)

	result, err := f.run()
	require.NoError(t, err)
	require.Nil(t, result, "an empty spool commits nothing")

	one, two := f.spoolSegment(1, 42), f.spoolSegment(2, 42)
	result, err = f.run()
	require.NoError(t, err)
	require.Equal(t, []string{one, two}, f.held(result))
	require.Equal(t, "42", result.Commit.Metadata[MetaSystemID])
	require.Empty(t, f.spooled())

	f.base(2)
	three := f.spoolSegment(3, 42)

	result, err = f.run()
	require.NoError(t, err)
	require.Equal(t, []string{two, three}, f.held(result), "what precedes the oldest base is let go")
	require.Equal(t, two, result.Commit.Metadata[MetaFirstWALFile])
	require.Equal(t, three, result.Commit.Metadata[MetaLastWALFile])
}

func TestWALFromAnotherClusterOrWithAGapStaysInTheSpool(t *testing.T) {
	f := newWALFixture(t)

	f.spoolSegment(1, 42)
	_, err := f.run()
	require.NoError(t, err)

	foreign := f.spoolSegment(2, 7)
	_, err = f.run()
	require.ErrorIs(t, err, ErrForeignSegment)
	require.Equal(t, []string{foreign}, f.spooled())
	require.NoError(t, f.spool.Remove(foreign))

	skipped := f.spoolSegment(3, 42)
	_, err = f.run()
	require.ErrorIs(t, err, ErrGap)
	require.Equal(t, []string{skipped}, f.spooled())
}

// readCounting counts the commits and trees read from the store, and
// streams a tree with its splits in one call as a remote store does.
type readCounting struct {
	*sql.Index
	commits, trees, getTrees atomic.Int64
}

func (c *readCounting) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	obj, err := c.Index.Get(ctx, ref)

	switch obj.Type() {
	case proto.ObjectType_COMMIT:
		c.commits.Add(1)
	case proto.ObjectType_TREE:
		c.trees.Add(1)
	}

	return obj, err
}

func (c *readCounting) GetTree(ctx context.Context, ref *proto.Ref, _ uint32) ([]*proto.Object, error) {
	c.getTrees.Add(1)

	var objects []*proto.Object

	for queue := []*proto.Ref{ref}; len(queue) > 0; queue = queue[1:] {
		obj, err := c.Index.Get(ctx, queue[0])
		if err != nil {
			return nil, err
		}

		objects = append(objects, obj)
		queue = append(queue, obj.GetTree().GetSplits()...)
	}

	return objects, nil
}

func (c *readCounting) reads() int64 {
	return c.commits.Load() + c.trees.Load() + c.getTrees.Load()
}

func (f *walFixture) runOver(objects backup.ObjectStore, key *storekey.Key) (*backup.WalkResult, error) {
	return f.commit(objects, key, nil)
}

// cached is the store a new process sees with the tree cache in dir.
func (f *walFixture) cached(dir string, store backup.ObjectStore) backup.ObjectStore {
	f.t.Helper()

	trees, err := blobcache.Open(dir, "trees", 0)
	require.NoError(f.t, err)

	return cache.New(trees, store)
}

func TestAWALCommitReadsThePreviousCommitAndTreeOnce(t *testing.T) {
	f := newWALFixture(t)

	for n := uint64(1); n <= 600; n++ {
		f.spoolSegment(n, 42)
	}

	_, err := f.run()
	require.NoError(t, err)

	f.spoolSegment(601, 42)

	objects := &readCounting{Index: f.index}
	result, err := f.runOver(objects, nil)
	require.NoError(t, err)
	require.Len(t, f.held(result), 601)
	require.Equal(t, int64(1), objects.commits.Load())
	require.Equal(t, int64(1), objects.getTrees.Load())
	require.Zero(t, objects.trees.Load(), "the previous tree's splits were read one by one")
}

func TestSteadyWALCommitsReadNoCommitOrTreeFromTheStore(t *testing.T) {
	f := newWALFixture(t)
	dir := t.TempDir()

	for n := uint64(1); n <= 600; n++ {
		f.spoolSegment(n, 42)
	}

	_, err := f.run()
	require.NoError(t, err)

	for n := uint64(601); n <= 603; n++ {
		f.spoolSegment(n, 42)

		objects := &readCounting{Index: f.index}
		result, err := f.runOver(f.cached(dir, objects), nil)
		require.NoError(t, err)
		require.Len(t, f.held(result), int(n))

		if n == 601 {
			require.Equal(t, int64(2), objects.reads(), "a cold cache reads the commit and its tree once")
			continue
		}

		require.Zero(t, objects.reads(), "commit %d read what the last one wrote", n)
	}
}

func TestAWALCommitOverAnotherAgentsCommitReadsItFromTheStore(t *testing.T) {
	f := newWALFixture(t)
	dir := t.TempDir()

	one := f.spoolSegment(1, 42)
	_, err := f.runOver(f.cached(dir, f.index), nil)
	require.NoError(t, err)

	two := f.spoolSegment(2, 42)
	_, err = f.run()
	require.NoError(t, err)

	three := f.spoolSegment(3, 42)
	objects := &readCounting{Index: f.index}
	result, err := f.runOver(f.cached(dir, objects), nil)
	require.NoError(t, err)
	require.Equal(t, []string{one, two, three}, f.held(result))
	require.Equal(t, int64(1), objects.commits.Load(), "the newest commit is the store's to say")
	require.Equal(t, int64(1), objects.getTrees.Load())
}

func TestASealedWALSetsTreeCacheHoldsNoNames(t *testing.T) {
	f := newWALFixture(t)
	dir := t.TempDir()

	key, err := storekey.Generate("s1")
	require.NoError(t, err)

	f.spoolSegment(1, 42)
	middle := f.spoolSegment(2, 42)
	f.spoolSegment(3, 42)

	_, err = f.runOver(f.cached(dir, f.index), key)
	require.NoError(t, err)

	f.spoolSegment(4, 42)
	_, err = f.runOver(f.cached(dir, f.index), key)
	require.NoError(t, err)

	var files int
	err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}

		data, err := os.ReadFile(path)
		require.NoError(t, err)
		require.NotContains(t, string(data), middle, "%s holds a name in the clear", path)
		files++

		return nil
	})
	require.NoError(t, err)
	require.NotZero(t, files)
}

// receiving writes the first size bytes of segment n of timeline into the
// spool's receiving directory, as the receiver does.
func (f *walFixture) receiving(timeline uint32, n uint64, size int) *OpenSegment {
	f.t.Helper()

	name := walName(timeline, n)
	dir := filepath.Join(f.spool.Dir, receivingDir)
	require.NoError(f.t, os.MkdirAll(dir, 0o700))

	path := filepath.Join(dir, name)
	require.NoError(f.t, os.WriteFile(path, segmentContent(timeline, n, size), 0o600))

	return &OpenSegment{Name: name, Path: path, Size: int64(size)}
}

// segmentContent is the first size bytes of segment n of timeline.
func segmentContent(timeline uint32, n uint64, size int) []byte {
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i * 7)
	}

	copy(content, header(timeline, n*segSize, 42))

	return content
}

// put spools the first size bytes of segment n of timeline under name.
func (f *walFixture) put(name string, timeline uint32, n uint64, size int) {
	f.t.Helper()

	require.NoError(f.t, f.spool.put(name, segmentContent(timeline, n, size)))
}

func (f *walFixture) receive(open *OpenSegment) (*backup.WalkResult, error) {
	return f.commit(f.index, nil, open)
}

func (f *walFixture) sizes(result *backup.WalkResult) map[string]int64 {
	f.t.Helper()

	tree, err := backup.LoadTree(f.ctx, f.index, result.Commit.Tree)
	require.NoError(f.t, err)

	sizes := map[string]int64{}
	for _, node := range tree.Nodes {
		sizes[string(node.Stat.Name)] = node.Stat.GetSize()
	}

	return sizes
}

func TestTheSegmentBeingReceivedIsCommittedAsPartialUntilItIsWhole(t *testing.T) {
	f := newWALFixture(t)

	one := f.spoolSegment(1, 42)

	result, err := f.receive(f.receiving(1, 2, 1000))
	require.NoError(t, err)
	require.Equal(t, map[string]int64{one: 41, walName(1, 2) + partialSuffix: 1000}, f.sizes(result))
	require.Equal(t, map[string]string{
		MetaFirstWALFile: one,
		MetaLastWALFile:  one,
		MetaSystemID:     "42",
		MetaTimeline:     "1",
		MetaEndLSN:       FormatLSN(2*segSize + 1000),
		MetaCapture:      CaptureReplication,
	}, result.Commit.Metadata)

	result, err = f.receive(f.receiving(1, 2, 300<<10))
	require.NoError(t, err)
	require.Equal(t, map[string]int64{one: 41, walName(1, 2) + partialSuffix: 300 << 10}, f.sizes(result))

	f.put(walName(1, 2), 1, 2, 400<<10)

	result, err = f.receive(f.receiving(1, 3, 100))
	require.NoError(t, err)
	require.Equal(t, map[string]int64{one: 41, walName(1, 2): 400 << 10, walName(1, 3) + partialSuffix: 100}, f.sizes(result), "a whole segment replaces its partial one")
	require.Equal(t, walName(1, 2), result.Commit.Metadata[MetaLastWALFile])
	require.Equal(t, FormatLSN(3*segSize+100), result.Commit.Metadata[MetaEndLSN])

	dst := filepath.Join(t.TempDir(), "RECOVERYXLOG")
	require.NoError(t, FetchWAL(f.ctx, f.index, nil, result.Ref, walName(1, 2), dst))

	got, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Equal(t, segmentContent(1, 2, 400<<10), got, "the whole segment, from the parts it shares with its partial versions")
}

func TestAPartialSegmentThatSkipsOneIsRefused(t *testing.T) {
	f := newWALFixture(t)

	f.spoolSegment(1, 42)

	_, err := f.receive(f.receiving(1, 3, 1000))
	require.ErrorIs(t, err, ErrGap)
}

func TestAnOlderTimelinesPartialSegmentStaysBesideTheNewOnes(t *testing.T) {
	f := newWALFixture(t)

	one := f.spoolSegment(1, 42)

	_, err := f.receive(f.receiving(1, 2, 1000))
	require.NoError(t, err)

	f.put(walName(1, 2)+partialSuffix, 1, 2, 2000)
	require.NoError(t, f.spool.put("00000002.history", []byte("1\t0/2000800\tno recovery target specified\n")))

	result, err := f.receive(f.receiving(2, 2, 3000))
	require.NoError(t, err)
	require.Equal(t, map[string]int64{
		one:                           41,
		"00000002.history":            41,
		walName(1, 2) + partialSuffix: 2000,
		walName(2, 2) + partialSuffix: 3000,
	}, f.sizes(result))
	require.Equal(t, "2", result.Commit.Metadata[MetaTimeline])
	require.Equal(t, FormatLSN(2*segSize+3000), result.Commit.Metadata[MetaEndLSN])

	dst := filepath.Join(t.TempDir(), "RECOVERYXLOG")
	require.NoError(t, FetchWAL(f.ctx, f.index, nil, result.Ref, walName(2, 2), dst))

	got, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Len(t, got, segSize, "padded to the size its header records")
	require.Equal(t, segmentContent(2, 2, 3000), got[:3000])
	require.Equal(t, make([]byte, segSize-3000), got[3000:])

	require.ErrorIs(t, FetchWAL(f.ctx, f.index, nil, result.Ref, walName(1, 2), dst), backup.ErrNotFound, "never an older timeline's partial segment")
}
