package pack

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"io/fs"
	"path"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/badger"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// requestCounting counts what a bucket under the storage would be sent:
// an Open is an attributes request, the first read of an opened file and
// every ranged read a get, a closed writer a put.
type requestCounting struct {
	*memView

	// reports has a closed writer tell what it stored, as Cloud Storage
	// answers an upload
	reports bool

	mtx    sync.Mutex
	counts map[string]int
}

func newRequestCounting(bucket *memBucket) *requestCounting {
	return &requestCounting{memView: bucket.view(), counts: map[string]int{}}
}

func kindOf(name string) string {
	switch ext := path.Ext(name); ext {
	case ".goback":
		return "archive"
	case "":
		return "all"
	default:
		return ext
	}
}

func (c *requestCounting) count(op, name string) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	c.counts[op+" "+kindOf(name)]++
}

// take returns the counts so far and starts over.
func (c *requestCounting) take() map[string]int {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	counts := c.counts
	c.counts = map[string]int{}

	return counts
}

func (c *requestCounting) Create(name string) (File, error) {
	file, err := c.memView.Create(name)
	if err != nil {
		return nil, err
	}

	return &countedFile{File: file, storage: c, name: name, writer: true}, nil
}

func (c *requestCounting) CreateNew(name string, data []byte) error {
	c.count("put", name)

	return c.memView.CreateNew(name, data)
}

func (c *requestCounting) Open(name string) (File, error) {
	file, err := c.memView.Open(name)

	// a file still being written is answered without asking the bucket
	if _, writing := file.(*memWriter); writing {
		return file, nil
	}

	c.count("head", name)

	if err != nil {
		return nil, err
	}

	return &countedFile{File: file, storage: c, name: name}, nil
}

func (c *requestCounting) OpenListed(listed ListedFile) (File, error) {
	file, err := c.memView.Open(listed.Name)
	if err != nil {
		return nil, err
	}

	return &countedFile{File: file, storage: c, name: listed.Name}, nil
}

func (c *requestCounting) List(extension string) ([]string, error) {
	c.count("list", extension)

	return c.memView.List(extension)
}

func (c *requestCounting) ListInfo(extension string) ([]ListedFile, error) {
	c.count("list", extension)

	return c.memView.ListInfo(extension)
}

func (c *requestCounting) Delete(name string) error {
	c.count("delete", name)

	return c.memView.Delete(name)
}

func (c *requestCounting) Checksum(name string) ([]byte, error) {
	c.count("head", name)

	file, err := c.memView.Open(name)
	if err != nil {
		return nil, err
	}

	sum := md5.New()
	_, err = io.Copy(sum, file)

	return sum.Sum(nil), err
}

type countedFile struct {
	File
	storage   *requestCounting
	name      string
	writer    bool
	closed    bool
	streaming bool
}

func (f *countedFile) Read(p []byte) (int, error) {
	if !f.streaming {
		f.streaming = true
		f.storage.count("get", f.name)
	}

	return f.File.Read(p)
}

func (f *countedFile) Seek(offset int64, whence int) (int64, error) {
	f.streaming = false

	return f.File.Seek(offset, whence)
}

func (f *countedFile) ReadAt(p []byte, off int64) (int, error) {
	f.storage.count("get", f.name)

	return f.File.(io.ReaderAt).ReadAt(p, off)
}

func (f *countedFile) Close() error {
	if f.writer {
		f.storage.count("put", f.name)
		f.closed = true
	}

	return f.File.Close()
}

func (f *countedFile) Stored() (fs.FileInfo, []byte, bool) {
	if !f.writer || !f.closed || !f.storage.reports {
		return nil, nil, false
	}

	file, err := f.storage.memBucket.Open(f.name)
	if err != nil {
		return nil, nil, false
	}

	info, _ := file.Stat()
	sum := md5.New()
	_, _ = io.Copy(sum, file)

	return info, sum.Sum(nil), true
}

func formatCounts(counts map[string]int) string {
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	var out strings.Builder
	for _, key := range keys {
		fmt.Fprintf(&out, "%s=%d ", key, counts[key])
	}

	return out.String()
}

// TestACommitAsksTheBucketForLittleBeyondWhatItWrites pins the requests a
// small commit that deduplicates against the one before it costs a store
// with a metadata and an index cache, as a server runs it.
func TestACommitAsksTheBucketForLittleBeyondWhatItWrites(t *testing.T) {
	for _, reports := range []bool{false, true} {
		t.Run(fmt.Sprintf("reports=%v", reports), func(t *testing.T) {
			bucket := newMemBucket()
			storage := newRequestCounting(bucket)
			storage.reports = reports

			cache, err := badger.New(t.TempDir())
			require.NoError(t, err)

			index := NewInMemoryIndex()
			store, err := NewPackStorage(
				WithArchiveStorage(storage),
				WithArchiveIndex(index),
				WithMetadataCache(cache),
				WithIndexCache(t.TempDir()),
				WithCloseBeforeRead(true),
			)
			require.NoError(t, err)
			require.NoError(t, store.Open())
			t.Cleanup(func() { _ = store.Close() })

			previous := makeChain(makeTestData(t, 10))
			commit := func(objects []*proto.Object) {
				ctx, err := store.BeginSession(context.Background(), &backup.Session{AgentID: "agent", Set: "world"})
				require.NoError(t, err)

				for _, obj := range objects {
					require.NoError(t, store.Put(ctx, obj))
				}
			}

			commit(previous)
			storage.take()

			// one new file beside the trees the previous commit stored
			blob := makeTestData(t, 1)
			file := makeGCFiles(blob)
			root := treeOf(append(file, previous[len(previous)-3]))
			commit(append(append(blob, file...), root, proto.NewObject(&proto.Commit{Tree: root.Ref(), Timestamp: 2, BackupSet: "world"})))

			// the session's archive, and one with the commit's un-tombstone
			most := map[string]int{
				"put archive": 2, "put .idx": 2, "put .begin": 1, "put .end": 1,
				// the seals at begin and at commit
				"list .seal": 2,
			}

			if !reports {
				// what an upload kept, and when the storage stamped each index
				most["head archive"], most["head .idx"] = 2, 2
			}

			counts := storage.take()
			for request, n := range counts {
				require.LessOrEqualf(t, n, most[request], "%s in %s", request, formatCounts(counts))
			}

			for name, obj := range bucket.files {
				if archive, ok := strings.CutSuffix(name, IndexExt); ok {
					info, known, err := index.LookupArchive(archive)
					require.NoError(t, err)
					require.True(t, known)
					require.True(t, obj.created.Equal(info.Created), "an archive is versioned by when the bucket stored its index")
				}
			}
		})
	}
}

// TestAColdOpenListsTheMarkResultsOnce pins that opening a store reads the
// mark result of each archive that has one and asks nothing of the others.
func TestAColdOpenListsTheMarkResultsOnce(t *testing.T) {
	ctx := context.Background()
	bucket := newMemBucket()

	store, err := NewPackStorage(WithArchiveStorage(bucket.view()), WithArchiveIndex(NewInMemoryIndex()))
	require.NoError(t, err)
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })

	chain := makeChain(makeTestData(t, 20))
	for i := 0; i < len(chain); i += 5 {
		for _, obj := range chain[i:min(i+5, len(chain))] {
			require.NoError(t, store.Put(ctx, obj))
		}

		require.NoError(t, store.Flush())
	}

	_, err = store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	for _, obj := range makeTestData(t, 3) {
		require.NoError(t, store.Put(ctx, obj))
		require.NoError(t, store.Flush())
	}

	marked := 0
	for name := range bucket.files {
		if strings.HasSuffix(name, GCExt) {
			marked++
		}
	}

	storage := newRequestCounting(bucket)
	other, err := NewPackStorage(WithArchiveStorage(storage), WithArchiveIndex(NewInMemoryIndex()))
	require.NoError(t, err)
	require.NoError(t, other.Open())
	t.Cleanup(func() { _ = other.Close() })

	counts := storage.take()
	require.Zero(t, counts["head .gc"], formatCounts(counts))
	require.Equal(t, 1, counts["list .gc"], formatCounts(counts))
	require.Equal(t, marked, counts["get .gc"], formatCounts(counts))

	require.Greater(t, len(other.archives), marked, "some archives were never marked")

	for _, a := range other.archives {
		_, stored := bucket.files[a.name+GCExt]
		require.Equal(t, stored, a.gcResult() != nil, a.name)
	}
}

// TestARewriteRetiresItsInputsWithOneWrite pins that the archives one
// rewrite retires cost one write between them, however many there are.
func TestARewriteRetiresItsInputsWithOneWrite(t *testing.T) {
	bucket := newMemBucket()
	storage := newRequestCounting(bucket)

	store, err := NewPackStorage(
		WithArchiveStorage(storage),
		WithArchiveIndex(NewInMemoryIndex()),
		WithCompaction(CompactionConfig{MinimumCandidates: 1}),
	)
	require.NoError(t, err)
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })

	objects := makeTestData(t, 40)
	for i := 0; i < len(objects); i += 10 {
		for _, object := range objects[i : i+10] {
			require.NoError(t, store.Put(context.Background(), object))
		}

		require.NoError(t, store.Flush())
	}

	storage.take()

	report := compact(t, context.Background(), store)
	require.EqualValues(t, 4, report.Rewritten)

	counts := storage.take()
	require.Equal(t, 1, counts["put "+RetiredExt]+counts["put "+RetirementExt], formatCounts(counts))

	other, err := NewPackStorage(WithArchiveStorage(bucket.view()), WithArchiveIndex(NewInMemoryIndex()))
	require.NoError(t, err)
	require.NoError(t, other.Open())
	t.Cleanup(func() { _ = other.Close() })

	names, _, err := other.archiveNames()
	require.NoError(t, err)
	require.Len(t, names, 1, "another process loads only the rewrite's output")

	for _, object := range objects {
		got, err := other.Get(context.Background(), object.Ref())
		require.NoError(t, err)
		require.Equal(t, plain(object), plain(got))
	}
}

// signedCounting is a counting storage that also signs ranges, as the
// bucket under a server does; signing asks the bucket nothing.
type signedCounting struct{ *requestCounting }

func (signedCounting) SignRange(_ context.Context, name string, offset, length int64, ttl time.Duration) (*proto.Location, error) {
	return &proto.Location{Url: name, Length: length, Expires: timestamppb.New(time.Now().Add(ttl))}, nil
}

// TestAServerServesWhatItCachesAndLocatesTheRest pins that a store with a
// metadata cache, as a server runs it, answers commits, trees and files
// without asking the bucket and hands out only parts as locations.
func TestAServerServesWhatItCachesAndLocatesTheRest(t *testing.T) {
	ctx := context.Background()
	storage := newRequestCounting(newMemBucket())

	cache, err := badger.New(t.TempDir())
	require.NoError(t, err)

	store, err := NewPackStorage(
		WithArchiveStorage(signedCounting{storage}),
		WithArchiveIndex(NewInMemoryIndex()),
		WithMetadataCache(cache),
		WithIndexCache(t.TempDir()),
		WithCloseBeforeRead(true),
	)
	require.NoError(t, err)
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })

	session, err := store.BeginSession(ctx, &backup.Session{AgentID: "agent", Set: "world"})
	require.NoError(t, err)

	var metadata, blobs []*proto.Ref
	for _, obj := range makeChain(makeTestData(t, 10)) {
		require.NoError(t, store.Put(session, obj))

		if obj.Type().Metadata() {
			metadata = append(metadata, obj.Ref())
		} else {
			blobs = append(blobs, obj.Ref())
		}
	}

	require.NoError(t, store.Flush())
	storage.take()

	for _, ref := range metadata {
		_, err := store.Get(ctx, ref)
		require.NoError(t, err)
	}

	read, err := store.ReadRecords(ctx, metadata)
	require.NoError(t, err)
	require.NotContains(t, read, (*proto.Object)(nil))

	runs, err := store.LocateRecords(ctx, metadata)
	require.NoError(t, err)
	require.Empty(t, runs, "metadata is served, never located")
	require.Empty(t, storage.take(), "cached metadata asks the bucket nothing")

	require.NoError(t, cache.Delete(ctx, metadata[0]))
	_, err = store.Get(ctx, metadata[0])
	require.NoError(t, err)
	require.NotEmpty(t, storage.take(), "uncached metadata is read through")
	_, err = store.Get(ctx, metadata[0])
	require.NoError(t, err)
	require.Empty(t, storage.take(), "into the cache")

	located := func() int {
		runs, err := store.LocateRecords(ctx, blobs)
		require.NoError(t, err)

		n := 0
		for _, run := range runs {
			n += len(run.GetRecords())
		}

		return n
	}

	require.Equal(t, len(blobs), located(), "parts go out as locations")
	require.Empty(t, storage.take(), "locating parts asks the bucket nothing")

	held, err := store.Get(ctx, blobs[0])
	require.NoError(t, err)
	require.NoError(t, cache.Put(ctx, held))
	require.Equal(t, len(blobs)-1, located(), "a part the cache holds is served")
}

// TestAServerReadsWhatItsBoundedCacheLetGoFromTheBucket pins that metadata a
// full cache evicted is still served, read through from the bucket.
func TestAServerReadsWhatItsBoundedCacheLetGoFromTheBucket(t *testing.T) {
	ctx := context.Background()
	storage := newRequestCounting(newMemBucket())

	chain := makeChain(makeTestData(t, 10))

	var metadataBytes int
	for _, obj := range chain {
		if obj.Type().Metadata() {
			metadataBytes += len(obj.Bytes())
		}
	}

	cache, err := badger.New(t.TempDir(), badger.WithCapacity(int64(metadataBytes/2)))
	require.NoError(t, err)

	store, err := NewPackStorage(
		WithArchiveStorage(signedCounting{storage}),
		WithArchiveIndex(NewInMemoryIndex()),
		WithMetadataCache(cache),
		WithCloseBeforeRead(true),
	)
	require.NoError(t, err)
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })

	session, err := store.BeginSession(ctx, &backup.Session{AgentID: "agent", Set: "world"})
	require.NoError(t, err)

	var metadata []*proto.Object
	for _, obj := range chain {
		require.NoError(t, store.Put(session, obj))

		if obj.Type().Metadata() {
			metadata = append(metadata, obj)
		}
	}

	require.NoError(t, store.Flush())
	storage.take()

	newest, oldest := metadata[len(metadata)-1], metadata[0]

	_, err = store.Get(ctx, newest.Ref())
	require.NoError(t, err)
	require.Empty(t, storage.take(), "the newest is still cached")

	held, err := cache.Has(ctx, oldest.Ref())
	require.NoError(t, err)
	require.False(t, held, "the oldest was evicted")

	obj, err := store.Get(ctx, oldest.Ref())
	require.NoError(t, err)
	require.Equal(t, plain(oldest), plain(obj))
	require.NotEmpty(t, storage.take(), "an evicted object is read from the bucket")
}

// putFlushed stores objects in archives of per objects each.
func putFlushed(t *testing.T, store *PackStorage, objects []*proto.Object, per int) {
	for i := 0; i < len(objects); i += per {
		for _, obj := range objects[i:min(i+per, len(objects))] {
			require.NoError(t, store.Put(context.Background(), obj))
		}

		require.NoError(t, store.Flush())
	}
}

// TestOpeningAStoreReadsNoArchiveItsIndexKnows pins that opening a store,
// or catching up with archives another process stored, reads no bytes of
// an archive the index knows until one of its objects is read.
func TestOpeningAStoreReadsNoArchiveItsIndexKnows(t *testing.T) {
	ctx := context.Background()
	bucket := newMemBucket()
	index := NewInMemoryIndex()

	writer, err := NewPackStorage(WithArchiveStorage(bucket.view()), WithArchiveIndex(index))
	require.NoError(t, err)
	require.NoError(t, writer.Open())
	t.Cleanup(func() { _ = writer.Close() })

	objects := makeTestData(t, 20)
	putFlushed(t, writer, objects[:15], 5)

	storage := newRequestCounting(bucket)
	store, err := NewPackStorage(WithArchiveStorage(storage), WithArchiveIndex(index))
	require.NoError(t, err)
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })

	require.Len(t, store.archives, 3)

	putFlushed(t, writer, objects[15:], 5)
	require.NoError(t, store.refreshArchives())
	require.Len(t, store.archives, 4)

	counts := storage.take()
	require.Zero(t, counts["get archive"], formatCounts(counts))

	for _, obj := range objects {
		got, err := store.Get(ctx, obj.Ref())
		require.NoError(t, err)
		require.Equal(t, plain(obj), plain(got))
	}

	counts = storage.take()
	require.Equal(t, len(store.archives)+len(objects), counts["get archive"], "a header per archive and a record per object: %s", formatCounts(counts))
}

// TestAForeignArchiveIsRefusedBeforeItsRecordsAreTrusted pins that an
// archive whose header is not goback's fails the first read of it when the
// index knows it, and opening the store when the index does not.
func TestAForeignArchiveIsRefusedBeforeItsRecordsAreTrusted(t *testing.T) {
	ctx := context.Background()
	bucket := newMemBucket()
	index := NewInMemoryIndex()

	writer, err := NewPackStorage(WithArchiveStorage(bucket.view()), WithArchiveIndex(index))
	require.NoError(t, err)
	require.NoError(t, writer.Open())

	obj := makeTestData(t, 1)[0]
	putFlushed(t, writer, []*proto.Object{obj}, 1)
	require.NoError(t, writer.Close())

	bucket.mtx.Lock()
	for name, stored := range bucket.files {
		if strings.HasSuffix(name, ArchiveSuffix) {
			data := bytes.Clone(stored.data)
			copy(data, "NOTGOBACK!")
			bucket.files[name] = memObject{data: data, created: stored.created}
		}
	}
	bucket.mtx.Unlock()

	known, err := NewPackStorage(WithArchiveStorage(bucket.view()), WithArchiveIndex(index))
	require.NoError(t, err)
	require.NoError(t, known.Open())
	t.Cleanup(func() { _ = known.Close() })

	_, err = known.Get(ctx, obj.Ref())
	require.ErrorContains(t, err, "not a goback archive")

	unknown, err := NewPackStorage(WithArchiveStorage(bucket.view()), WithArchiveIndex(NewInMemoryIndex()))
	require.NoError(t, err)
	require.ErrorContains(t, unknown.Open(), "not a goback archive")
}
