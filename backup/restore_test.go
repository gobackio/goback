package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobackio/goback/backup/blobcache"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

type getCountingStore struct {
	*memStore
	gets int64
}

func (g *getCountingStore) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	atomic.AddInt64(&g.gets, 1)
	return g.memStore.Get(ctx, ref)
}

func (g *getCountingStore) reset() int64 {
	return atomic.SwapInt64(&g.gets, 0)
}

func randomData(size int, seed int64) []byte {
	data := make([]byte, size)
	rand.New(rand.NewSource(seed)).Read(data)

	return data
}

func putFile(t *testing.T, store ObjectStore, key *storekey.Key, data []byte) *proto.Ref {
	t.Helper()

	writer := newFileWriter(context.Background(), store, key, int64(len(data)))
	_, err := writer.Write(data)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	return writer.Ref()
}

func fileParts(t *testing.T, store ObjectStore, ref *proto.Ref) int {
	t.Helper()

	obj, err := store.Get(context.Background(), ref)
	require.NoError(t, err)

	return len(obj.GetFile().GetParts())
}

func statFor(data []byte) *proto.FileInfo {
	return &proto.FileInfo{Size: int64(len(data)), Mode: 0o644, MtimeNs: time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC).UnixNano()}
}

func requireRestored(t *testing.T, path string, data []byte, stat *proto.FileInfo) {
	t.Helper()

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.True(t, bytes.Equal(data, got), "content differs")

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, stat.MtimeNs, info.ModTime().UnixNano())
}

func keyCases(t *testing.T) map[string]*storekey.Key {
	return map[string]*storekey.Key{"plaintext": nil, "keyed": newKey(t)}
}

func TestRestoreFileReplacesAReadOnlyDestination(t *testing.T) {
	store := newMemStore()
	first, second := randomData(2*maxBlobSize+5, 1), randomData(2*maxBlobSize+9, 2)
	firstRef, secondRef := putFile(t, store, nil, first), putFile(t, store, nil, second)
	firstStat, secondStat := statFor(first), statFor(second)
	firstStat.Mode, secondStat.Mode = 0o444, 0o444

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "locked.dat")
	r := &Restorer{Store: store, Workers: 2}

	_, err := r.RestoreFile(ctx, path, firstStat, firstRef)
	require.NoError(t, err)

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Zero(t, info.Mode()&0o200, "the recorded mode leaves the file read-only")

	outcome, err := r.RestoreFile(ctx, path, secondStat, secondRef)
	require.NoError(t, err)
	require.Equal(t, OutcomeWritten, outcome)
	requireRestored(t, path, second, secondStat)

	info, err = os.Stat(path)
	require.NoError(t, err)
	require.Zero(t, info.Mode()&0o200)
}

func TestRestoreFileReportsEachPartAsItLands(t *testing.T) {
	store := newMemStore()
	data := randomData(3*maxBlobSize+7, 1)
	ref := putFile(t, store, nil, data)

	var calls, written atomic.Int64
	ctx := WithWritten(context.Background(), func(n int64) {
		calls.Add(1)
		written.Add(n)
	})

	r := &Restorer{Store: store, Workers: 2}
	_, err := r.RestoreFile(ctx, filepath.Join(t.TempDir(), "region.mca"), statFor(data), ref)
	require.NoError(t, err)
	require.Equal(t, int64(fileParts(t, store, ref)), calls.Load(), "once per part, not once per file")
	require.Equal(t, int64(len(data)), written.Load())
}

func TestRestoreFileRejectsAPartThatDoesNotHashToItsRef(t *testing.T) {
	store := newMemStore()
	data := randomData(3*maxBlobSize, 1)
	ref := putFile(t, store, nil, data)

	file, err := store.Get(context.Background(), ref)
	require.NoError(t, err)
	part := file.GetFile().GetParts()[1]

	// a blob of the right length under the wrong ref
	forged := randomData(int(part.Length), 2)
	store.objects[string(part.Ref.Hash)] = proto.NewObject(&proto.Blob{Data: forged})

	path := filepath.Join(t.TempDir(), "region.mca")
	r := &Restorer{Store: store, Workers: 2}
	_, err = r.RestoreFile(context.Background(), path, statFor(data), ref)
	require.ErrorContains(t, err, "does not hash to its ref")
	require.NoFileExists(t, path)
}

func TestRestoreFileTakesPartsFromDestination(t *testing.T) {
	for name, key := range keyCases(t) {
		t.Run(name, func(t *testing.T) {
			store := &getCountingStore{memStore: newMemStore()}
			data := randomData(6*maxBlobSize+777, 1)
			ref := putFile(t, store, key, data)
			parts := fileParts(t, store, ref)
			require.Greater(t, parts, 3)
			stat := statFor(data)

			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "world", "region.mca")
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))

			r := &Restorer{Store: store, Key: key, Workers: 4}

			store.reset()
			outcome, err := r.RestoreFile(ctx, path, stat, ref)
			require.NoError(t, err)
			require.Equal(t, OutcomeWritten, outcome)
			requireRestored(t, path, data, stat)
			require.EqualValues(t, parts+1, store.reset(), "the file object and every part are fetched")
			require.EqualValues(t, len(data), r.Stats().BytesFromStore)

			outcome, err = r.RestoreFile(ctx, path, stat, ref)
			require.NoError(t, err)
			require.Equal(t, OutcomeUnchanged, outcome)
			require.EqualValues(t, 1, store.reset(), "an identical file costs only the file object")

			damaged := append([]byte(nil), data...)
			damaged[len(damaged)/2] ^= 0xff
			require.NoError(t, os.WriteFile(path, damaged, 0o644))

			outcome, err = r.RestoreFile(ctx, path, stat, ref)
			require.NoError(t, err)
			require.Equal(t, OutcomeWritten, outcome)
			requireRestored(t, path, data, stat)
			require.LessOrEqual(t, store.reset(), int64(3), "only the damaged part is downloaded")

			stats := r.Stats()
			require.EqualValues(t, 3, stats.Files)
			require.Greater(t, stats.BytesFromDestination, stats.BytesFromStore)
		})
	}
}

func TestRestoreFileOverwriteModes(t *testing.T) {
	store := &getCountingStore{memStore: newMemStore()}
	data := randomData(3*maxBlobSize, 2)
	ref := putFile(t, store, nil, data)
	stat := statFor(data)
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "level.dat")

	damaged := append([]byte(nil), data...)
	damaged[10] ^= 0xff
	require.NoError(t, os.WriteFile(path, damaged, 0o644))
	require.NoError(t, setMeta(ctx, LocalFS{}, path, fileMode(stat), mtimeOf(stat)))

	trusting := &Restorer{Store: store, Overwrite: OverwriteIfChanged}
	outcome, err := trusting.RestoreFile(ctx, path, stat, ref)
	require.NoError(t, err)
	require.Equal(t, OutcomeSkipped, outcome, "a stat match is trusted")

	dry := &Restorer{Store: store, DryRun: true}
	outcome, err = dry.RestoreFile(ctx, path, stat, ref)
	require.NoError(t, err)
	require.Equal(t, OutcomeWouldWrite, outcome)
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.True(t, bytes.Equal(damaged, got), "a dry run writes nothing")

	entries, err := os.ReadDir(filepath.Dir(path))
	require.NoError(t, err)
	require.Len(t, entries, 1, "no temp file is left behind")

	verifying := &Restorer{Store: store, Verify: true}
	outcome, err = verifying.RestoreFile(ctx, path, stat, ref)
	require.NoError(t, err)
	require.Equal(t, OutcomeWritten, outcome)
	requireRestored(t, path, data, stat)
}

func TestRestoreFileInline(t *testing.T) {
	for name, key := range keyCases(t) {
		t.Run(name, func(t *testing.T) {
			store := newMemStore()
			data := []byte("server-port=25565\nmotd=hello\n")
			ref := putFile(t, store, key, data)
			stat := statFor(data)
			path := filepath.Join(t.TempDir(), "server.properties")

			r := &Restorer{Store: store, Key: key}
			outcome, err := r.RestoreFile(context.Background(), path, stat, ref)
			require.NoError(t, err)
			require.Equal(t, OutcomeWritten, outcome)
			requireRestored(t, path, data, stat)

			outcome, err = r.RestoreFile(context.Background(), path, stat, ref)
			require.NoError(t, err)
			require.Equal(t, OutcomeUnchanged, outcome)
		})
	}
}

// changingFS is the local disk whose files read back other bytes after
// their first reads, as a file another process rewrites does.
type changingFS struct {
	LocalFS
	honest int
}

func (c changingFS) Open(ctx context.Context, path string) (File, error) {
	f, err := c.LocalFS.Open(ctx, path)
	if err != nil {
		return nil, err
	}

	return &changingFile{File: f.(*os.File), honest: c.honest}, nil
}

type changingFile struct {
	*os.File
	honest, reads int
}

func (f *changingFile) ReadAt(b []byte, off int64) (int, error) {
	n, err := f.File.ReadAt(b, off)

	if f.reads++; f.reads > f.honest {
		for i := range b[:n] {
			b[i] ^= 0xff
		}
	}

	return n, err
}

func TestADestinationPartThatChangesAfterItIsHashedIsNotCopied(t *testing.T) {
	store := newMemStore()
	data := randomData(3*maxBlobSize+7, 1)
	ref := putFile(t, store, nil, data)
	stat := statFor(data)

	path := filepath.Join(t.TempDir(), "db")
	require.NoError(t, os.WriteFile(path, append(bytes.Clone(data), "tail"...), 0o644))

	r := &Restorer{Store: store, FS: changingFS{honest: fileParts(t, store, ref)}}
	_, err := r.RestoreFile(context.Background(), path, stat, ref)
	require.NoError(t, err)
	requireRestored(t, path, data, stat)
}

func TestAFileWhosePartsDoNotAddUpIsRefused(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()

	blob := proto.NewObject(&proto.Blob{Data: []byte("x")})
	require.NoError(t, store.Put(ctx, blob))

	for name, parts := range map[string][]*proto.FilePart{
		"overflowing": {{Offset: 0, Length: math.MaxUint64, Ref: blob.Ref()}},
		"huge":        {{Offset: 0, Length: 1, Ref: blob.Ref()}, {Offset: 1, Length: math.MaxInt64, Ref: blob.Ref()}},
	} {
		file := proto.NewObject(&proto.File{Parts: parts})
		require.NoError(t, store.Put(ctx, file))

		path := filepath.Join(t.TempDir(), "f")
		require.NoError(t, os.WriteFile(path, []byte("xx"), 0o644))

		_, err := (&Restorer{Store: store}).RestoreFile(ctx, path, statFor([]byte("x")), file.Ref())
		require.Error(t, err, name)

		var failed error
		err = (&Restorer{Store: filesStore{store}}).RestoreFiles(ctx, []FileToRestore{{Path: path, Stat: statFor([]byte("x")), Ref: file.Ref()}}, func(_ int, _ Outcome, err error) error {
			failed = err
			return nil
		})
		require.NoError(t, err, name)
		require.Error(t, failed, name)
	}
}

func TestRestoreFileLeavesACloudOnlyFilesDestinationAlone(t *testing.T) {
	store := newMemStore()
	empty := putFile(t, store, nil, nil)

	path := filepath.Join(t.TempDir(), "report.docx")
	require.NoError(t, os.WriteFile(path, []byte("the local content"), 0o644))

	stat := statFor(make([]byte, 25))
	stat.Attributes = recallAttributes

	r := &Restorer{Store: store}
	outcome, err := r.RestoreFile(context.Background(), path, stat, empty)
	require.NoError(t, err)
	require.Equal(t, OutcomeCloudOnly, outcome)
	require.EqualValues(t, 1, r.Stats().CloudOnly)

	content, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "the local content", string(content))
}

func TestRestoreFileFromSeeds(t *testing.T) {
	for name, key := range keyCases(t) {
		t.Run(name, func(t *testing.T) {
			store := &getCountingStore{memStore: newMemStore()}
			data := randomData(5*maxBlobSize+99, 3)
			ref := putFile(t, store, key, data)
			stat := statFor(data)

			base := t.TempDir()
			seed := filepath.Join(base, "other-server", "world", "r.0.0.mca")
			require.NoError(t, os.MkdirAll(filepath.Dir(seed), 0o755))
			require.NoError(t, os.WriteFile(seed, data, 0o644))

			seeds := NewSeedMap(key)
			require.NoError(t, seeds.Add(filepath.Join(base, "other-server")))
			require.Equal(t, fileParts(t, store, ref), seeds.Len(), "the seed is cut like the backup was")

			path := filepath.Join(base, "this-server", "world", "r.0.0.mca")
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))

			r := &Restorer{Store: store, Key: key, Seeds: seeds}
			store.reset()
			outcome, err := r.RestoreFile(context.Background(), path, stat, ref)
			require.NoError(t, err)
			require.Equal(t, OutcomeWritten, outcome)
			requireRestored(t, path, data, stat)
			require.EqualValues(t, 1, store.reset(), "every part came from the seed")
			require.EqualValues(t, len(data), r.Stats().BytesFromSeeds)
		})
	}
}

func TestRestoreFileFromBlobCache(t *testing.T) {
	for name, key := range keyCases(t) {
		t.Run(name, func(t *testing.T) {
			store := &getCountingStore{memStore: newMemStore()}
			data := randomData(4*maxBlobSize+5, 4)
			ref := putFile(t, store, key, data)
			stat := statFor(data)
			ctx := context.Background()

			cache, err := blobcache.Open(t.TempDir(), "s1", 0)
			require.NoError(t, err)

			path := filepath.Join(t.TempDir(), "a")
			r := &Restorer{Store: store, Key: key, Cache: cache}
			_, err = r.RestoreFile(ctx, path, stat, ref)
			require.NoError(t, err)
			require.EqualValues(t, len(data), r.Stats().BytesFromStore)

			// only the file object remains reachable; every blob must come from the cache
			fileObj, err := store.Get(ctx, ref)
			require.NoError(t, err)
			onlyFile := newMemStore()
			require.NoError(t, onlyFile.Put(ctx, fileObj))

			require.NoError(t, os.Remove(path))
			r = &Restorer{Store: onlyFile, Key: key, Cache: cache}
			outcome, err := r.RestoreFile(ctx, path, stat, ref)
			require.NoError(t, err)
			require.Equal(t, OutcomeWritten, outcome)
			requireRestored(t, path, data, stat)
			require.EqualValues(t, len(data), r.Stats().BytesFromCache)

			// a corrupt entry is dropped and the part downloaded
			parts := fileObj.GetFile().GetParts()
			first := parts[0].Ref
			corrupt := filepath.Join(cache.Dir(), "zz", "corrupt")
			require.NoError(t, os.MkdirAll(filepath.Dir(corrupt), 0o755))
			cached, ok := cache.Get(first)
			require.True(t, ok)
			require.NoError(t, cache.Put(first, cached))

			entries, err := filepath.Glob(filepath.Join(cache.Dir(), "*", "*"))
			require.NoError(t, err)
			require.Len(t, entries, len(parts))

			require.NoError(t, os.Remove(path))
			r = &Restorer{Store: store, Key: key, Cache: cache}
			cache.Drop(first)
			store.reset()
			_, err = r.RestoreFile(ctx, path, stat, ref)
			require.NoError(t, err)
			requireRestored(t, path, data, stat)
			require.EqualValues(t, 2, store.reset(), "the dropped part and the file object are fetched")
		})
	}
}

func TestLiveMarkersIgnoreUnheldLocks(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "world"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "world", "session.lock"), []byte{0x36, 0x37, 0x38}, 0o644))

	held, err := LiveMarkers(root)
	require.NoError(t, err)
	require.Empty(t, held)
}

// streamingStore serves parts through ReadParts and refuses blob Gets,
// like the remote client, and records the parts each call asked for.
type streamingStore struct {
	*memStore

	mtx   sync.Mutex
	calls int
	gets  int
	asked [][]int

	// failAfter, when set, makes the stream fail once it has handed out
	// that many parts
	failAfter, handed int
}

func (s *streamingStore) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	obj, err := s.memStore.Get(ctx, ref)
	if err == nil && obj.GetBlob() != nil {
		return nil, ErrNotFound
	}

	s.mtx.Lock()
	s.gets++
	s.mtx.Unlock()

	return obj, err
}

func (s *streamingStore) ReadParts(ctx context.Context, file *proto.Ref, skip []int, fn func(int, *proto.Object) error) error {
	obj, err := s.memStore.Get(ctx, file)
	if err != nil {
		return err
	}

	parts, err := FileParts(ctx, s.memStore, obj.GetFile())
	if err != nil {
		return err
	}

	skipped := make(map[int]bool, len(skip))
	for _, i := range skip {
		skipped[i] = true
	}

	var asked []int

	for i, part := range parts {
		if !skipped[i] && part.Ref != nil {
			asked = append(asked, i)
		}
	}

	s.mtx.Lock()
	s.calls++
	s.asked = append(s.asked, asked)
	s.mtx.Unlock()

	for _, i := range asked {
		s.mtx.Lock()
		s.handed++
		failed := s.failAfter > 0 && s.handed > s.failAfter
		s.mtx.Unlock()

		if failed {
			return errStreamReset
		}

		blob, err := s.memStore.Get(ctx, parts[i].Ref)
		if err != nil {
			return err
		}

		if err := fn(i, blob); err != nil {
			return err
		}
	}

	return nil
}

// manyParts stores a file of count small parts, every tenth of them the
// same as the first, and returns its content and ref.
func manyParts(t *testing.T, store ObjectStore, count int) ([]byte, *proto.Ref) {
	t.Helper()

	ctx := context.Background()

	var (
		data  []byte
		parts []*proto.FilePart
	)

	for i := range count {
		chunk := randomData(64, int64(i))
		if i%10 == 0 {
			chunk = randomData(64, 0)
		}

		blob := proto.NewObject(&proto.Blob{Data: chunk})
		require.NoError(t, store.Put(ctx, blob))

		parts = append(parts, &proto.FilePart{Offset: uint64(len(data)), Length: uint64(len(chunk)), Ref: blob.Ref()})
		data = append(data, chunk...)
	}

	file := proto.NewObject(&proto.File{Parts: parts})
	require.NoError(t, store.Put(ctx, file))

	return data, file.Ref()
}

func TestAStripedStreamAsksForEveryDistinctPartOnce(t *testing.T) {
	store := &streamingStore{memStore: newMemStore()}
	data, ref := manyParts(t, store.memStore, 3*stripeParts+70)
	stat := statFor(data)

	var written atomic.Int64
	ctx := WithWritten(context.Background(), func(n int64) { written.Add(n) })
	path := filepath.Join(t.TempDir(), "region.mca")

	r := &Restorer{Store: store, Workers: 8}
	outcome, err := r.RestoreFile(ctx, path, stat, ref)
	require.NoError(t, err)
	require.Equal(t, OutcomeWritten, outcome)
	requireRestored(t, path, data, stat)
	require.EqualValues(t, len(data), written.Load(), "every copy of a part counts")

	obj, err := store.memStore.Get(ctx, ref)
	require.NoError(t, err)

	parts := obj.GetFile().GetParts()
	asked := map[string]int{}

	for _, stripe := range store.asked {
		require.True(t, sort.IntsAreSorted(stripe))
		require.Less(t, stripe[len(stripe)-1]-stripe[0], len(parts)/2, "a stripe is a contiguous stretch of the file")

		for _, i := range stripe {
			asked[string(parts[i].Ref.Hash)]++
		}
	}

	distinct := map[string]bool{}
	for _, part := range parts {
		distinct[string(part.Ref.Hash)] = true
	}

	require.Len(t, asked, len(distinct), "every distinct part is asked for")
	require.Equal(t, len(distinct)/stripeParts, store.calls, "a stripe per stripeParts distinct parts")
	require.Greater(t, store.calls, 1)

	for _, n := range asked {
		require.Equal(t, 1, n, "and only once")
	}
}

func TestAPartMissingFromOneStripeFailsTheFile(t *testing.T) {
	ctx := context.Background()
	store := &streamingStore{memStore: newMemStore()}
	data, ref := manyParts(t, store.memStore, 3*stripeParts)

	obj, err := store.memStore.Get(ctx, ref)
	require.NoError(t, err)
	require.NoError(t, store.memStore.Delete(ctx, obj.GetFile().GetParts()[2*stripeParts+5].Ref))

	path := filepath.Join(t.TempDir(), "region.mca")

	_, err = (&Restorer{Store: store, Workers: 8}).RestoreFile(ctx, path, statFor(data), ref)
	require.ErrorIs(t, err, ErrNotFound)
	require.Contains(t, err.Error(), fmt.Sprintf("file %x", ref.Hash))

	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err), "nothing is left behind")
}

func TestAPartTheFileHoldsTwiceIsFetchedOnce(t *testing.T) {
	ctx := context.Background()
	store := &getCountingStore{memStore: newMemStore()}
	data, ref := manyParts(t, store.memStore, 40)
	stat := statFor(data)
	path := filepath.Join(t.TempDir(), "region.mca")

	r := &Restorer{Store: store, Workers: 4}
	_, err := r.RestoreFile(ctx, path, stat, ref)
	require.NoError(t, err)
	requireRestored(t, path, data, stat)
	require.EqualValues(t, 1+40-3, store.reset(), "the file object and each distinct part")
	require.EqualValues(t, len(data), r.Stats().BytesFromStore)
}

func TestRestoreFileStreamsPartsFromPartReader(t *testing.T) {
	for name, key := range keyCases(t) {
		t.Run(name, func(t *testing.T) {
			store := &streamingStore{memStore: newMemStore()}
			data := randomData(6*maxBlobSize+777, 1)
			ref := putFile(t, store.memStore, key, data)
			stat := statFor(data)

			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "region.mca")

			r := &Restorer{Store: store, Key: key, Workers: 4}
			outcome, err := r.RestoreFile(ctx, path, stat, ref)
			require.NoError(t, err)
			require.Equal(t, OutcomeWritten, outcome)
			requireRestored(t, path, data, stat)
			require.Equal(t, 1, store.calls, "one stream per file")
			require.Equal(t, 1, store.gets, "only the file object is fetched")
			require.EqualValues(t, len(data), r.Stats().BytesFromStore)

			damaged := append([]byte(nil), data...)
			damaged[len(damaged)/2] ^= 0xff
			require.NoError(t, os.WriteFile(path, damaged, 0o644))

			outcome, err = r.RestoreFile(ctx, path, stat, ref)
			require.NoError(t, err)
			require.Equal(t, OutcomeWritten, outcome)
			requireRestored(t, path, data, stat)
			require.Less(t, r.Stats().BytesFromStore, int64(len(data))+int64(maxBlobSize), "only the damaged part is streamed")
		})
	}
}

// recordStore reads parts through ReadRecords, leaving out every
// skipEvery'th ref it is asked for, and records the batches and the most
// part bytes it held at once.
type recordStore struct {
	*memStore

	skipEvery int

	mtx      sync.Mutex
	batches  [][]*proto.Ref
	held     int64
	mostHeld int64
}

func (s *recordStore) ReadRecords(ctx context.Context, refs []*proto.Ref) ([]*proto.Object, error) {
	objects := make([]*proto.Object, len(refs))

	var size int64

	for i, ref := range refs {
		obj, err := s.memStore.Get(ctx, ref)
		if err != nil {
			return nil, err
		}

		size += int64(len(obj.GetBlob().GetData()))

		if s.skipEvery == 0 || (i+1)%s.skipEvery != 0 {
			objects[i] = obj
		}
	}

	s.mtx.Lock()
	s.batches = append(s.batches, refs)
	s.held += size
	s.mostHeld = max(s.mostHeld, s.held)
	s.mtx.Unlock()

	time.Sleep(time.Millisecond)

	s.mtx.Lock()
	s.held -= size
	s.mtx.Unlock()

	return objects, nil
}

func TestARecordReaderIsAskedForNeighbouringPartsInBatches(t *testing.T) {
	store := &recordStore{memStore: newMemStore(), skipEvery: 7}
	data, ref := manyParts(t, store.memStore, 3*MaxFilesPerRead+70)
	stat := statFor(data)
	path := filepath.Join(t.TempDir(), "000000010000000000000001")

	r := &Restorer{Store: store, ReadAhead: 2 * 64 * MaxFilesPerRead}
	outcome, err := r.RestoreFile(context.Background(), path, stat, ref)
	require.NoError(t, err)
	require.Equal(t, OutcomeWritten, outcome)
	requireRestored(t, path, data, stat)

	asked := map[string]int{}
	for _, batch := range store.batches {
		require.LessOrEqual(t, len(batch), MaxFilesPerRead)

		for _, ref := range batch {
			asked[string(ref.Hash)]++
		}
	}

	for _, n := range asked {
		require.Equal(t, 1, n, "every distinct part is asked for once")
	}

	require.LessOrEqual(t, store.mostHeld, r.ReadAhead, "the read-ahead bounds the bytes asked for at once")
}

func TestARecordBatchIsAtMostRecordBatchBytes(t *testing.T) {
	store := &recordStore{memStore: newMemStore()}
	data := randomData(3*recordBatch+5, 1)
	ref := putFile(t, store.memStore, nil, data)
	stat := statFor(data)
	path := filepath.Join(t.TempDir(), "16384")

	_, err := (&Restorer{Store: store}).RestoreFile(context.Background(), path, stat, ref)
	require.NoError(t, err)
	requireRestored(t, path, data, stat)

	require.GreaterOrEqual(t, len(store.batches), 4)

	for _, batch := range store.batches {
		var size int64
		for _, ref := range batch {
			obj, err := store.memStore.Get(context.Background(), ref)
			require.NoError(t, err)

			size += int64(len(obj.GetBlob().GetData()))
		}

		require.LessOrEqual(t, size, int64(recordBatch))
	}
}

// zeroed returns data with the parts' ranges blanked, which is what a
// salvaged file holds where its holes are.
func zeroed(data []byte, parts ...*proto.FilePart) []byte {
	want := append([]byte(nil), data...)
	for _, part := range parts {
		copy(want[part.Offset:part.Offset+part.Length], make([]byte, part.Length))
	}

	return want
}

func TestSalvageWritesWhatItCanAndNamesTheHoles(t *testing.T) {
	for name, key := range keyCases(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := newMemStore()

			data := randomData(5*maxBlobSize+99, 11)
			ref := putFile(t, store, key, data)

			obj, err := store.Get(ctx, ref)
			require.NoError(t, err)

			parts := obj.GetFile().GetParts()
			require.Greater(t, len(parts), 3)

			// one hole in the middle and one at the end, which would leave
			// the file short if it were not filled
			lost := []*proto.FilePart{parts[1], parts[len(parts)-1]}
			for _, part := range lost {
				require.NoError(t, store.Delete(ctx, part.Ref))
			}

			path := filepath.Join(t.TempDir(), "world.dat")

			var holes []Hole
			restorer := &Restorer{Store: store, Key: key, Salvage: true, Verify: true, OnHole: func(h Hole) { holes = append(holes, h) }}

			outcome, err := restorer.RestoreFile(ctx, path, statFor(data), ref)
			require.NoError(t, err)
			require.Equal(t, OutcomeSalvaged, outcome)
			require.Len(t, holes, 2)
			require.EqualValues(t, lost[0].Offset, holes[0].Offset)
			require.EqualValues(t, lost[1].Length, holes[1].Length)

			got, err := os.ReadFile(path)
			require.NoError(t, err)
			require.True(t, bytes.Equal(zeroed(data, lost...), got), "only the missing parts are blank")

			stats := restorer.Stats()
			require.EqualValues(t, 1, stats.Salvaged)
			require.EqualValues(t, lost[0].Length+lost[1].Length, stats.MissingBytes)
		})
	}
}

func TestASalvageOverAPartialLeavesZeroesInItsHoles(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()

	data := randomData(3*maxBlobSize+99, 12)
	ref := putFile(t, store, nil, data)
	parts := partsOf(t, store, ref)
	require.NoError(t, store.Delete(ctx, parts[1].Ref))

	path := filepath.Join(t.TempDir(), "vm.img")
	writePartial(t, path, bytes.Repeat([]byte{0xaa}, len(data)))

	outcome, err := (&Restorer{Store: store, Salvage: true}).RestoreFile(ctx, path, statFor(data), ref)
	require.NoError(t, err)
	require.Equal(t, OutcomeSalvaged, outcome)

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.True(t, bytes.Equal(zeroed(data, parts[1]), got), "the hole holds zeroes, not what the partial held")
}

func TestAMissingPartFailsTheFileWithoutSalvage(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()

	data := randomData(3*maxBlobSize, 12)
	ref := putFile(t, store, nil, data)

	obj, err := store.Get(ctx, ref)
	require.NoError(t, err)
	require.NoError(t, store.Delete(ctx, obj.GetFile().GetParts()[1].Ref))

	path := filepath.Join(t.TempDir(), "world.dat")

	_, err = (&Restorer{Store: store}).RestoreFile(ctx, path, statFor(data), ref)
	require.ErrorIs(t, err, ErrNotFound)

	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err), "nothing is left behind")
}

func TestRechunkTakesMovedPartsFromTheDestination(t *testing.T) {
	for name, key := range keyCases(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := newMemStore()

			data := randomData(6*maxBlobSize+13, 21)
			ref := putFile(t, store, key, data)

			// the destination holds the same content a few bytes further
			// along, so nothing matches at the recorded offsets
			shifted := append(randomData(64, 22), data...)
			path := filepath.Join(t.TempDir(), "world.dat")
			require.NoError(t, os.WriteFile(path, shifted, 0o644))

			counting := &getCountingStore{memStore: store}

			restorer := &Restorer{Store: counting, Key: key, Rechunk: true}
			outcome, err := restorer.RestoreFile(ctx, path, statFor(data), ref)
			require.NoError(t, err)
			require.Equal(t, OutcomeWritten, outcome)
			requireRestored(t, path, data, statFor(data))

			stats := restorer.Stats()
			require.Greater(t, stats.BytesFromDestination, int64(0), "the moved parts come off the disk")
			require.Less(t, stats.BytesFromStore, int64(len(data)/2), "most of the file is not downloaded")

			// without it the same restore downloads the whole file
			plain := filepath.Join(t.TempDir(), "world.dat")
			require.NoError(t, os.WriteFile(plain, shifted, 0o644))

			straight := &Restorer{Store: counting, Key: key}
			_, err = straight.RestoreFile(ctx, plain, statFor(data), ref)
			require.NoError(t, err)
			require.Greater(t, straight.Stats().BytesFromStore, stats.BytesFromStore)
		})
	}
}

func TestAPartSealedAsAnotherRefIsRefused(t *testing.T) {
	key := newKey(t)
	part := SealBlob(key, []byte("the part"))
	other := SealBlob(key, []byte("some other part"))

	reader := newFileReader(context.Background(), newMemStore(), &proto.File{}, key)
	filePart := &proto.FilePart{Length: 8, Ref: part.Ref}

	_, err := reader.openPart(0, filePart, proto.NewObject(part))
	require.NoError(t, err)

	_, err = reader.openPart(0, filePart, proto.NewObject(other))
	require.Error(t, err, "a part is only ever the object its ref names")
}

var errStreamReset = errors.New("stream reset")

// failingStore serves the file object and the first ok parts asked for,
// then fails every part, like a connection that dropped part way.
type failingStore struct {
	*memStore

	mtx sync.Mutex
	ok  int
}

func (f *failingStore) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	obj, err := f.memStore.Get(ctx, ref)
	if err != nil || obj.GetFile() != nil {
		return obj, err
	}

	f.mtx.Lock()
	defer f.mtx.Unlock()

	if f.ok == 0 {
		return nil, errStreamReset
	}

	f.ok--

	return obj, nil
}

func partsOf(t *testing.T, store ObjectStore, ref *proto.Ref) []*proto.FilePart {
	t.Helper()

	obj, err := store.Get(context.Background(), ref)
	require.NoError(t, err)

	return obj.GetFile().GetParts()
}

// intactParts are the parts the file at path holds as data has them.
func intactParts(t *testing.T, path string, data []byte, parts []*proto.FilePart) []int {
	t.Helper()

	got, err := os.ReadFile(path)
	require.NoError(t, err)

	var intact []int

	for i, part := range parts {
		end := part.Offset + part.Length
		if end <= uint64(len(got)) && bytes.Equal(got[part.Offset:end], data[part.Offset:end]) {
			intact = append(intact, i)
		}
	}

	return intact
}

// failPartWay restores data's file through a store that fails after ok
// parts and returns where the restore was headed.
func failPartWay(t *testing.T, store *memStore, key *storekey.Key, data []byte, ref *proto.Ref, ok int) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "region.mca")

	_, err := (&Restorer{Store: &failingStore{memStore: store, ok: ok}, Key: key, Workers: 1}).RestoreFile(context.Background(), path, statFor(data), ref)
	require.ErrorIs(t, err, errStreamReset)
	require.NoFileExists(t, path)
	require.Len(t, intactParts(t, PartialPath(path), data, partsOf(t, store, ref)), ok, "the parts written before the failure stay")

	return path
}

func TestARestoreResumesFromItsPartial(t *testing.T) {
	for name, key := range keyCases(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := newMemStore()
			data := randomData(6*maxBlobSize+777, 5)
			ref := putFile(t, store, key, data)
			parts := partsOf(t, store, ref)
			stat := statFor(data)

			path := failPartWay(t, store, key, data, ref, 3)

			var written atomic.Int64
			counting := &getCountingStore{memStore: store}
			r := &Restorer{Store: counting, Key: key, Workers: 4}

			outcome, err := r.RestoreFile(WithWritten(ctx, func(n int64) { written.Add(n) }), path, stat, ref)
			require.NoError(t, err)
			require.Equal(t, OutcomeWritten, outcome)
			requireRestored(t, path, data, stat)
			require.EqualValues(t, 1+len(parts)-3, counting.reset(), "only the parts the partial lacks are fetched")
			require.EqualValues(t, len(data), written.Load(), "the parts kept count too")

			entries, err := os.ReadDir(filepath.Dir(path))
			require.NoError(t, err)
			require.Len(t, entries, 1, "the partial became the file")

			writePartial(t, path, []byte("left over"))

			outcome, err = r.RestoreFile(ctx, path, stat, ref)
			require.NoError(t, err)
			require.Equal(t, OutcomeUnchanged, outcome)
			require.NoFileExists(t, PartialPath(path), "a destination that needs no writing drops its partial")
		})
	}
}

func TestAStreamedRestoreResumesFromItsPartial(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	data := randomData(6*maxBlobSize+777, 6)
	ref := putFile(t, store, nil, data)
	parts := partsOf(t, store, ref)
	stat := statFor(data)
	path := filepath.Join(t.TempDir(), "region.mca")

	_, err := (&Restorer{Store: &streamingStore{memStore: store, failAfter: 4}}).RestoreFile(ctx, path, stat, ref)
	require.ErrorIs(t, err, errStreamReset)
	require.Len(t, intactParts(t, PartialPath(path), data, parts), 4)

	resumed := &streamingStore{memStore: store}
	_, err = (&Restorer{Store: resumed}).RestoreFile(ctx, path, stat, ref)
	require.NoError(t, err)
	requireRestored(t, path, data, stat)
	require.NoFileExists(t, PartialPath(path))

	asked := 0
	for _, stripe := range resumed.asked {
		asked += len(stripe)
	}

	require.Equal(t, len(parts)-4, asked, "only the parts the partial lacks are streamed")
}

func TestADamagedOrShortPartialIsRefetched(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	data := randomData(8*maxBlobSize+31, 7)
	ref := putFile(t, store, nil, data)
	parts := partsOf(t, store, ref)
	stat := statFor(data)

	path := failPartWay(t, store, nil, data, ref, 5)
	partial := PartialPath(path)
	intact := intactParts(t, partial, data, parts)

	first, last := parts[intact[0]], parts[intact[len(intact)-1]]

	file, err := os.OpenFile(partial, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = file.WriteAt([]byte{^data[first.Offset]}, int64(first.Offset))
	require.NoError(t, err)
	require.NoError(t, file.Truncate(int64(last.Offset+last.Length/2)))
	require.NoError(t, file.Close())

	kept := intactParts(t, partial, data, parts)
	require.Len(t, kept, 3)

	counting := &getCountingStore{memStore: store}
	r := &Restorer{Store: counting, Workers: 4}

	_, err = r.RestoreFile(ctx, path, stat, ref)
	require.NoError(t, err)
	requireRestored(t, path, data, stat)
	require.EqualValues(t, 1+len(parts)-len(kept), counting.reset(), "the damaged and the cut part are fetched again")

	var keptBytes uint64
	for _, i := range kept {
		keptBytes += parts[i].Length
	}

	require.EqualValues(t, keptBytes, r.Stats().BytesFromDestination)
}

func TestAPartialOfAnotherVersionIsNotTrusted(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	older := randomData(6*maxBlobSize+777, 8)
	path := failPartWay(t, store, nil, older, putFile(t, store, nil, older), 4)

	// longer than the version restored next, with a hole in it
	require.NoError(t, os.Truncate(PartialPath(path), int64(len(older))))

	data := randomData(3*maxBlobSize+5, 9)
	ref := putFile(t, store, nil, data)
	stat := statFor(data)

	r := &Restorer{Store: store, Verify: true}
	outcome, err := r.RestoreFile(ctx, path, stat, ref)
	require.NoError(t, err)
	require.Equal(t, OutcomeWritten, outcome)
	requireRestored(t, path, data, stat)
	require.EqualValues(t, len(data), r.Stats().BytesFromStore)
	require.NoFileExists(t, PartialPath(path))
}
