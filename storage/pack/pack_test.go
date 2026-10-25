package pack

import (
	"bytes"
	"context"
	"math/rand"
	"slices"
	"testing"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// number of objects to generate for the tests
const numObjects = 1000

// average size of objects
const ObjectSize = 1024 * 8

// plain is obj marshaled without the Stored a decoded object read back
// carries.
func plain(obj *proto.Object) []byte {
	if obj.GetObject() == nil {
		return obj.Bytes()
	}

	return proto.Bytes(&proto.Object{Object: obj.Object, KeyId: obj.KeyId})
}

func makeRef() *proto.Ref {
	hash := make([]byte, proto.HashSize)
	_, err := rand.Read(hash)
	if err != nil {
		panic(err)
	}

	return &proto.Ref{
		Hash: hash,
	}
}

func makeTestData(t *testing.T, num int) []*proto.Object {
	t.Logf("Generating %d test objects", num)

	objects := make([]*proto.Object, num)

	for i := 0; i < num; i++ {
		size := rand.Int63n(ObjectSize * 2)
		randomBytes := make([]byte, size)
		_, err := rand.Read(randomBytes)
		if err != nil {
			t.Fatalf("Failed reading random data: %v", err)
		}

		objects[i] = proto.NewObject(&proto.Blob{
			Data: randomBytes,
		})
	}

	return objects
}

func TestPack(t *testing.T) {
	base := t.TempDir()

	storage := newLocal(base)
	index := NewInMemoryIndex()

	options := []PackOption{
		WithArchiveStorage(storage),
		WithArchiveIndex(index),
		WithMaxSize(1024 * 1024 * 5),
	}

	store, err := NewPackStorage(options...)
	require.Nil(t, err)

	objects := makeTestData(t, numObjects)
	t.Logf("Storing %d objects", numObjects)

	readObjects := func(t *testing.T) {
		t.Helper()

		t.Logf("Reading back %d objects", numObjects)
		for _, i := range rand.Perm(numObjects) {
			original := objects[i]

			object, err := store.Get(context.Background(), original.Ref())
			if err != nil {
				t.Fatal(err)
			}

			if object == nil {
				t.Fatalf("Couldn't find expected object %x", original.Ref().Hash)
			}

			if !bytes.Equal(plain(object), plain(original)) {
				t.Logf("Original %x", plain(original))
				t.Logf("Stored %x", plain(object))
				t.Fatalf("Object read back incorrectly")
			}
		}
	}

	for _, object := range objects {
		err := store.Put(context.Background(), object)
		if err != nil {
			t.Fatal(err)
		}
	}

	readObjects(t)

	t.Log("Closing pack store")
	err = store.Close()
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Reopening pack store")
	store, err = NewPackStorage(options...)
	require.Nil(t, err)

	err = store.Open()
	require.Nil(t, err)

	readObjects(t)

	t.Log("Closing pack store")
	err = store.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestPackMissingObject(t *testing.T) {
	store, err := NewPackStorage(
		WithArchiveStorage(newLocal(t.TempDir())),
		WithArchiveIndex(NewInMemoryIndex()),
	)
	require.NoError(t, err)

	_, err = store.Get(context.Background(), makeRef())
	require.ErrorIs(t, err, backup.ErrNotFound)

	has, err := store.Has(context.Background(), makeRef())
	require.NoError(t, err)
	require.False(t, has)

	require.NoError(t, store.Close())
}

// TestCompactionKeepsAnsweringReaders runs Has and Get against every object
// while a compaction retires their archives: no committed object may look
// absent, and a retired archive is never opened again.
func TestCompactionKeepsAnsweringReaders(t *testing.T) {
	base := t.TempDir()
	index := NewInMemoryIndex()

	store, err := NewPackStorage(
		WithArchiveStorage(newLocal(base)),
		WithArchiveIndex(index),
		WithMaxSize(1024*1024),
		WithCompaction(CompactionConfig{MinimumCandidates: 0}),
	)
	require.NoError(t, err)

	// many small archives, so the compaction has plenty to retire
	objects := makeTestData(t, numObjects)
	for i, object := range objects {
		require.NoError(t, store.Put(context.Background(), object))
		if i%10 == 9 {
			require.NoError(t, store.Flush())
		}
	}
	require.NoError(t, store.Flush())

	before, _, err := store.archiveNames()
	require.NoError(t, err)
	require.Greater(t, len(before), 10)

	compacted := make(chan error, 1)
	go func() { _, err := store.Compact(context.Background()); compacted <- err }()

	ctx := context.Background()
	for done := false; !done; {
		select {
		case err := <-compacted:
			require.NoError(t, err)
			done = true
		default:
		}

		for _, object := range objects {
			has, err := store.Has(ctx, object.Ref())
			require.NoError(t, err)
			require.True(t, has, "object %x looked absent during compaction", object.Ref().Hash)

			_, err = store.Get(ctx, object.Ref())
			require.NoError(t, err, "object %x during compaction", object.Ref().Hash)
		}
	}

	after, _, err := store.archiveNames()
	require.NoError(t, err)
	require.Less(t, len(after), len(before))

	for _, name := range before {
		if slices.Contains(after, name) {
			continue
		}

		_, err := store.archiveByName(name)
		require.ErrorIs(t, err, errArchiveRetired, "a stale location must not reopen %s", name)
	}

	require.NoError(t, store.Close())
}

// TestPackCompaction writes objects across many small archives, compacts them
// while the store stays open, and expects every object to remain readable.
func TestPackCompaction(t *testing.T) {
	base := t.TempDir()
	index := NewInMemoryIndex()

	store, err := NewPackStorage(
		WithArchiveStorage(newLocal(base)),
		WithArchiveIndex(index),
		WithMaxSize(1024*64),
		WithCompaction(CompactionConfig{MinimumCandidates: 0}),
	)
	require.NoError(t, err)

	objects := makeTestData(t, numObjects)
	for _, object := range objects {
		require.NoError(t, store.Put(context.Background(), object))
	}

	// store a duplicate so compaction has something to drop
	require.NoError(t, store.Flush())
	require.NoError(t, store.Put(context.Background(), objects[0]))
	require.NoError(t, store.Flush())

	archivesBefore, _, err := store.archiveNames()
	require.NoError(t, err)
	require.Greater(t, len(archivesBefore), 1)

	compact(t, context.Background(), store)

	archivesAfter, _, err := store.archiveNames()
	require.NoError(t, err)
	require.Less(t, len(archivesAfter), len(archivesBefore))

	for _, i := range rand.Perm(numObjects) {
		original := objects[i]

		object, err := store.Get(context.Background(), original.Ref())
		require.NoError(t, err, "object %x after compaction", original.Ref().Hash)
		require.True(t, bytes.Equal(plain(object), plain(original)))

		loc, err := index.LocateObject(original.Ref(), Scope{})
		require.NoError(t, err)
		require.Contains(t, archivesAfter, loc.Archive)
	}

	require.NoError(t, store.Close())
}

func TestParallelCompactionKeepsOneCopyOfAnObjectSeveralArchivesHold(t *testing.T) {
	index := NewInMemoryIndex()

	store, err := NewPackStorage(
		WithArchiveStorage(newLocal(t.TempDir())),
		WithArchiveIndex(index),
		WithMaxSize(64<<20),
		WithCompaction(CompactionConfig{Workers: 4}),
	)
	require.NoError(t, err)

	objects := makeTestData(t, numObjects)
	for range 3 {
		for _, object := range objects {
			require.NoError(t, store.Put(context.Background(), object))
		}

		require.NoError(t, store.Flush())
	}

	refs := make([]*proto.Ref, len(objects))
	for i, object := range objects {
		refs[i] = object.Ref()
	}

	before, err := index.LocateCopies(refs, Scope{})
	require.NoError(t, err)
	require.Greater(t, len(before[string(refs[0].Hash)]), 1, "the test needs objects held more than once")

	compact(t, context.Background(), store)

	after, err := index.LocateCopies(refs, Scope{})
	require.NoError(t, err)

	for i, ref := range refs {
		require.Len(t, after[string(ref.Hash)], 1, "object %d", i)

		object, err := store.Get(context.Background(), ref)
		require.NoError(t, err)
		require.Equal(t, plain(objects[i]), plain(object))
	}

	require.NoError(t, store.Close())
}

var benchRnd = rand.New(rand.NewSource(0))

type Opener interface {
	Open() error
}

type Closer interface {
	Close() error
}

func benchmarkStorage(b *testing.B, store backup.ObjectStore) {
	benchRnd.Seed(0)
	b.ReportAllocs()

	if op, ok := store.(Opener); ok {
		err := op.Open()
		if err != nil {
			b.Fatal(err)
		}
	}

	for i := 0; i < b.N; i++ {
		blobBytes := make([]byte, benchRnd.Intn(16*1024))
		_, _ = benchRnd.Read(blobBytes)
		object := proto.NewObject(&proto.Blob{
			Data: blobBytes,
		})

		err := store.Put(context.Background(), object)
		if err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(len(blobBytes)))
	}

	if cl, ok := store.(Closer); ok {
		err := cl.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestArchivesPastWhatTheirOffsetsReachAreRefused(t *testing.T) {
	_, err := NewPackStorage(WithArchiveStorage(newLocal(t.TempDir())), WithArchiveIndex(NewInMemoryIndex()), WithMaxSize(8<<30))
	require.Error(t, err)

	store, err := NewPackStorage(WithArchiveStorage(newLocal(t.TempDir())), WithArchiveIndex(NewInMemoryIndex()), WithMaxSize(MaxArchiveSize))
	require.NoError(t, err)
	require.NoError(t, store.Close())
}
