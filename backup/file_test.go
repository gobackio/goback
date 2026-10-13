package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"math/rand"
	"sync"
	"testing"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// memStore is the smallest ObjectStore that honours the ErrNotFound contract.
func testRef(seed string) *proto.Ref {
	sum := sha256.Sum256([]byte(seed))
	return &proto.Ref{Hash: sum[:]}
}

type memStore struct {
	mtx     sync.RWMutex
	objects map[string]*proto.Object
}

func newMemStore() *memStore {
	return &memStore{objects: make(map[string]*proto.Object)}
}

func (m *memStore) Put(_ context.Context, obj *proto.Object) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.objects[string(obj.Ref().Hash)] = obj
	return nil
}

func (m *memStore) Get(_ context.Context, ref *proto.Ref) (*proto.Object, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	obj, ok := m.objects[string(ref.Hash)]
	if !ok {
		return nil, ErrNotFound
	}

	return obj, nil
}

func (m *memStore) Delete(_ context.Context, ref *proto.Ref) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	delete(m.objects, string(ref.Hash))
	return nil
}

func (m *memStore) Walk(context.Context, bool, proto.ObjectType, ObjectReceiver) error {
	return ErrNotImplemented
}

func (m *memStore) Has(_ context.Context, ref *proto.Ref) (bool, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	_, ok := m.objects[string(ref.Hash)]
	return ok, nil
}

func writeTestFile(t *testing.T, store ObjectStore, size int) ([]byte, *proto.File) {
	t.Helper()

	data := make([]byte, size)
	_, err := rand.New(rand.NewSource(int64(size))).Read(data)
	require.NoError(t, err)

	writer := newFileWriter(context.Background(), store, nil, 0)
	_, err = writer.Write(data)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	obj, err := store.Get(context.Background(), writer.Ref())
	require.NoError(t, err)

	return data, obj.GetFile()
}

func TestFileReaderRoundTrip(t *testing.T) {
	store := newMemStore()
	data, file := writeTestFile(t, store, 3*maxBlobSize+12345)
	require.Greater(t, len(file.Parts), 1)

	t.Run("WriteTo", func(t *testing.T) {
		var out bytes.Buffer
		n, err := newFileReader(context.Background(), store, file, nil).WriteTo(&out)
		require.NoError(t, err)
		require.EqualValues(t, len(data), n)
		require.True(t, bytes.Equal(data, out.Bytes()))
	})

	t.Run("Read", func(t *testing.T) {
		out, err := io.ReadAll(newFileReader(context.Background(), store, file, nil))
		require.NoError(t, err)
		require.True(t, bytes.Equal(data, out))
	})

	t.Run("SeekEnd", func(t *testing.T) {
		reader := newFileReader(context.Background(), store, file, nil)

		pos, err := reader.Seek(-100, io.SeekEnd)
		require.NoError(t, err)
		require.EqualValues(t, len(data)-100, pos)

		tail, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.True(t, bytes.Equal(data[len(data)-100:], tail))

		pos, err = reader.Seek(0, io.SeekEnd)
		require.NoError(t, err)
		require.EqualValues(t, len(data), pos)

		_, err = reader.Read(make([]byte, 1))
		require.ErrorIs(t, err, io.EOF)
	})
}

func TestFileReaderMissingPart(t *testing.T) {
	store := newMemStore()
	data, file := writeTestFile(t, store, 2*maxBlobSize+999)

	missing := file.Parts[len(file.Parts)/2]
	require.NoError(t, store.Delete(context.Background(), missing.Ref))

	t.Run("WriteTo fails instead of zero-filling", func(t *testing.T) {
		var out bytes.Buffer
		_, err := newFileReader(context.Background(), store, file, nil).WriteTo(&out)
		require.ErrorIs(t, err, ErrNotFound)
		require.Less(t, out.Len(), len(data))
	})

	t.Run("Read fails instead of zero-filling", func(t *testing.T) {
		_, err := io.ReadAll(newFileReader(context.Background(), store, file, nil))
		require.ErrorIs(t, err, ErrNotFound)
	})
}

func TestFileReaderWriteToCancel(t *testing.T) {
	store := newMemStore()
	_, file := writeTestFile(t, store, 4*maxBlobSize)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() {
		_, err := newFileReader(ctx, store, file, nil).WriteTo(io.Discard)
		done <- err
	}()

	err := <-done
	require.ErrorIs(t, err, context.Canceled)
}

func TestRootTreeOrderIsDeterministic(t *testing.T) {
	refs := make(map[string]bool)

	for run := 0; run < 5; run++ {
		writer := newTree(newMemStore())

		names := []string{"b", "a", "d", "c"}
		rand.Shuffle(len(names), func(i, j int) { names[i], names[j] = names[j], names[i] })

		for _, name := range names {
			writer.Node(&proto.TreeNode{Stat: &proto.FileInfo{Name: []byte(name)}, Ref: testRef(name)})
		}

		tree := proto.NewObject(&proto.Tree{Nodes: writer.sortedNodes()})
		refs[string(tree.Ref().Hash)] = true
	}

	require.Len(t, refs, 1)
}

func TestWriteToStartsAtTheReadOffsetAndFetchesAhead(t *testing.T) {
	ctx := context.Background()
	store := &gatedStore{memStore: newMemStore()}

	data, ref := distinctParts(t, store.memStore, 3*readWindow)
	obj, err := store.memStore.Get(ctx, ref)
	require.NoError(t, err)
	// the first part and every part after the slow second one that fits
	// in a window sliding past the first
	store.gate, store.want = obj.GetFile().GetParts()[1].Ref, readWindow

	var out bytes.Buffer
	n, err := newFileReader(ctx, store, obj.GetFile(), nil).WriteTo(&out)
	require.NoError(t, err)
	require.EqualValues(t, len(data), n)
	require.Equal(t, data, out.Bytes())

	reader := newFileReader(ctx, store.memStore, obj.GetFile(), nil)
	_, err = reader.Seek(100, io.SeekStart)
	require.NoError(t, err)

	out.Reset()
	_, err = reader.WriteTo(&out)
	require.NoError(t, err)
	require.Equal(t, data[100:], out.Bytes())
}
