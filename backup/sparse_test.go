package backup

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// sparseLayout is a file, of 8 MiB unless a test sets size, with two data
// extents aligned to the 64 KiB unit NTFS allocates sparse files in.
type sparseLayout struct {
	size    int64
	extents map[int64][]byte
}

func newSparseLayout(t *testing.T) sparseLayout {
	t.Helper()

	extent := func(size int) []byte {
		buf := make([]byte, size)
		_, err := rand.Read(buf)
		require.NoError(t, err)

		return buf
	}

	return sparseLayout{
		size: 8 << 20,
		extents: map[int64][]byte{
			0:       extent(64 << 10),
			3 << 20: extent(192 << 10),
		},
	}
}

func (l sparseLayout) dense() []byte {
	return l.at(0, int(l.size))
}

// at is the n bytes of the file from off.
func (l sparseLayout) at(off int64, n int) []byte {
	buf := make([]byte, n)
	for at, data := range l.extents {
		from, to := max(at, off), min(at+int64(len(data)), off+int64(n))
		if from < to {
			copy(buf[from-off:], data[from-at:to-at])
		}
	}

	return buf
}

func (l sparseLayout) dataBytes() int64 {
	var n int64
	for _, data := range l.extents {
		n += int64(len(data))
	}

	return n
}

// write creates the file at path with holes between the extents, skipping
// the test where the file system keeps it dense.
func (l sparseLayout) write(t *testing.T, path string) {
	t.Helper()

	file, err := os.Create(path)
	require.NoError(t, err)
	defer file.Close()

	setSparse(t, file)
	require.NoError(t, file.Truncate(l.size))

	for off, data := range l.extents {
		_, err := file.WriteAt(data, off)
		require.NoError(t, err)
	}

	require.NoError(t, file.Sync())

	info, err := file.Stat()
	require.NoError(t, err)

	if !isSparse(info) {
		t.Skip("the file system keeps the file dense")
	}
}

type countingReaderAt struct {
	io.ReaderAt
	read int64
}

func (c *countingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	n, err := c.ReaderAt.ReadAt(p, off)
	atomic.AddInt64(&c.read, int64(n))

	return n, err
}

// openHoles opens path through contentReader, counting the bytes it reads
// from the file.
func openHoles(t *testing.T, path string) (*holeReader, *countingReaderAt) {
	t.Helper()

	file, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })

	info, err := file.Stat()
	require.NoError(t, err)

	reader, ok := contentReader(file, info, nil).(*holeReader)
	require.True(t, ok)

	counter := &countingReaderAt{ReaderAt: file}
	reader.data = counter

	return reader, counter
}

// holes are the holes of the file ref names.
func holes(t *testing.T, store ObjectStore, ref *proto.Ref) []*proto.FilePart {
	t.Helper()

	obj, err := store.Get(context.Background(), ref)
	require.NoError(t, err)

	parts, err := FileParts(context.Background(), store, obj.GetFile())
	require.NoError(t, err)

	var holes []*proto.FilePart
	for _, part := range parts {
		if part.Hole > 0 {
			holes = append(holes, part)
		}
	}

	return holes
}

// requireSparse fails unless the file at path takes less space than its
// size.
func requireSparse(t *testing.T, path string) {
	t.Helper()

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.True(t, isSparse(info), "%s is not sparse", path)
}

// requireLayout fails unless the file at path holds what layout does.
func requireLayout(t *testing.T, path string, layout sparseLayout) {
	t.Helper()

	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()

	info, err := file.Stat()
	require.NoError(t, err)
	require.Equal(t, layout.size, info.Size())

	buf := make([]byte, 1<<20)
	for off := int64(0); off < layout.size; off += int64(len(buf)) {
		n, err := file.ReadAt(buf, off)
		if err != io.EOF {
			require.NoError(t, err)
		}

		require.True(t, bytes.Equal(layout.at(off, n), buf[:n]), "content at %d", off)
	}
}

func TestASparseFileBelowSplitFileSizeBacksUpToTheRefOfTheSameContentWrittenDensely(t *testing.T) {
	f := newWalkerFixture(t)
	layout := newSparseLayout(t)

	layout.write(t, filepath.Join(f.root, "sparse.img"))
	f.write("dense.img", layout.dense())

	root := f.tree(f.run().Commit.Tree)
	require.True(t, root["sparse.img"].Ref.Equal(root["dense.img"].Ref))
	require.Empty(t, holes(t, f.store, root["sparse.img"].Ref))
}

func TestASparseFileIsStoredWithItsHolesAndRestoredSparse(t *testing.T) {
	f := newWalkerFixture(t)
	layout := newSparseLayout(t)
	layout.size = SplitFileSize + 8<<20
	path := filepath.Join(f.root, "sparse.img")
	layout.write(t, path)

	result := f.run()
	node := f.tree(result.Commit.Tree)["sparse.img"]

	found := holes(t, f.store, node.Ref)
	require.Len(t, found, 2)
	require.EqualValues(t, layout.size, found[1].Offset+found[1].Length)

	hashed, err := HashFile(path, nil)
	require.NoError(t, err)
	require.True(t, hashed.Equal(node.Ref))

	dst := t.TempDir()
	require.NoError(t, (&Restorer{Store: f.store, Verify: true}).RestoreTree(context.Background(), result.Commit, dst, TreeOptions{}))

	requireLayout(t, filepath.Join(dst, "sparse.img"), layout)
	requireSparse(t, filepath.Join(dst, "sparse.img"))
}

func TestAFileWithAHoleFallsShortOfItsLengthToAReaderThatDoesNotKnowHoles(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	ref, data := putSparse(t, store, nil, randomData(1000, 4), sparseMin)

	obj, err := store.Get(ctx, ref)
	require.NoError(t, err)

	file := obj.GetFile()
	require.NotZero(t, file.SplitDepth)
	require.EqualValues(t, len(data), sum(file.SplitLengths))

	var lengths uint64
	for _, split := range file.Splits {
		sub, err := store.Get(ctx, split)
		require.NoError(t, err)

		payload, err := sub.Canonical()
		require.NoError(t, err)

		stored, err := proto.NewObjectFromPayload(payload, proto.ObjectType_FILE)
		require.NoError(t, err)

		for _, part := range stored.GetFile().GetParts() {
			lengths += part.GetLength()
		}
	}

	require.EqualValues(t, 2000, lengths)
}

func TestAFileOf16TebibytesOfHolesBacksUpAndRestoresWithoutReadingThem(t *testing.T) {
	const size = 16<<40 - 1<<20

	f := newWalkerFixture(t)
	path := filepath.Join(f.root, "huge.img")
	data := []byte("the only data")

	file, err := os.Create(path)
	require.NoError(t, err)
	setSparse(t, file)
	if err := file.Truncate(size); err != nil {
		_ = file.Close()
		t.Skipf("the file system holds no file this large: %v", err)
	}
	_, err = file.WriteAt(data, 8<<40)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	info, err := os.Stat(path)
	require.NoError(t, err)
	if !isSparse(info) {
		t.Skip("the file system keeps the file dense")
	}

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	result := f.run()
	node := f.tree(result.Commit.Tree)["huge.img"]
	require.EqualValues(t, size, node.Stat.Size)
	require.Len(t, holes(t, f.store, node.Ref), 2)

	dst := filepath.Join(t.TempDir(), "huge.img")
	_, err = (&Restorer{Store: f.store}).RestoreFile(context.Background(), dst, node.Stat, node.Ref)
	require.NoError(t, err)

	runtime.ReadMemStats(&after)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(256<<20))

	requireSparse(t, dst)

	restored, err := os.Open(dst)
	require.NoError(t, err)
	defer restored.Close()

	got := make([]byte, len(data)+2)
	_, err = restored.ReadAt(got, 8<<40-1)
	require.NoError(t, err)
	require.Equal(t, append(append([]byte{0}, data...), 0), got)

	stat, err := restored.Stat()
	require.NoError(t, err)
	require.EqualValues(t, size, stat.Size())
}

func TestReadingASparseFileReadsOnlyItsData(t *testing.T) {
	layout := newSparseLayout(t)
	path := filepath.Join(t.TempDir(), "sparse.img")
	layout.write(t, path)

	reader, counter := openHoles(t, path)

	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.True(t, bytes.Equal(layout.dense(), content))

	require.GreaterOrEqual(t, counter.read, layout.dataBytes())
	require.Less(t, counter.read, layout.size/4)
}

func TestAWriteIntoAHoleAheadOfTheReadIsRead(t *testing.T) {
	layout := newSparseLayout(t)
	path := filepath.Join(t.TempDir(), "sparse.img")
	layout.write(t, path)

	reader, _ := openHoles(t, path)

	head := make([]byte, 64<<10)
	_, err := io.ReadFull(reader, head)
	require.NoError(t, err)

	late := bytes.Repeat([]byte{0xab}, 64<<10)
	writer, err := os.OpenFile(path, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = writer.WriteAt(late, 6<<20)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	rest, err := io.ReadAll(reader)
	require.NoError(t, err)

	want := layout.dense()
	copy(want[6<<20:], late)
	require.True(t, bytes.Equal(want, append(head, rest...)))
}

func TestADenseFileIsReadAsItIs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dense.bin")
	content := make([]byte, 1<<20)
	_, err := rand.Read(content)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, content, 0o644))

	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()

	info, err := file.Stat()
	require.NoError(t, err)

	require.Same(t, file, contentReader(file, info, nil))
}
