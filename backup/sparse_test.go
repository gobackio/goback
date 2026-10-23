package backup

import (
	"bytes"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// sparseLayout is a file of 8 MiB with two data extents, aligned to the
// 64 KiB unit NTFS allocates sparse files in.
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
	buf := make([]byte, l.size)
	for off, data := range l.extents {
		copy(buf[off:], data)
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

	reader, ok := contentReader(file, info).(*holeReader)
	require.True(t, ok)

	counter := &countingReaderAt{ReaderAt: file}
	reader.data = counter

	return reader, counter
}

func TestASparseFileBacksUpToTheRefOfTheSameContentWrittenDensely(t *testing.T) {
	f := newWalkerFixture(t)
	layout := newSparseLayout(t)

	layout.write(t, filepath.Join(f.root, "sparse.img"))
	f.write("dense.img", layout.dense())

	root := f.tree(f.run().Commit.Tree)
	require.True(t, root["sparse.img"].Ref.Equal(root["dense.img"].Ref))

	hashed, err := HashFile(filepath.Join(f.root, "sparse.img"), nil)
	require.NoError(t, err)
	require.True(t, hashed.Equal(root["dense.img"].Ref))
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

	require.Same(t, file, contentReader(file, info))
}
