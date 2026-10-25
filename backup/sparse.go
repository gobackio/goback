package backup

import (
	"errors"
	"io"
	"math"
	"os"
	"syscall"
)

// contentReader reads file from its start. For a sparse local file it
// produces the zeros of its holes without reading them, and, in a file of
// at least SplitFileSize, hands each hole of at least sparseMin bytes to
// holes rather than producing it; any other file is read as it is.
func contentReader(file File, info os.FileInfo, holes func(n int64)) io.Reader {
	local, ok := file.(localFile)
	if !ok || !info.Mode().IsRegular() || !isSparse(info) {
		return file
	}

	conn, err := local.SyscallConn()
	if err != nil {
		return file
	}

	// the index looks for the sub-files of a file only from SplitFileSize
	// on, and a file holding a hole is always split
	if info.Size() < SplitFileSize {
		holes = nil
	}

	return &holeReader{conn: conn, data: local, size: info.Size(), holes: holes}
}

// sparseMin is the shortest hole a backup records as a hole part; a
// shorter one is chunked as the zeros it reads as.
const sparseMin = maxBlobSize

// localFile is a file of the local disk, an *os.File or one embedding it.
type localFile interface {
	io.ReaderAt
	SyscallConn() (syscall.RawConn, error)
}

// holeReader reads a sparse file sequentially, asking the file system for
// the next hole or data extent each time it reaches the end of one, so a
// write ahead of the read is seen as plain reads would see it. Past size,
// or once the file system cannot answer, the rest is read as data.
type holeReader struct {
	conn  syscall.RawConn
	data  io.ReaderAt
	size  int64
	holes func(n int64)

	pos  int64
	end  int64
	hole bool
}

func (r *holeReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	if r.pos >= r.end {
		r.next()
	}

	for r.hole && r.holes != nil && r.end-r.pos >= sparseMin {
		r.holes(r.end - r.pos)
		r.pos = r.end
		r.next()
	}

	n := len(p)
	if left := r.end - r.pos; int64(n) > left {
		n = int(left)
	}

	if r.hole {
		clear(p[:n])
		r.pos += int64(n)

		return n, nil
	}

	n, err := r.data.ReadAt(p[:n], r.pos)
	r.pos += int64(n)

	if err == io.EOF && n > 0 {
		err = nil
	}

	return n, err
}

func (r *holeReader) next() {
	r.hole, r.end = false, math.MaxInt64
	if r.pos >= r.size {
		return
	}

	hole, end := nextExtent(r.conn, r.pos, r.size)
	if end <= r.pos {
		return
	}

	if hole && end > r.size {
		end = r.size
	}

	r.hole, r.end = hole, end
}

// dataExtents calls fn with each range of [off, off+n) that r may hold
// anything but zeros in: the extents a sparse local file has allocated
// there, or the whole range.
func dataExtents(r io.ReaderAt, off, n int64, fn func(off, n int64) error) error {
	var conn syscall.RawConn

	if local, ok := r.(localFile); ok {
		conn, _ = local.SyscallConn()
	}

	if conn == nil {
		return fn(off, n)
	}

	for end := off + n; off < end; {
		hole, next := nextExtent(conn, off, end)
		if next <= off || next > end {
			next = end
		}

		if !hole {
			if err := fn(off, next-off); err != nil {
				return err
			}
		}

		off = next
	}

	return nil
}

var errNotZero = errors.New("not all zeros")

// zeroAt reports whether r holds n zero bytes at off.
func zeroAt(r io.ReaderAt, off, n int64) bool {
	buf := make([]byte, min(n, int64(len(zeroes))))

	return dataExtents(r, off, n, func(off, n int64) error {
		for n > 0 {
			chunk := buf[:min(n, int64(len(buf)))]
			if _, err := r.ReadAt(chunk, off); err != nil {
				return err
			}

			if !allZero(chunk) {
				return errNotZero
			}

			off, n = off+int64(len(chunk)), n-int64(len(chunk))
		}

		return nil
	}) == nil
}

// zeroRange makes the n bytes at off in w zeros, writing only over what r,
// the same file, may hold there.
func zeroRange(w io.WriterAt, r io.ReaderAt, off, n int64) error {
	return dataExtents(r, off, n, func(off, n int64) error {
		for n > 0 {
			chunk := zeroes[:min(n, int64(len(zeroes)))]
			if _, err := w.WriteAt(chunk, off); err != nil {
				return err
			}

			off, n = off+int64(len(chunk)), n-int64(len(chunk))
		}

		return nil
	})
}
