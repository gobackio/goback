package backup

import (
	"io"
	"math"
	"os"
	"syscall"
)

// contentReader reads file from its start. For a sparse local file it
// produces the zeros of the holes without reading them; any other file is
// read as it is.
func contentReader(file File, info os.FileInfo) io.Reader {
	local, ok := file.(localFile)
	if !ok || !info.Mode().IsRegular() || !isSparse(info) {
		return file
	}

	conn, err := local.SyscallConn()
	if err != nil {
		return file
	}

	return &holeReader{conn: conn, data: local, size: info.Size()}
}

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
	conn syscall.RawConn
	data io.ReaderAt
	size int64

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
