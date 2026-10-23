//go:build linux || darwin || freebsd

package backup

import (
	"errors"
	"math"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// isSparse reports whether the file has fewer blocks allocated than its
// size needs.
func isSparse(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)

	return ok && int64(stat.Blocks)*512 < info.Size()
}

// nextExtent reports whether the file holds a hole or data at off and
// where that extent ends; data to math.MaxInt64 when the file system
// cannot say.
func nextExtent(conn syscall.RawConn, off, size int64) (hole bool, end int64) {
	end = math.MaxInt64

	_ = conn.Control(func(fd uintptr) {
		data, err := unix.Seek(int(fd), off, unix.SEEK_DATA)

		switch {
		case errors.Is(err, unix.ENXIO):
			hole, end = true, size
		case err != nil:
		case data > off:
			hole, end = true, data
		default:
			next, err := unix.Seek(int(fd), off, unix.SEEK_HOLE)
			if err == nil {
				end = next
			}
		}
	})

	return hole, end
}
