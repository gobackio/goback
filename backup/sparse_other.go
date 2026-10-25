//go:build !windows && !linux && !darwin && !freebsd

package backup

import (
	"math"
	"os"
	"syscall"
)

func isSparse(os.FileInfo) bool {
	return false
}

func markSparse(any) {}

func nextExtent(syscall.RawConn, int64, int64) (bool, int64) {
	return false, math.MaxInt64
}
