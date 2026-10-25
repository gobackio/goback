//go:build !windows && !linux && !darwin && !freebsd

package backup

import "os"

func deviceOf(os.FileInfo) uint64 {
	return 0
}

func timestampsAt(string) (timestamps, bool) {
	return timestamps{}, true
}
