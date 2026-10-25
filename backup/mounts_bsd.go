//go:build darwin || freebsd

package backup

import (
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func deviceOf(info os.FileInfo) uint64 {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0
	}

	return uint64(stat.Dev)
}

func timestampsAt(path string) (timestamps, bool) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return timestamps{}, false
	}

	return bsdTimestamps(unix.ByteSliceToString(stat.Fstypename[:]), uint64(stat.Flags)&unix.MNT_LOCAL != 0), true
}

// bsdTimestamps is what the filesystem named fsType holds, on a local
// mount or a network one.
func bsdTimestamps(fsType string, local bool) timestamps {
	var times timestamps

	switch fsType {
	case "msdos", "msdosfs":
		times.resolution = 2 * time.Second
	case "exfat":
		times.resolution = 10 * time.Millisecond
	case "hfs", "ext2fs":
		times.resolution = time.Second
	}

	if !local {
		times.skew = networkSkew
	}

	return times
}
