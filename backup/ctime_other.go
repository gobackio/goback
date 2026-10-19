//go:build !windows && !darwin && !freebsd && !netbsd

package backup

import "syscall"

func ctime(stat *syscall.Stat_t) int64 {
	return int64(stat.Ctim.Sec)*1e9 + int64(stat.Ctim.Nsec)
}
