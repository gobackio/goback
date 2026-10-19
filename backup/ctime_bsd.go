//go:build darwin || freebsd || netbsd

package backup

import "syscall"

func ctime(stat *syscall.Stat_t) int64 {
	return stat.Ctimespec.Sec*1e9 + stat.Ctimespec.Nsec
}
