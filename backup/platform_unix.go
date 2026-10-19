//go:build !windows

package backup

import (
	"errors"
	"io"
	"os"
	"os/user"
	"strconv"
	"sync"
	"syscall"

	"github.com/gobackio/goback/proto"
)

// IsPermissionError reports whether an open or list failed for lack of access.
func IsPermissionError(err error) bool {
	return errors.Is(err, os.ErrPermission)
}

// IsLockError reports whether a read failed because another process holds
// the file; never on Unix.
func IsLockError(err error) bool {
	return false
}

// lockedElsewhere reports whether another process holds a POSIX record
// lock on the file, as a Java FileChannel lock is.
func lockedElsewhere(path string) bool {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return false
	}
	defer file.Close()

	lock := syscall.Flock_t{Type: syscall.F_WRLCK, Whence: int16(io.SeekStart)}

	return syscall.FcntlFlock(file.Fd(), syscall.F_SETLK, &lock) != nil
}

var (
	ownerMtx   sync.Mutex
	userNames  = map[uint32]string{}
	groupNames = map[uint32]string{}
)

// OwnerNames resolves the owning user and group names, empty when unknown.
func OwnerNames(info os.FileInfo) (string, string) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "", ""
	}

	ownerMtx.Lock()
	defer ownerMtx.Unlock()

	userName, ok := userNames[stat.Uid]
	if !ok {
		if u, err := user.LookupId(strconv.FormatUint(uint64(stat.Uid), 10)); err == nil {
			userName = u.Username
		}
		userNames[stat.Uid] = userName
	}

	groupName, ok := groupNames[stat.Gid]
	if !ok {
		if g, err := user.LookupGroupId(strconv.FormatUint(uint64(stat.Gid), 10)); err == nil {
			groupName = g.Name
		}
		groupNames[stat.Gid] = groupName
	}

	return userName, groupName
}

// FileIdentity returns the change time and inode, the signals the stat
// cache adds on top of mtime and size.
func FileIdentity(info os.FileInfo) (ctimeNs int64, inode uint64) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0
	}

	return ctime(stat), stat.Ino
}

var localTraits = Traits{Identity: true, Reread: true}

func localReadDir(dir string) ([]os.DirEntry, error) {
	return os.ReadDir(dir)
}

// recordPlatform adds what a stat holds beyond the portable fields, which
// on Unix is nothing yet.
func recordPlatform(*proto.FileInfo, os.FileInfo) {}
