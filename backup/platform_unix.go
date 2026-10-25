//go:build !windows

package backup

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/user"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/gobackio/goback/proto"

	"golang.org/x/sys/unix"
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

// vanished reports whether an operation on an entry failed because the
// entry is gone, or is no longer the directory it was listed as.
func vanished(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR)
}

// entryError reports whether an open, read or listing failed for that entry
// alone: for lack of access, on a failing medium, through a stale network
// handle or at a path too long or too deep in links to reach.
func entryError(err error) bool {
	return IsPermissionError(err) || IsLockError(err) ||
		errors.Is(err, syscall.EIO) || errors.Is(err, syscall.ESTALE) ||
		errors.Is(err, syscall.ENAMETOOLONG) || errors.Is(err, syscall.ELOOP)
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

// openLocalRoot opens dir, the root of a walk on the local disk, as the
// os.Root the walk lists and opens everything in.
func openLocalRoot(dir string) (*os.Root, error) {
	return os.OpenRoot(dir)
}

// listIn lists the directory at rel in root sorted by name, each entry
// stat'ed relative to the directory listed rather than by path, and
// returns that directory's stat.
func listIn(root *os.Root, rel string) ([]os.DirEntry, os.FileInfo, error) {
	dir, err := openDirIn(root, rel)
	if err != nil {
		return nil, nil, err
	}
	defer dir.Close()

	self, err := dir.Stat()
	if err != nil {
		return nil, nil, err
	}

	entries, err := dir.ReadDir(-1)
	if err != nil {
		return nil, nil, err
	}

	slices.SortFunc(entries, func(a, b os.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })

	return entries, self, nil
}

// openDirIn opens the directory at rel in root without waiting on a FIFO
// there. It fails with errVanished when rel, or a directory on the way to
// it, is no longer a directory.
func openDirIn(root *os.Root, rel string) (*os.File, error) {
	if rel == "" {
		rel = "."
	}

	dir, err := root.OpenFile(rel, os.O_RDONLY|syscall.O_DIRECTORY, 0)
	if err != nil && !vanished(err) && !entryError(err) && moved(root, rel) {
		return nil, fmt.Errorf("%w: %v", errVanished, err)
	}

	return dir, err
}

// moved reports whether rel, or a directory on the way to it, is no longer
// a directory in root, as when one is now a symlink leading out of it.
func moved(root *os.Root, rel string) bool {
	prefix := ""

	for _, part := range strings.Split(rel, "/") {
		prefix = path.Join(prefix, part)

		info, err := root.Lstat(prefix)
		if err == nil && !info.IsDir() {
			return true
		}
	}

	return false
}

// openIn opens the file at rel in root to read, found at full on the disk,
// without following a symlink there or waiting on a FIFO, in the directory
// listed as parent. It fails with errVanished when rel's directory is no
// longer that one or the file is now a symlink.
func openIn(root *os.Root, rel, full string, parent os.FileInfo) (File, error) {
	dir, err := openDirIn(root, path.Dir(rel))
	if err != nil {
		return nil, err
	}
	defer dir.Close()

	self, err := dir.Stat()
	if err != nil {
		return nil, err
	}

	if !os.SameFile(self, parent) {
		return nil, fmt.Errorf("%w: the directory of %s is no longer the one listed", errVanished, full)
	}

	var fd int
	for {
		fd, err = unix.Openat(int(dir.Fd()), path.Base(rel), unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_NOCTTY|unix.O_CLOEXEC, 0)
		if !errors.Is(err, unix.EINTR) {
			break
		}
	}

	// FreeBSD refuses a symlink with EMLINK
	if errors.Is(err, unix.ELOOP) || errors.Is(err, unix.EMLINK) {
		return nil, fmt.Errorf("%w: %s is now a symlink", errVanished, full)
	}

	if err != nil {
		return nil, &os.PathError{Op: "openat", Path: full, Err: err}
	}

	if err := unix.SetNonblock(fd, false); err != nil {
		_ = unix.Close(fd)
		return nil, &os.PathError{Op: "fcntl", Path: full, Err: err}
	}

	return os.NewFile(uintptr(fd), full), nil
}

// recordPlatform adds what a stat holds beyond the portable fields, which
// on Unix is nothing yet.
func recordPlatform(*proto.FileInfo, os.FileInfo) {}

// openShared opens path to read.
func openShared(path string) (*os.File, error) {
	return os.Open(path)
}
