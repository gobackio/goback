package backup

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// darwinFlagsKept are the file flags a walk records: those chflags sets,
// without the ones the system keeps for itself, such as compressed or
// dataless, which a restored file does not have.
const darwinFlagsKept = unix.UF_NODUMP | unix.UF_IMMUTABLE | unix.UF_APPEND | unix.UF_OPAQUE | unix.UF_HIDDEN |
	unix.SF_ARCHIVED | unix.SF_IMMUTABLE | unix.SF_APPEND | unix.SF_NOUNLINK

var xattrGone = unix.ENOATTR

func flagsOf(_ int, info os.FileInfo) (linux, darwin uint32, err error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, nil
	}

	return 0, stat.Flags & darwinFlagsKept, nil
}

func platformFlags(linux, darwin uint32) (uint32, error) {
	if darwin == 0 && linux != 0 {
		return 0, fmt.Errorf("%w: the flags are Linux's", errors.ErrUnsupported)
	}

	return darwin, nil
}

// setFlags gives the entry open as fd the recorded flags, keeping the ones
// a walk does not record.
func setFlags(fd int, flags uint32) error {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return err
	}

	return unix.Fchflags(fd, int(stat.Flags&^darwinFlagsKept|flags&darwinFlagsKept))
}
