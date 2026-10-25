package backup

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// linuxFlagsKept are the inode flags a walk records: those chattr sets,
// without the ones a filesystem keeps for itself, such as extents, or that
// only an empty file or directory takes, such as casefold.
const linuxFlagsKept = 0x0000_0001 | // secure deletion
	0x0000_0002 | // undelete
	0x0000_0004 | // compress
	0x0000_0008 | // synchronous updates
	0x0000_0010 | // immutable
	0x0000_0020 | // append only
	0x0000_0040 | // no dump
	0x0000_0080 | // no atime updates
	0x0000_0400 | // no compression
	0x0000_4000 | // data journaling
	0x0000_8000 | // no tail merging
	0x0001_0000 | // synchronous directory updates
	0x0002_0000 | // top of directory hierarchies
	0x0080_0000 | // no copy on write
	0x0200_0000 | // direct access
	0x2000_0000 // project id inheritance

var xattrGone = unix.ENODATA

// flagsOf reads the inode flags of the entry open as fd, 0 where its
// filesystem keeps none.
func flagsOf(fd int, _ os.FileInfo) (linux, darwin uint32, err error) {
	flags, err := unix.IoctlGetUint32(fd, unix.FS_IOC_GETFLAGS)
	if noFlags(err) {
		return 0, 0, nil
	}

	return flags & linuxFlagsKept, 0, err
}

func noFlags(err error) bool {
	return errors.Is(err, unix.ENOTTY) || errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOSYS)
}

func platformFlags(linux, darwin uint32) (uint32, error) {
	if linux == 0 && darwin != 0 {
		return 0, fmt.Errorf("%w: the flags are macOS's", errors.ErrUnsupported)
	}

	return linux, nil
}

// setFlags gives the entry open as fd the recorded flags, keeping the ones
// a walk does not record.
func setFlags(fd int, flags uint32) error {
	current, err := unix.IoctlGetUint32(fd, unix.FS_IOC_GETFLAGS)
	if noFlags(err) {
		return fmt.Errorf("%w: %v", errors.ErrUnsupported, err)
	}

	if err != nil {
		return err
	}

	return unix.IoctlSetPointerInt(fd, unix.FS_IOC_SETFLAGS, int(current&^linuxFlagsKept|flags&linuxFlagsKept))
}
