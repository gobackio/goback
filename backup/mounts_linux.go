//go:build linux

package backup

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
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

	fstype := ""

	var file unix.Stat_t
	if uint32(stat.Type) == unix.EXT4_SUPER_MAGIC && unix.Stat(path, &file) == nil {
		if mountinfo, err := os.Open("/proc/self/mountinfo"); err == nil {
			fstype = mountType(mountinfo, uint64(file.Dev))
			_ = mountinfo.Close()
		}
	}

	return linuxTimestamps(uint32(stat.Type), fstype), true
}

// mountType is the filesystem type mountinfo, in the format of
// /proc/self/mountinfo, gives the mount of device; empty when it has none.
func mountType(mountinfo io.Reader, device uint64) string {
	want := fmt.Sprintf("%d:%d", unix.Major(device), unix.Minor(device))

	scanner := bufio.NewScanner(mountinfo)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 3 || fields[2] != want {
			continue
		}

		for i, field := range fields {
			if field == "-" && i+1 < len(fields) {
				return fields[i+1]
			}
		}
	}

	return ""
}

const (
	hfsMagic     = 0x4244
	hfsPlusMagic = 0x482b
	gfs2Magic    = 0x01161970
	lustreMagic  = 0x0bd00bd0
)

// linuxTimestamps is what the filesystem with the statfs f_type magic
// holds. ext2, ext3 and ext4 share theirs, so fstype, the mount's type,
// tells ext4 apart; one it does not name counts as ext3. FUSE counts as a
// network filesystem.
func linuxTimestamps(magic uint32, fstype string) timestamps {
	switch magic {
	case unix.MSDOS_SUPER_MAGIC:
		return timestamps{resolution: 2 * time.Second}
	case unix.EXFAT_SUPER_MAGIC:
		return timestamps{resolution: 10 * time.Millisecond}
	case unix.EXT4_SUPER_MAGIC:
		if fstype == "ext4" {
			return timestamps{}
		}

		return timestamps{resolution: time.Second}
	case hfsMagic, hfsPlusMagic:
		return timestamps{resolution: time.Second}
	case unix.NFS_SUPER_MAGIC, unix.SMB_SUPER_MAGIC, unix.SMB2_SUPER_MAGIC, unix.CIFS_SUPER_MAGIC,
		unix.V9FS_MAGIC, unix.FUSE_SUPER_MAGIC, unix.CEPH_SUPER_MAGIC, unix.AFS_SUPER_MAGIC,
		unix.AFS_FS_MAGIC, unix.CODA_SUPER_MAGIC, unix.NCP_SUPER_MAGIC, unix.OCFS2_SUPER_MAGIC,
		gfs2Magic, lustreMagic:
		return timestamps{skew: networkSkew}
	}

	return timestamps{}
}
