//go:build linux

package backup

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestTimestampsFollowTheMountsFilesystem(t *testing.T) {
	require.Equal(t, timestamps{resolution: 2 * time.Second}, linuxTimestamps(unix.MSDOS_SUPER_MAGIC, "vfat"))
	require.Equal(t, timestamps{resolution: 10 * time.Millisecond}, linuxTimestamps(unix.EXFAT_SUPER_MAGIC, "exfat"))
	require.Equal(t, timestamps{resolution: time.Second}, linuxTimestamps(unix.EXT4_SUPER_MAGIC, "ext3"))
	require.Equal(t, timestamps{resolution: time.Second}, linuxTimestamps(unix.EXT4_SUPER_MAGIC, ""))
	require.Equal(t, timestamps{}, linuxTimestamps(unix.EXT4_SUPER_MAGIC, "ext4"))
	require.Equal(t, timestamps{resolution: time.Second}, linuxTimestamps(hfsPlusMagic, "hfsplus"))
	require.Equal(t, timestamps{skew: networkSkew}, linuxTimestamps(unix.NFS_SUPER_MAGIC, "nfs4"))
	require.Equal(t, timestamps{skew: networkSkew}, linuxTimestamps(unix.CIFS_SUPER_MAGIC, "cifs"))
	require.Equal(t, timestamps{skew: networkSkew}, linuxTimestamps(unix.V9FS_MAGIC, "9p"))
	require.Equal(t, timestamps{}, linuxTimestamps(unix.XFS_SUPER_MAGIC, "xfs"))
	require.Equal(t, timestamps{}, linuxTimestamps(unix.TMPFS_MAGIC, "tmpfs"))
}

func TestTheMountTypeComesFromTheDevicesMountinfoLine(t *testing.T) {
	const mountinfo = `22 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw
36 22 8:17 / /old rw,noatime master:1 - ext3 /dev/sdb1 rw,errors=continue
37 22 0:32 / /tmp rw shared:2 - tmpfs tmpfs rw
`

	require.Equal(t, "ext4", mountType(strings.NewReader(mountinfo), unix.Mkdev(8, 1)))
	require.Equal(t, "ext3", mountType(strings.NewReader(mountinfo), unix.Mkdev(8, 17)))
	require.Empty(t, mountType(strings.NewReader(mountinfo), unix.Mkdev(8, 2)))
}
