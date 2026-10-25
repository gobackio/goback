//go:build windows

package backup

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTimestampsFollowTheVolumesFilesystem(t *testing.T) {
	require.Equal(t, timestamps{resolution: 2 * time.Second}, windowsTimestamps("FAT32", false))
	require.Equal(t, timestamps{resolution: 2 * time.Second}, windowsTimestamps("FAT", false))
	require.Equal(t, timestamps{resolution: 10 * time.Millisecond}, windowsTimestamps("exFAT", false))
	require.Equal(t, timestamps{}, windowsTimestamps("NTFS", false))
	require.Equal(t, timestamps{}, windowsTimestamps("ReFS", false))
	require.Equal(t, timestamps{skew: networkSkew}, windowsTimestamps("NTFS", true))
}

func TestTheTempDirectorysVolumeIsFound(t *testing.T) {
	dir := t.TempDir()
	info, err := os.Stat(dir)
	require.NoError(t, err)

	times, ok := timestampsAt(dir)
	require.True(t, ok)
	require.Equal(t, times, newMountTimes().of(dir, info))
}
