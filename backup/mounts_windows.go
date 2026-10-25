//go:build windows

package backup

import (
	"os"
	"strings"
	"time"

	"golang.org/x/sys/windows"
)

// deviceOf is the same for every file: a walk does not enter a mount
// point, so it stays on the volume of its root.
func deviceOf(os.FileInfo) uint64 {
	return 0
}

func timestampsAt(path string) (timestamps, bool) {
	name, err := windows.UTF16FromString(extended(path))
	if err != nil {
		return timestamps{}, false
	}

	volume := make([]uint16, len(name)+1)
	if err := windows.GetVolumePathName(&name[0], &volume[0], uint32(len(volume))); err != nil {
		return timestamps{}, false
	}

	fsName := make([]uint16, windows.MAX_PATH+1)
	if err := windows.GetVolumeInformation(&volume[0], nil, 0, nil, nil, nil, &fsName[0], uint32(len(fsName))); err != nil {
		return timestamps{}, false
	}

	remote := windows.GetDriveType(&volume[0]) == windows.DRIVE_REMOTE

	return windowsTimestamps(windows.UTF16ToString(fsName), remote), true
}

// windowsTimestamps is what the filesystem GetVolumeInformation names
// holds, on a local volume or a network share.
func windowsTimestamps(fsName string, remote bool) timestamps {
	var times timestamps

	switch name := strings.ToUpper(fsName); {
	case strings.HasPrefix(name, "FAT"):
		times.resolution = 2 * time.Second
	case name == "EXFAT":
		times.resolution = 10 * time.Millisecond
	}

	if remote {
		times.skew = networkSkew
	}

	return times
}
