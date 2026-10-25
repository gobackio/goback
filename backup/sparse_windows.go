//go:build windows

package backup

import (
	"errors"
	"math"
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// isSparse reports whether the file carries the sparse attribute; only
// such a file can have unallocated ranges.
func isSparse(info os.FileInfo) bool {
	switch stat := info.Sys().(type) {
	case *syscall.Win32FileAttributeData:
		return stat.FileAttributes&windows.FILE_ATTRIBUTE_SPARSE_FILE != 0
	case *fileStatInformation:
		return stat.FileAttributes&windows.FILE_ATTRIBUTE_SPARSE_FILE != 0
	}

	return false
}

// markSparse marks file sparse when it is a local file, so the ranges a
// restore leaves unwritten take no space.
func markSparse(file any) {
	local, ok := file.(interface{ Fd() uintptr })
	if !ok {
		return
	}

	var returned uint32
	_ = windows.DeviceIoControl(windows.Handle(local.Fd()), windows.FSCTL_SET_SPARSE, nil, 0, nil, 0, &returned, nil)
}

// allocatedRange is a FILE_ALLOCATED_RANGE_BUFFER.
type allocatedRange struct {
	Offset int64
	Length int64
}

// nextExtent reports whether the file holds a hole or data at off and
// where that extent ends; data to math.MaxInt64 when the file system
// cannot say.
func nextExtent(conn syscall.RawConn, off, size int64) (hole bool, end int64) {
	end = math.MaxInt64

	_ = conn.Control(func(fd uintptr) {
		query := allocatedRange{Offset: off, Length: size - off}

		var ranges [16]allocatedRange
		var returned uint32

		err := windows.DeviceIoControl(windows.Handle(fd), windows.FSCTL_QUERY_ALLOCATED_RANGES,
			(*byte)(unsafe.Pointer(&query)), uint32(unsafe.Sizeof(query)),
			(*byte)(unsafe.Pointer(&ranges[0])), uint32(unsafe.Sizeof(ranges)), &returned, nil)
		if err != nil && !errors.Is(err, windows.ERROR_MORE_DATA) {
			return
		}

		count := int(returned / uint32(unsafe.Sizeof(ranges[0])))
		for _, r := range ranges[:count] {
			if r.Offset+r.Length <= off {
				continue
			}

			if r.Offset > off {
				hole, end = true, r.Offset
			} else {
				end = r.Offset + r.Length
			}

			return
		}

		if err == nil {
			hole, end = true, size
		}
	})

	return hole, end
}
