//go:build windows

package backup

import "golang.org/x/sys/windows"

// ioError is how a read from a failing medium fails.
var ioError = windows.ERROR_CRC
