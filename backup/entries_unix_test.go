//go:build !windows

package backup

import "syscall"

// ioError is how a read from a failing medium fails.
var ioError = syscall.EIO
