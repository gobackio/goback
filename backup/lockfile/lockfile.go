// Package lockfile takes exclusive locks on files, so a second process on
// the same directory refuses to start.
package lockfile

import (
	"errors"
	"os"
)

// ErrLocked is returned by Lock when another process holds the lock.
var ErrLocked = errors.New("locked by another process")

// Lock takes the exclusive lock on the file at path, creating it, without
// waiting. The returned func releases it.
func Lock(path string) (func(), error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}

	if err := lockFile(f); err != nil {
		_ = f.Close()
		return nil, ErrLocked
	}

	return func() {
		_ = unlockFile(f)
		_ = f.Close()
	}, nil
}
