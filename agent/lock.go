package agent

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ErrLocked is returned by Lock when another agent holds the state dir.
var ErrLocked = errors.New("another goback agent uses this state dir")

// Lock creates dir and takes the exclusive lock on its lock file, so a
// second agent on the same state dir refuses to start. The returned func
// releases it.
func Lock(dir string) (func(), error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}

	if err := lockFile(f); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("%w: %s", ErrLocked, dir)
	}

	return func() {
		_ = unlockFile(f)
		_ = f.Close()
	}, nil
}
