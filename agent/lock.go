package agent

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gobackio/goback/backup/lockfile"
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

	unlock, err := lockfile.Lock(filepath.Join(dir, "lock"))
	if errors.Is(err, lockfile.ErrLocked) {
		return nil, fmt.Errorf("%w: %s", ErrLocked, dir)
	}

	return unlock, err
}
