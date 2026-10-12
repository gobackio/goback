// Package postgres backs up Postgres clusters: base backups, and the WAL a
// receiver streams through a replication slot, each into a set of its own.
package postgres

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// ErrConflict is returned when a file is received again under a name the
// spool holds with other contents.
var ErrConflict = errors.New("the spool holds a different file of that name")

// Spool is the directory a receiver keeps WAL in until a commit holds it:
// whole files under their names, and the segment being received in
// .receiving. A file appears under its name only once it is whole.
type Spool struct {
	Dir string
}

// Files lists the whole files in the spool, in name order, which is WAL
// order within a timeline.
func (s Spool) Files() ([]string, error) {
	entries, err := os.ReadDir(s.Dir)
	if err != nil {
		return nil, err
	}

	var names []string
	for _, e := range entries {
		if e.Type().IsRegular() && !strings.HasPrefix(e.Name(), ".") {
			names = append(names, e.Name())
		}
	}

	sort.Strings(names)

	return names, nil
}

func (s Spool) path(name string) string {
	return filepath.Join(s.Dir, name)
}

// adopt renames the whole file at src, in the receiving directory, into
// the spool as name. A file the spool already holds under that name must
// be the same.
func (s Spool) adopt(src, name string) error {
	dst := s.path(name)

	if held, err := os.ReadFile(dst); err == nil {
		received, err := os.ReadFile(src)
		if err != nil {
			return err
		}

		if !bytes.Equal(held, received) {
			return fmt.Errorf("%w: %s", ErrConflict, name)
		}

		return os.Remove(src)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	if err := os.Rename(src, dst); err != nil {
		return err
	}

	if err := syncDir(s.receiving()); err != nil {
		return err
	}

	return syncDir(s.Dir)
}

// Remove deletes files a backup has committed.
func (s Spool) Remove(names ...string) error {
	for _, name := range names {
		if err := os.Remove(filepath.Join(s.Dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}

	return syncDir(s.Dir)
}
