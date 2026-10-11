// Package postgres backs up Postgres clusters: base backups taken with
// pg_basebackup and the WAL archive, each into a set of its own.
package postgres

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// ErrConflict is returned by Spool.Add for a file the spool already holds
// with other contents.
var ErrConflict = errors.New("the spool holds a different file of that name")

const spoolingSuffix = ".spooling"

// Spool is the directory archive_command copies finished WAL files into,
// and the WAL backup commits from. A file in it is whole: it appears under
// its name only once its contents are on disk.
type Spool struct {
	Dir string
}

// Add copies the file at path into the spool under name, as
// archive_command does with %p and %f. Adding a file the spool already
// holds with the same contents succeeds, so Postgres may retry.
func (s Spool) Add(path, name string) error {
	if name == "" || name != filepath.Base(name) || strings.HasSuffix(name, spoolingSuffix) || strings.HasPrefix(name, ".") {
		return fmt.Errorf("%q is not a WAL file name", name)
	}

	dst := filepath.Join(s.Dir, name)

	if held, err := os.ReadFile(dst); err == nil {
		incoming, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		if !bytes.Equal(held, incoming) {
			return fmt.Errorf("%w: %s", ErrConflict, name)
		}

		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	tmp := dst + spoolingSuffix
	if err := copyFile(path, tmp); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	return syncDir(s.Dir)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}

	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}

	if err := out.Sync(); err != nil {
		_ = out.Close()
		return err
	}

	return out.Close()
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
		name := e.Name()
		if e.Type().IsRegular() && !strings.HasSuffix(name, spoolingSuffix) && !strings.HasPrefix(name, ".") {
			names = append(names, name)
		}
	}

	sort.Strings(names)

	return names, nil
}

// Open opens a whole file of the spool.
func (s Spool) Open(name string) (*os.File, error) {
	return os.Open(s.path(name))
}

func (s Spool) path(name string) string {
	return filepath.Join(s.Dir, name)
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
