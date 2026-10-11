package mysql

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

const (
	// receivingDir is the spool's directory for the binlog being received.
	receivingDir = ".receiving"
	// chainFile records the chain the spool's binlogs belong to.
	chainFile = ".chain"
)

// Spool is the directory a receiver keeps binlogs in until a commit holds
// them: whole binlogs under their names, and the one being received in
// .receiving. A binlog appears under its name only once it is whole.
type Spool struct {
	Dir string
}

// Files lists the whole binlogs in the spool, oldest first.
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

	slices.SortFunc(names, compareBinlogs)

	return names, nil
}

func (s Spool) path(name string) string {
	return filepath.Join(s.Dir, name)
}

func (s Spool) receiving() string {
	return filepath.Join(s.Dir, receivingDir)
}

// Remove deletes binlogs a commit holds.
func (s Spool) Remove(names ...string) error {
	for _, name := range names {
		if err := os.Remove(s.path(name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}

	return syncDir(s.Dir)
}

// clear deletes every binlog of the spool, whole or being received.
func (s Spool) clear() error {
	files, err := s.Files()
	if err != nil {
		return err
	}

	if err := os.RemoveAll(s.receiving()); err != nil {
		return err
	}

	return s.Remove(files...)
}

// chain reads the chain the spool's binlogs belong to; ok is false when
// the spool records none.
func (s Spool) chain() (int, bool, error) {
	data, err := os.ReadFile(s.path(chainFile))
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}

	if err != nil {
		return 0, false, err
	}

	n, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || n < 1 {
		return 0, false, fmt.Errorf("the spool's %s holds %q", chainFile, data)
	}

	return n, true, nil
}

// setChain records the chain the spool's binlogs belong to.
func (s Spool) setChain(n int) error {
	tmp := s.path(chainFile + ".tmp")

	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}

	_, err = f.WriteString(strconv.Itoa(n) + "\n")
	if err == nil {
		err = f.Sync()
	}

	if closeErr := f.Close(); err == nil {
		err = closeErr
	}

	if err != nil {
		return err
	}

	if err := os.Rename(tmp, s.path(chainFile)); err != nil {
		return err
	}

	return syncDir(s.Dir)
}
