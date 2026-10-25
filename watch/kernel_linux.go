package watch

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

// kernelSource is what the Linux backends share: a notification fd whose
// events are taken in as they arrive, and the changes taken in since the
// last Read. An error, such as an overflow, ends it.
type kernelSource struct {
	backend string
	root    string
	id      identity
	// fd is file's; it is only used while rc holds it
	fd   int
	file *os.File
	rc   syscall.RawConn
	done chan struct{}
	// take takes in the events the kernel queued, with mtx held
	take func()
	// check, with mtx held, reports a change around the root that puts
	// every position in doubt
	check func() error

	mtx     sync.Mutex
	stream  string
	seq     int64
	end     int64
	ready   bool
	err     error
	pending *Changes
	// linked are the files checked for other hard links since the last
	// Read
	linked map[string]bool
	buf    []byte
}

// identity tells a directory from another that takes its path.
type identity struct{ dev, ino uint64 }

func identify(path string) (identity, error) {
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		return identity{}, err
	}

	if st.Mode&unix.S_IFMT != unix.S_IFDIR {
		return identity{}, fmt.Errorf("%s is not a directory", path)
	}

	return identity{dev: uint64(st.Dev), ino: st.Ino}, nil
}

func newKernelSource(backend, root string, id identity, fd int) (*kernelSource, error) {
	file := os.NewFile(uintptr(fd), backend)

	rc, err := file.SyscallConn()
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	b := make([]byte, 8)
	_, _ = rand.Read(b)

	return &kernelSource{
		backend: backend, root: root, id: id, fd: fd, file: file, rc: rc, done: make(chan struct{}),
		stream: backend + "-" + hex.EncodeToString(b), seq: 1, end: 1,
		pending: NewChanges(), linked: map[string]bool{}, buf: make([]byte, 64<<10),
	}, nil
}

func (s *kernelSource) Backend() string { return s.backend }

// follow takes in events as they arrive until the source is closed or
// ends.
func (s *kernelSource) follow() {
	defer close(s.done)

	_ = s.rc.Read(func(uintptr) bool {
		s.mtx.Lock()
		defer s.mtx.Unlock()

		s.take()

		return s.err != nil
	})
}

// fail ends the source for err; positions taken before are in doubt.
func (s *kernelSource) fail(err error) {
	if s.err == nil {
		s.err = err
	}
}

// read reads the queued events into buf, returning how many bytes, or 0
// once there are none.
func (s *kernelSource) read() int {
	for s.err == nil {
		n, err := unix.Read(s.fd, s.buf)
		switch {
		case errors.Is(err, unix.EINTR):
			continue
		case errors.Is(err, unix.EAGAIN):
			return 0
		case err != nil:
			s.fail(fmt.Errorf("%w: reading %s events: %w", ErrInDoubt, s.backend, err))
			return 0
		}

		return max(n, 0)
	}

	return 0
}

// changedFile records that the file at rel changed; a file with other
// hard links changes directories that hear nothing of it, so the whole
// tree counts as changed.
func (s *kernelSource) changedFile(rel string) {
	if linked, ok := s.linked[rel]; ok {
		if linked {
			s.pending.MarkTree("", s.seq)
		}

		return
	}

	if len(s.linked) >= MaxDirs {
		clear(s.linked)
	}

	var st unix.Stat_t
	linked := unix.Lstat(filepath.Join(s.root, rel), &st) == nil && st.Mode&unix.S_IFMT == unix.S_IFREG && st.Nlink > 1
	s.linked[rel] = linked

	if linked {
		s.pending.MarkTree("", s.seq)
	}
}

// sync takes in every event queued so far and checks the root is still
// the one followed.
func (s *kernelSource) sync() error {
	if err := s.rc.Control(func(uintptr) { s.take() }); err != nil {
		return fmt.Errorf("%w: %w", ErrInDoubt, err)
	}

	if id, err := identify(s.root); err != nil || id != s.id {
		s.fail(fmt.Errorf("%w: the root was moved, removed or unmounted", ErrInDoubt))
	}

	if s.check != nil && s.err == nil {
		if err := s.check(); err != nil {
			s.fail(err)
		}
	}

	switch {
	case s.err != nil:
		return s.err
	case !s.ready:
		return ErrStarting
	}

	return nil
}

func (s *kernelSource) Now() (Position, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if err := s.sync(); err != nil {
		return Position{}, err
	}

	s.seq++

	return Position{Stream: s.stream, Seq: s.seq}, nil
}

func (s *kernelSource) Read(from Position) (*Changes, Position, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if err := s.sync(); err != nil {
		return nil, Position{}, err
	}

	if from.Stream != s.stream || from.Seq < s.end {
		return nil, Position{}, fmt.Errorf("%w: the position is from before %s started following", ErrInDoubt, s.backend)
	}

	changes := s.pending
	s.pending = NewChanges()
	clear(s.linked)

	for rel, why := range changes.Unwatched {
		s.pending.Unwatch(rel, why)
	}

	s.end = s.seq

	return changes, Position{Stream: s.stream, Seq: s.seq}, nil
}

func (s *kernelSource) Close() error {
	err := s.file.Close()
	<-s.done

	return err
}
