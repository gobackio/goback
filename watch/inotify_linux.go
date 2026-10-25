package watch

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

const inotifyMask = unix.IN_CREATE | unix.IN_DELETE | unix.IN_MODIFY | unix.IN_ATTRIB | unix.IN_CLOSE_WRITE |
	unix.IN_MOVED_FROM | unix.IN_MOVED_TO | unix.IN_DELETE_SELF | unix.IN_MOVE_SELF |
	unix.IN_ONLYDIR | unix.IN_DONT_FOLLOW | unix.IN_EXCL_UNLINK

// addWatch is inotify_add_watch; tests stand in for it.
var addWatch = unix.InotifyAddWatch

// inotifySource follows a tree with a watch on each of its directories.
// It sets them up in the background, listing the tree, and has no
// position until they are all set. An overflow, or the root moving away,
// ends it.
type inotifySource struct {
	root string
	id   identity
	// fd is file's; it is only used while rc holds it
	fd   int
	file *os.File
	rc   syscall.RawConn
	done chan struct{}

	mtx     sync.Mutex
	stream  string
	seq     int64
	end     int64
	ready   bool
	err     error
	pending *Changes
	wds     map[int32]string
	paths   map[string]int32
	queue   []string
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

func newStream(backend string) string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)

	return backend + "-" + hex.EncodeToString(b)
}

func openInotify(root string) (*inotifySource, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}

	id, err := identify(root)
	if err != nil {
		return nil, err
	}

	fd, err := unix.InotifyInit1(unix.IN_NONBLOCK | unix.IN_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("%w: inotify: %w", ErrUnavailable, err)
	}

	file := os.NewFile(uintptr(fd), "inotify")

	rc, err := file.SyscallConn()
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	s := &inotifySource{
		root: root, id: id, fd: fd, file: file, rc: rc, done: make(chan struct{}),
		stream: newStream("inotify"), seq: 1, end: 1,
		pending: NewChanges(), wds: map[int32]string{}, paths: map[string]int32{},
		queue: []string{""}, linked: map[string]bool{}, buf: make([]byte, 64<<10),
	}

	go s.run()

	return s, nil
}

func (s *inotifySource) Backend() string { return "inotify" }

// run sets up the watches, taking in events between directories, then
// takes in events until the source is closed or ends.
func (s *inotifySource) run() {
	defer close(s.done)

	for {
		var more bool

		err := s.rc.Control(func(uintptr) {
			s.mtx.Lock()
			defer s.mtx.Unlock()

			s.drain()
			s.watchQueued(64)

			more = len(s.queue) > 0 && s.err == nil
			if !more && s.err == nil {
				s.ready = true
			}
		})
		if err != nil || !more {
			break
		}
	}

	_ = s.rc.Read(func(uintptr) bool {
		s.mtx.Lock()
		defer s.mtx.Unlock()

		s.drain()
		s.watchQueued(-1)

		return s.err != nil
	})
}

// fail ends the source for err; positions taken before are in doubt.
func (s *inotifySource) fail(err error) {
	if s.err == nil {
		s.err = err
	}
}

// drain takes in every event the kernel queued.
func (s *inotifySource) drain() {
	for s.err == nil {
		n, err := unix.Read(s.fd, s.buf)
		switch {
		case errors.Is(err, unix.EINTR):
			continue
		case errors.Is(err, unix.EAGAIN):
			return
		case err != nil:
			s.fail(fmt.Errorf("%w: reading inotify events: %w", ErrInDoubt, err))
			return
		case n <= 0:
			return
		}

		for off := 0; off+unix.SizeofInotifyEvent <= n; {
			ev := (*unix.InotifyEvent)(unsafe.Pointer(&s.buf[off]))
			name := s.buf[off+unix.SizeofInotifyEvent : off+unix.SizeofInotifyEvent+int(ev.Len)]
			if i := bytes.IndexByte(name, 0); i >= 0 {
				name = name[:i]
			}

			s.handle(ev.Wd, ev.Mask, string(name))
			off += unix.SizeofInotifyEvent + int(ev.Len)
		}
	}
}

func (s *inotifySource) handle(wd int32, mask uint32, name string) {
	if mask&unix.IN_Q_OVERFLOW != 0 {
		s.fail(fmt.Errorf("%w: the inotify event queue overflowed", ErrInDoubt))
		return
	}

	dir, ok := s.wds[wd]
	if !ok {
		return
	}

	switch {
	case mask&unix.IN_IGNORED != 0:
		delete(s.wds, wd)
		if s.paths[dir] == wd {
			delete(s.paths, dir)
		}

		if dir == "" {
			s.fail(fmt.Errorf("%w: the root was moved, removed or unmounted", ErrInDoubt))
		}

		return
	case mask&unix.IN_UNMOUNT != 0:
		s.fail(fmt.Errorf("%w: a filesystem at %q was unmounted", ErrInDoubt, "/"+dir))
		return
	case mask&(unix.IN_DELETE_SELF|unix.IN_MOVE_SELF) != 0:
		if dir == "" {
			s.fail(fmt.Errorf("%w: the root was moved or removed", ErrInDoubt))
		}

		return
	case name == "":
		s.pending.Mark(dir, s.seq)
		return
	}

	rel := join(dir, name)
	s.pending.Mark(dir, s.seq)

	if mask&unix.IN_ISDIR != 0 {
		switch {
		case mask&(unix.IN_CREATE|unix.IN_MOVED_TO) != 0:
			s.pending.MarkTree(rel, s.seq)
			s.queue = append(s.queue, rel)
		case mask&unix.IN_MOVED_FROM != 0:
			s.unwatchUnder(rel)
		}

		return
	}

	if mask&(unix.IN_MODIFY|unix.IN_ATTRIB|unix.IN_CLOSE_WRITE|unix.IN_CREATE|unix.IN_MOVED_TO) != 0 && s.hardLinked(rel) {
		s.pending.MarkTree("", s.seq)
	}
}

// hardLinked reports whether the file at rel has other hard links, whose
// directories don't hear of its changes.
func (s *inotifySource) hardLinked(rel string) bool {
	if linked, ok := s.linked[rel]; ok {
		return linked
	}

	if len(s.linked) >= MaxDirs {
		clear(s.linked)
	}

	var st unix.Stat_t
	linked := unix.Lstat(filepath.Join(s.root, rel), &st) == nil && st.Mode&unix.S_IFMT == unix.S_IFREG && st.Nlink > 1
	s.linked[rel] = linked

	return linked
}

// unwatchUnder removes the watches at and under rel, which moved away.
func (s *inotifySource) unwatchUnder(rel string) {
	for p, wd := range s.paths {
		if p == rel || strings.HasPrefix(p, rel+"/") {
			_, _ = unix.InotifyRmWatch(s.fd, uint32(wd))
			delete(s.paths, p)
			delete(s.wds, wd)
		}
	}
}

// watchQueued watches and lists up to limit queued directories, all of
// them when limit is negative, queueing the directories they hold.
func (s *inotifySource) watchQueued(limit int) {
	for n := 0; len(s.queue) > 0 && s.err == nil && (limit < 0 || n < limit); n++ {
		rel := s.queue[len(s.queue)-1]
		s.queue = s.queue[:len(s.queue)-1]

		path := filepath.Join(s.root, rel)

		wd, err := addWatch(s.fd, path, inotifyMask)
		switch {
		case errors.Is(err, unix.ENOENT), errors.Is(err, unix.ENOTDIR):
			continue
		case errors.Is(err, unix.ENOSPC):
			s.pending.Unwatch(rel, "the inotify watch limit, fs.inotify.max_user_watches, is reached")
			continue
		case err != nil:
			s.pending.Unwatch(rel, err.Error())
			continue
		}

		if other, ok := s.wds[int32(wd)]; ok && other != rel {
			if _, err := os.Lstat(filepath.Join(s.root, other)); err == nil {
				s.pending.Unwatch(rel, fmt.Sprintf("the directory is also at %q", "/"+other))
				continue
			}

			delete(s.paths, other)
		}

		s.wds[int32(wd)] = rel
		s.paths[rel] = int32(wd)

		if rel == "" {
			if id, err := identify(path); err != nil || id != s.id {
				s.fail(fmt.Errorf("%w: the root was moved or replaced", ErrInDoubt))
				return
			}
		}

		subdirs, err := listDirs(path)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				s.pending.Unwatch(rel, err.Error())
			}

			continue
		}

		for _, name := range subdirs {
			s.queue = append(s.queue, join(rel, name))
		}
	}
}

// listDirs lists the directories in dir, symlinks left out.
func listDirs(dir string) ([]string, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var dirs []string

	for {
		entries, err := f.ReadDir(1024)
		for _, e := range entries {
			if e.IsDir() {
				dirs = append(dirs, e.Name())
			}
		}

		if errors.Is(err, io.EOF) {
			return dirs, nil
		}

		if err != nil {
			return dirs, err
		}
	}
}

// sync takes in every event queued so far and checks the root is still
// the one followed.
func (s *inotifySource) sync() error {
	err := s.rc.Control(func(uintptr) {
		s.drain()
		s.watchQueued(-1)
	})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInDoubt, err)
	}

	if id, err := identify(s.root); err != nil || id != s.id {
		s.fail(fmt.Errorf("%w: the root was moved, removed or unmounted", ErrInDoubt))
	}

	switch {
	case s.err != nil:
		return s.err
	case !s.ready:
		return ErrStarting
	}

	return nil
}

func (s *inotifySource) Now() (Position, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if err := s.sync(); err != nil {
		return Position{}, err
	}

	s.seq++

	return Position{Stream: s.stream, Seq: s.seq}, nil
}

func (s *inotifySource) Read(from Position) (*Changes, Position, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if err := s.sync(); err != nil {
		return nil, Position{}, err
	}

	if from.Stream != s.stream || from.Seq < s.end {
		return nil, Position{}, fmt.Errorf("%w: the position is from before inotify started following", ErrInDoubt)
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

func (s *inotifySource) Close() error {
	err := s.file.Close()
	<-s.done

	return err
}
