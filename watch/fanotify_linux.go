package watch

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"unsafe"

	"golang.org/x/sys/unix"
)

const fanotifyMask = unix.FAN_CREATE | unix.FAN_DELETE | unix.FAN_MOVED_FROM | unix.FAN_MOVED_TO |
	unix.FAN_MODIFY | unix.FAN_ATTRIB | unix.FAN_CLOSE_WRITE | unix.FAN_ONDIR

// fanotifySource follows the root's whole filesystem with one mark, each
// event naming its directory by a file handle and the entry by name, and
// keeps the events under the root. Other filesystems mounted under the
// root are left unwatched; a change to the mounts under the root ends it.
type fanotifySource struct {
	*kernelSource

	// mountFD opens the handles events carry
	mountFD int
	mounts  []string
	// dirs caches the path below the root of each directory handle seen,
	// or outside for one that isn't under it
	dirs map[string]string
}

const outside = "\x00"

func openFanotify(root string) (*fanotifySource, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}

	if root, err = filepath.EvalSymlinks(root); err != nil {
		return nil, err
	}

	id, err := identify(root)
	if err != nil {
		return nil, err
	}

	fd, err := unix.FanotifyInit(unix.FAN_CLASS_NOTIF|unix.FAN_CLOEXEC|unix.FAN_NONBLOCK|unix.FAN_REPORT_DFID_NAME, unix.O_RDONLY|unix.O_LARGEFILE)
	if err != nil {
		return nil, fmt.Errorf("%w: fanotify: %w", ErrUnavailable, err)
	}

	if err := unix.FanotifyMark(fd, unix.FAN_MARK_ADD|unix.FAN_MARK_FILESYSTEM, fanotifyMask, unix.AT_FDCWD, root); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("%w: marking the filesystem of %s for fanotify: %w", ErrUnavailable, root, err)
	}

	mountFD, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		_ = unix.Close(fd)
		return nil, err
	}

	if err := openableByHandle(mountFD); err != nil {
		_ = unix.Close(fd)
		_ = unix.Close(mountFD)

		return nil, fmt.Errorf("%w: opening files by handle: %w", ErrUnavailable, err)
	}

	mounts, err := mountsUnder(root)
	if err != nil {
		_ = unix.Close(fd)
		_ = unix.Close(mountFD)

		return nil, err
	}

	k, err := newKernelSource("fanotify", root, id, fd)
	if err != nil {
		_ = unix.Close(mountFD)
		return nil, err
	}

	s := &fanotifySource{kernelSource: k, mountFD: mountFD, mounts: mounts, dirs: map[string]string{}}
	k.take = s.drain
	k.check = s.checkMounts
	k.ready = true

	for _, m := range mounts {
		s.pending.Unwatch(m, "another filesystem is mounted there")
	}

	go s.follow()

	return s, nil
}

// openableByHandle checks the process may open files by handle, which
// takes CAP_DAC_READ_SEARCH.
func openableByHandle(dirFD int) error {
	h, _, err := unix.NameToHandleAt(dirFD, "", unix.AT_EMPTY_PATH)
	if err != nil {
		return err
	}

	fd, err := unix.OpenByHandleAt(dirFD, h, unix.O_PATH|unix.O_CLOEXEC)
	if err != nil {
		return err
	}

	return unix.Close(fd)
}

// mountsUnder lists the mount points below root, as paths below it.
func mountsUnder(root string) ([]string, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var mounts []string

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 1<<20)

	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 5 {
			continue
		}

		point := unescapeMount(fields[4])
		if strings.HasPrefix(point, root+"/") {
			mounts = append(mounts, point[len(root)+1:])
		}
	}

	if err := sc.Err(); err != nil {
		return nil, err
	}

	slices.Sort(mounts)

	return slices.Compact(mounts), nil
}

// unescapeMount undoes mountinfo's octal escapes, as \040 for a space.
func unescapeMount(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}

	var b strings.Builder

	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if n, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(n))
				i += 3

				continue
			}
		}

		b.WriteByte(s[i])
	}

	return b.String()
}

func (s *fanotifySource) checkMounts() error {
	mounts, err := mountsUnder(s.root)
	if err != nil {
		return fmt.Errorf("%w: reading the mounts: %w", ErrInDoubt, err)
	}

	if !slices.Equal(mounts, s.mounts) {
		return fmt.Errorf("%w: the filesystems mounted under the root changed", ErrInDoubt)
	}

	return nil
}

var metadataSize = int(unsafe.Sizeof(unix.FanotifyEventMetadata{}))

// drain takes in every event the kernel queued.
func (s *fanotifySource) drain() {
	for n := s.read(); n > 0; n = s.read() {
		for off := 0; off+metadataSize <= n && s.err == nil; {
			m := (*unix.FanotifyEventMetadata)(unsafe.Pointer(&s.buf[off]))
			if int(m.Event_len) < metadataSize || off+int(m.Event_len) > n {
				s.fail(fmt.Errorf("%w: a fanotify event is cut off", ErrInDoubt))
				return
			}

			if m.Fd >= 0 {
				_ = unix.Close(int(m.Fd))
			}

			switch {
			case m.Vers != unix.FANOTIFY_METADATA_VERSION:
				s.fail(fmt.Errorf("%w: fanotify events of version %d", ErrInDoubt, m.Vers))
			case m.Mask&unix.FAN_Q_OVERFLOW != 0:
				s.fail(fmt.Errorf("%w: the fanotify event queue overflowed", ErrInDoubt))
			default:
				s.event(m.Mask, s.buf[off+int(m.Metadata_len):off+int(m.Event_len)])
			}

			off += int(m.Event_len)
		}
	}
}

// event takes in one event from its info records.
func (s *fanotifySource) event(mask uint64, info []byte) {
	for len(info) >= 4 {
		typ, size := info[0], int(*(*uint16)(unsafe.Pointer(&info[2])))
		if size < 4 || size > len(info) {
			return
		}

		rec := info[4:size]
		info = info[size:]

		if typ != unix.FAN_EVENT_INFO_TYPE_DFID_NAME && typ != unix.FAN_EVENT_INFO_TYPE_DFID {
			continue
		}

		// fsid, then struct file_handle, then for DFID_NAME the name
		if len(rec) < 16 {
			continue
		}

		handleBytes := int(*(*uint32)(unsafe.Pointer(&rec[8])))
		handleType := *(*int32)(unsafe.Pointer(&rec[12]))
		if 16+handleBytes > len(rec) {
			continue
		}

		handle := rec[16 : 16+handleBytes]

		var name string
		if typ == unix.FAN_EVENT_INFO_TYPE_DFID_NAME {
			raw := rec[16+handleBytes:]
			if i := bytes.IndexByte(raw, 0); i >= 0 {
				raw = raw[:i]
			}

			name = string(raw)
		}

		s.handle(mask, handleType, handle, name)

		return
	}
}

func (s *fanotifySource) handle(mask uint64, handleType int32, handle []byte, name string) {
	dir, ok := s.resolve(handleType, handle)
	if !ok {
		return
	}

	s.pending.Mark(dir, s.seq)

	if name == "" || name == "." {
		return
	}

	rel := join(dir, name)

	if mask&unix.FAN_ONDIR != 0 {
		if mask&(unix.FAN_CREATE|unix.FAN_MOVED_TO) != 0 {
			s.pending.MarkTree(rel, s.seq)
		}

		// the paths of everything under a moved or deleted directory change
		if mask&(unix.FAN_MOVED_FROM|unix.FAN_MOVED_TO|unix.FAN_DELETE) != 0 {
			clear(s.dirs)
		}

		return
	}

	if mask&(unix.FAN_MODIFY|unix.FAN_ATTRIB|unix.FAN_CLOSE_WRITE|unix.FAN_CREATE|unix.FAN_MOVED_TO) != 0 {
		s.changedFile(rel)
	}
}

// resolve is the path below the root of the directory handle names, and
// whether it is under the root at all.
func (s *fanotifySource) resolve(handleType int32, handle []byte) (string, bool) {
	key := strconv.Itoa(int(handleType)) + ":" + string(handle)
	if rel, ok := s.dirs[key]; ok {
		return rel, rel != outside
	}

	fd, err := unix.OpenByHandleAt(s.mountFD, unix.NewFileHandle(handleType, handle), unix.O_PATH|unix.O_CLOEXEC)
	if err != nil {
		// a directory gone since; its removal changed one that isn't
		return "", false
	}

	path, err := os.Readlink("/proc/self/fd/" + strconv.Itoa(fd))
	_ = unix.Close(fd)

	if err != nil || strings.HasSuffix(path, " (deleted)") {
		return "", false
	}

	rel := outside

	switch {
	case path == s.root:
		rel = ""
	case strings.HasPrefix(path, s.root+"/"):
		rel = path[len(s.root)+1:]
	}

	if len(s.dirs) >= MaxDirs {
		clear(s.dirs)
	}

	s.dirs[key] = rel

	return rel, rel != outside
}

func (s *fanotifySource) Close() error {
	err := s.kernelSource.Close()
	_ = unix.Close(s.mountFD)

	return err
}
