package mysql

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"
)

const (
	rotateEvent       = 4
	heartbeatEvent    = 27
	heartbeatEventV2  = 41
	binlogHeaderBytes = 4

	// LOG_EVENT_ARTIFICIAL_F marks events the server makes up for a replica
	// that are in no binlog file.
	artificialEvent = 0x20
)

// eventHeader is the common header of a binlog event.
type eventHeader struct {
	timestamp uint32
	kind      byte
	serverID  uint32
	size      uint32
	// logPos is where the event ends in its binlog
	logPos uint32
	flags  uint16
}

func parseEventHeader(b []byte) (eventHeader, error) {
	if len(b) < eventHeaderLen {
		return eventHeader{}, fmt.Errorf("an event of %d bytes", len(b))
	}

	return eventHeader{
		timestamp: binary.LittleEndian.Uint32(b),
		kind:      b[4],
		serverID:  binary.LittleEndian.Uint32(b[5:]),
		size:      binary.LittleEndian.Uint32(b[9:]),
		logPos:    binary.LittleEndian.Uint32(b[13:]),
		flags:     binary.LittleEndian.Uint16(b[17:]),
	}, nil
}

// binlogWriter appends received events to the binlog being received in
// the spool's receiving directory, and moves each binlog into the spool
// once it is whole. Each binlog is written from its first byte.
type binlogWriter struct {
	spool Spool

	file *os.File
	name string
	// written is the open binlog's length, at the end of its last event
	written int64
	// stamp is the timestamp of the open binlog's last event, which a commit
	// records as its mtime
	stamp uint32
	// created is the timestamp of its format description event; zero
	// before one arrived
	created uint32

	// held is the binlog a commit in flight reads in the receiving
	// directory. Moving it, and every binlog after it, waits in pending
	// until the commit is done, so the spool never holds a binlog without
	// the ones before it.
	held    string
	pending []move
}

type move struct{ from, to string }

// openBinlogWriter takes over what the receiving directory holds after a
// stop or a crash: binlogs that are whole move into the spool, and the one
// being received is cut back to its last whole event.
func openBinlogWriter(spool Spool) (*binlogWriter, error) {
	w := &binlogWriter{spool: spool}

	if err := os.MkdirAll(spool.receiving(), 0o700); err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(spool.receiving())
	if err != nil {
		return nil, err
	}

	var names []string
	for _, e := range entries {
		if e.Type().IsRegular() {
			names = append(names, e.Name())
		}
	}

	slices.SortFunc(names, compareBinlogs)

	// a binlog waited for a commit only once it was whole and on disk
	for len(names) > 1 {
		if err := w.move(names[0], names[0]); err != nil {
			return nil, err
		}

		names = names[1:]
	}

	if len(names) == 1 {
		if err := w.reopen(names[0]); err != nil {
			return nil, err
		}
	}

	return w, nil
}

// reopen opens the binlog name of the receiving directory to append to,
// cut back to its last whole event.
func (w *binlogWriter) reopen(name string) error {
	path := w.path(name)

	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}

	end, stamp, created, err := lastEvent(file)
	if err != nil {
		_ = file.Close()
		return os.Remove(path)
	}

	if err := file.Truncate(end); err != nil {
		_ = file.Close()
		return err
	}

	if _, err := file.Seek(end, io.SeekStart); err != nil {
		_ = file.Close()
		return err
	}

	w.file, w.name, w.written, w.stamp, w.created = file, name, end, stamp, created

	return nil
}

// lastEvent reads a binlog's events and returns where its last whole one
// ends, that event's timestamp and the format description's.
func lastEvent(r io.ReadSeeker) (int64, uint32, uint32, error) {
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return 0, 0, 0, err
	}

	br := bufio.NewReader(r)

	magic := make([]byte, binlogHeaderBytes)
	if _, err := io.ReadFull(br, magic); err != nil || !bytes.Equal(magic, binlogMagic) {
		return 0, 0, 0, errors.New("no binlog magic")
	}

	var (
		end     = int64(binlogHeaderBytes)
		stamp   uint32
		created uint32
		header  = make([]byte, eventHeaderLen)
	)

	for {
		if _, err := io.ReadFull(br, header); err != nil {
			return end, stamp, created, nil
		}

		h, _ := parseEventHeader(header)
		if h.size < eventHeaderLen {
			return end, stamp, created, nil
		}

		if _, err := br.Discard(int(h.size) - eventHeaderLen); err != nil {
			return end, stamp, created, nil
		}

		if end == binlogHeaderBytes && h.kind == formatDescriptionEvent {
			created = h.timestamp
		}

		end += int64(h.size)
		stamp = h.timestamp
	}
}

func (w *binlogWriter) path(name string) string {
	return filepath.Join(w.spool.receiving(), name)
}

// begin readies the writer for the server's stream of name from pos: the
// open binlog continues where it ends, or, once the server moved on to
// another binlog, the open one is whole and name starts.
func (w *binlogWriter) begin(name string, pos int64) error {
	if w.file != nil && w.name == name {
		if pos != w.written {
			return fmt.Errorf("the server sends %s from %d, where the receiver holds %d bytes", name, pos, w.written)
		}

		return nil
	}

	if err := w.close(); err != nil {
		return err
	}

	if pos != binlogHeaderBytes {
		return fmt.Errorf("the server sends %s from %d, and the receiver holds none of it", name, pos)
	}

	file, err := os.OpenFile(w.path(name), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}

	if _, err := file.Write(binlogMagic); err != nil {
		_ = file.Close()
		return err
	}

	w.file, w.name, w.written, w.stamp, w.created = file, name, binlogHeaderBytes, 0, 0

	return nil
}

// write appends one event of the open binlog.
func (w *binlogWriter) write(h eventHeader, event []byte) error {
	if w.file == nil {
		return errors.New("an event arrived before the server named its binlog")
	}

	if want := uint32(w.written + int64(len(event))); h.logPos != want {
		return fmt.Errorf("an event of %s ends at %d, where the receiver expects %d", w.name, h.logPos, want)
	}

	if _, err := w.file.Write(event); err != nil {
		return err
	}

	if w.written == binlogHeaderBytes && h.kind == formatDescriptionEvent {
		w.created = h.timestamp
	}

	w.written += int64(len(event))
	w.stamp = h.timestamp

	return nil
}

// close makes the open binlog durable, stamps it with its last event's
// time and moves it into the spool, or leaves the move to release while a
// commit reads it.
func (w *binlogWriter) close() error {
	if w.file == nil {
		return nil
	}

	err := w.file.Sync()
	if closeErr := w.file.Close(); err == nil {
		err = closeErr
	}

	w.file = nil

	if err != nil {
		return err
	}

	at := time.Unix(int64(w.stamp), 0)
	if err := os.Chtimes(w.path(w.name), at, at); err != nil {
		return err
	}

	if w.name == w.held || len(w.pending) > 0 {
		w.pending = append(w.pending, move{w.name, w.name})
		return nil
	}

	return w.move(w.name, w.name)
}

// move renames a whole binlog of the receiving directory into the spool.
// A binlog the spool holds already under that name must be the same.
func (w *binlogWriter) move(from, to string) error {
	src, dst := w.path(from), w.spool.path(to)

	if held, err := os.ReadFile(dst); err == nil {
		received, err := os.ReadFile(src)
		if err != nil {
			return err
		}

		if !bytes.Equal(held, received) {
			return fmt.Errorf("%w: %s was received again with other contents", ErrBinlogChanged, to)
		}

		return os.Remove(src)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	if err := os.Rename(src, dst); err != nil {
		return err
	}

	if err := syncDir(w.spool.receiving()); err != nil {
		return err
	}

	return syncDir(w.spool.Dir)
}

// OpenBinlog is a binlog a receiver is still writing.
type OpenBinlog struct {
	Name string
	Path string
	// Size is how much of it holds whole events.
	Size int64
	// ModTime is its last event's time.
	ModTime time.Time
	// Created is its format description event's time.
	Created time.Time
}

// cut is what one commit takes from the spool.
type cut struct {
	// files are the spool's whole binlogs; never nil, so a commit takes
	// these and not what the spool holds by then.
	files []string
	// open is the binlog being received up to its last event; nil when it
	// holds no event yet.
	open *OpenBinlog
	// end and pos are where the binlogs the commit holds end.
	end string
	pos int64
}

// cut makes what was received durable and describes it for a commit. The
// open binlog stays where it is until release.
func (w *binlogWriter) cut() (*cut, error) {
	c := &cut{}

	if w.file != nil {
		if err := w.file.Sync(); err != nil {
			return nil, err
		}

		if w.written > binlogHeaderBytes {
			c.open = &OpenBinlog{Name: w.name, Path: w.path(w.name), Size: w.written,
				ModTime: time.Unix(int64(w.stamp), 0), Created: time.Unix(int64(w.created), 0)}
			c.end, c.pos = w.name, w.written
			w.held = w.name
		}
	}

	if err := syncDir(w.spool.receiving()); err != nil {
		return nil, err
	}

	if err := syncDir(w.spool.Dir); err != nil {
		return nil, err
	}

	files, err := w.spool.Files()
	if err != nil {
		return nil, err
	}

	c.files = append([]string{}, files...)

	if c.open == nil && len(files) > 0 {
		c.end = files[len(files)-1]
		if info, err := os.Stat(w.spool.path(c.end)); err == nil {
			c.pos = info.Size()
		}
	}

	return c, nil
}

// release moves the binlogs that became whole while a commit read them
// into the spool.
func (w *binlogWriter) release() error {
	w.held = ""

	for len(w.pending) > 0 {
		if err := w.move(w.pending[0].from, w.pending[0].to); err != nil {
			return err
		}

		w.pending = w.pending[1:]
	}

	return nil
}

// position is where the open binlog ends; empty before one is open.
func (w *binlogWriter) position() (string, int64) {
	if w.file == nil {
		return "", 0
	}

	return w.name, w.written
}

// resume is where the server's stream goes on for the writer: where the
// open binlog ends, or the start of the binlog after the last whole one.
// ok is false before the writer took any binlog.
func (w *binlogWriter) resume() (string, int64, bool) {
	if w.file != nil {
		return w.name, w.written, true
	}

	base, n, ok := splitBinlog(w.name)
	if !ok {
		return "", 0, false
	}

	return binlogName(base, n+1, w.name), binlogHeaderBytes, true
}

// stop closes the open binlog's file, leaving it in the receiving
// directory.
func (w *binlogWriter) stop() {
	if w.file != nil {
		_ = w.file.Close()
		w.file = nil
	}
}
