package postgres

import (
	"fmt"
	"os"
	"path/filepath"
)

// receivingDir is the spool's directory for the segment being received;
// Spool.Files skips it.
const receivingDir = ".receiving"

// segmentWriter writes streamed WAL into its segment under the spool's
// receiving directory, and moves each segment into the spool once it is
// whole. Streaming starts at a segment's start, so a segment is always
// written from its first byte.
type segmentWriter struct {
	spool    Spool
	segSize  uint64
	timeline uint32

	file  *os.File
	name  string
	start uint64
	// written is the end of the WAL written so far: the write position
	written uint64

	// held is the segment a commit in flight reads in the receiving
	// directory. Closing it, and every segment after it, waits in pending
	// until the commit is done, so the spool never holds a segment without
	// the ones before it.
	held    string
	pending []move
}

type move struct{ from, to string }

// newSegmentWriter writes timeline's WAL from from, a segment's start.
func newSegmentWriter(spool Spool, segSize uint64, timeline uint32, from uint64) (*segmentWriter, error) {
	if from%segSize != 0 {
		return nil, fmt.Errorf("streaming starts at %s, inside a segment", FormatLSN(from))
	}

	if err := os.MkdirAll(spool.receiving(), 0o700); err != nil {
		return nil, err
	}

	return &segmentWriter{spool: spool, segSize: segSize, timeline: timeline, written: from}, nil
}

func (s Spool) receiving() string {
	return filepath.Join(s.Dir, receivingDir)
}

// write puts WAL that starts at lsn into its segments.
func (w *segmentWriter) write(lsn uint64, data []byte) error {
	if lsn != w.written {
		return fmt.Errorf("WAL arrived at %s, after %s", FormatLSN(lsn), FormatLSN(w.written))
	}

	for len(data) > 0 {
		if w.file == nil {
			if err := w.open(); err != nil {
				return err
			}
		}

		offset := w.written - w.start
		n := min(uint64(len(data)), w.segSize-offset)

		if _, err := w.file.WriteAt(data[:n], int64(offset)); err != nil {
			return err
		}

		data = data[n:]
		w.written += n

		if w.written == w.start+w.segSize {
			if err := w.close(w.name); err != nil {
				return err
			}
		}
	}

	return nil
}

func (w *segmentWriter) open() error {
	w.start = w.written - w.written%w.segSize
	w.name = segmentName(w.timeline, w.start, w.segSize)

	file, err := os.OpenFile(w.path(w.name), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}

	w.file = file

	return nil
}

func (w *segmentWriter) path(name string) string {
	return filepath.Join(w.spool.receiving(), name)
}

// close makes the open segment durable and moves it into the spool as
// name, or leaves that to release while a commit reads it.
func (w *segmentWriter) close(name string) error {
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

	if w.name == w.held || len(w.pending) > 0 {
		w.pending = append(w.pending, move{w.name, name})
		return nil
	}

	return w.move(w.name, name)
}

// move renames a whole segment of the receiving directory into the spool.
func (w *segmentWriter) move(from, to string) error {
	return w.spool.adopt(w.path(from), to)
}

// switchTimeline closes the open segment as a partial one, as Postgres
// names the last segment of a timeline it leaves, and goes on with
// timeline from from.
func (w *segmentWriter) switchTimeline(timeline uint32, from uint64) error {
	if w.file != nil {
		if err := w.close(w.name + partialSuffix); err != nil {
			return err
		}
	}

	w.timeline = timeline
	w.written = from - from%w.segSize

	return nil
}

// cut is what one commit takes from the spool.
type cut struct {
	// files are the spool's whole files.
	files []string
	// open is the segment being received, up to the end of the WAL written
	// to it; nil when it holds nothing yet.
	open *OpenSegment
	// end is where the WAL the commit holds ends.
	end uint64
}

// cut makes what was received durable and describes it for a commit. The
// open segment stays where it is until release, however full it gets.
func (w *segmentWriter) cut() (*cut, error) {
	c := &cut{end: w.written}

	if w.file != nil {
		if err := w.file.Sync(); err != nil {
			return nil, err
		}

		if size := w.written - w.start; size >= longPageHeaderLen {
			c.open = &OpenSegment{Name: w.name, Path: w.path(w.name), Size: int64(size)}
			w.held = w.name
		} else {
			c.end = w.start
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

	c.files = files

	return c, nil
}

// release moves the segments that filled up while a commit read them into
// the spool.
func (w *segmentWriter) release() error {
	w.held = ""

	for len(w.pending) > 0 {
		if err := w.move(w.pending[0].from, w.pending[0].to); err != nil {
			return err
		}

		w.pending = w.pending[1:]
	}

	return nil
}

// stop closes the open segment's file, leaving it in the receiving
// directory.
func (w *segmentWriter) stop() {
	if w.file != nil {
		_ = w.file.Close()
		w.file = nil
	}
}
