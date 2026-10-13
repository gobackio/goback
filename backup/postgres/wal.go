package postgres

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/gobackio/goback/proto"
)

// The commit metadata the Postgres sets carry.
const (
	// MetaSystemID is the cluster's system identifier, in decimal.
	MetaSystemID = "pg.system_id"
	// MetaTimeline is the timeline a base backup started on, in decimal.
	MetaTimeline = "pg.timeline"
	// MetaStartLSN and MetaStopLSN bound a base backup, as Postgres prints
	// LSNs (0/2000028).
	MetaStartLSN = "pg.start_lsn"
	MetaStopLSN  = "pg.stop_lsn"
	// MetaStartWALFile names the WAL file a base backup started in.
	MetaStartWALFile = "pg.start_wal_file"
	// MetaFirstWALFile and MetaLastWALFile bound the segments a WAL commit
	// holds.
	MetaFirstWALFile = "pg.first_wal_file"
	MetaLastWALFile  = "pg.last_wal_file"
	// MetaEndLSN is where the WAL a WAL commit holds ends, its open
	// segment included; MetaTimeline then names the timeline it ends on.
	MetaEndLSN = "pg.end_lsn"
	// MetaCapture says how a WAL commit's WAL was collected: CaptureReplication,
	// or archive for WAL that archive_command delivered, which restore reads
	// alike.
	MetaCapture = "pg.capture"
	// MetaBaseSet and MetaWindow are, on a WAL commit, the set its base
	// backups go to and how far back point-in-time recovery reaches, as a
	// Go duration.
	MetaBaseSet = "pg.base_set"
	MetaWindow  = "pg.window"
)

// CaptureReplication is the MetaCapture of WAL a receiver streamed.
const CaptureReplication = "replication"

// partialSuffix marks a segment Postgres or the receiver did not finish.
const partialSuffix = ".partial"

// ErrForeignSegment is returned for a WAL segment from another cluster than
// the set's.
var ErrForeignSegment = errors.New("WAL segment from another cluster")

// ErrGap is returned for WAL that skips a segment.
var ErrGap = errors.New("WAL is missing a segment")

const walNameLen = 24

// walPosition is the log and segment part of a WAL file name, which orders
// WAL files across timelines; ok is false for names that are not WAL.
func walPosition(name string) (string, bool) {
	if len(name) < walNameLen {
		return "", false
	}

	if _, err := hex.DecodeString(name[:walNameLen]); err != nil {
		return "", false
	}

	return name[8:walNameLen], true
}

// isSegment reports whether name is a whole WAL segment, not a history,
// backup history or partial file.
func isSegment(name string) bool {
	_, ok := walPosition(name)
	return ok && len(name) == walNameLen
}

// isHistory reports whether name is a timeline's history file.
func isHistory(name string) bool {
	timeline, ok := strings.CutSuffix(name, ".history")
	_, err := strconv.ParseUint(timeline, 16, 32)

	return ok && len(timeline) == 8 && err == nil
}

// isPartial reports whether name is a segment's partial file, and names
// the segment.
func isPartial(name string) (string, bool) {
	segment, ok := strings.CutSuffix(name, partialSuffix)
	return segment, ok && isSegment(segment)
}

// segmentNumber is the segment's place in the WAL of every timeline.
func segmentNumber(name string, segSize uint64) uint64 {
	log, _ := strconv.ParseUint(name[8:16], 16, 32)
	seg, _ := strconv.ParseUint(name[16:24], 16, 32)

	return log*(0x100000000/segSize) + seg
}

// segmentTimeline is the timeline a segment's name places it on.
func segmentTimeline(name string) uint32 {
	timeline, _ := strconv.ParseUint(name[:8], 16, 32)
	return uint32(timeline)
}

// segmentName names the segment of timeline that holds lsn.
func segmentName(timeline uint32, lsn, segSize uint64) string {
	n := lsn / segSize
	perLog := 0x100000000 / segSize

	return fmt.Sprintf("%08X%08X%08X", timeline, n/perLog, n%perLog)
}

// Cutoff names the WAL file the WAL set must reach back to: the one the
// oldest base backup received within window started in, or the newest base
// when none was. ok is false when no base backup records one, and then
// every WAL file is kept.
func Cutoff(bases []*proto.Commit, window time.Duration, now time.Time) (string, bool) {
	since := now.Add(-window).UnixNano()

	var cutoff string
	for _, base := range bases {
		file := base.GetMetadata()[MetaStartWALFile]
		if _, ok := walPosition(file); !ok {
			continue
		}

		if cutoff == "" || base.GetReceivedAtNs() >= since {
			cutoff = file
		}
	}

	return cutoff, cutoff != ""
}

// Carry keeps every timeline history and every WAL file at or after the
// cutoff, and whatever is not WAL at all.
func Carry(cutoff string) func(name string) bool {
	limit, _ := walPosition(cutoff)

	return func(name string) bool {
		position, ok := walPosition(name)
		return !ok || position >= limit
	}
}

// segmentHeader is what the long page header at the start of every WAL
// segment says about it.
type segmentHeader struct {
	timeline uint32
	pageAddr uint64
	systemID uint64
	segSize  uint32
}

// readSegmentHeader reads XLogLongPageHeaderData, which Postgres writes in
// the byte order of the machine; every platform it builds on for production
// is little-endian.
func readSegmentHeader(r io.Reader) (segmentHeader, error) {
	buf := make([]byte, 40)
	if _, err := io.ReadFull(r, buf); err != nil {
		return segmentHeader{}, fmt.Errorf("reading the segment header: %w", err)
	}

	return segmentHeader{
		timeline: binary.LittleEndian.Uint32(buf[4:]),
		pageAddr: binary.LittleEndian.Uint64(buf[8:]),
		systemID: binary.LittleEndian.Uint64(buf[24:]),
		segSize:  binary.LittleEndian.Uint32(buf[32:]),
	}, nil
}

// checkSegment checks that the segment named name belongs to the cluster
// systemID, which is learned from the segment when zero, and that its
// header agrees with its name.
func checkSegment(name string, r io.Reader, systemID uint64) (uint64, error) {
	hdr, err := readSegmentHeader(r)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}

	if systemID != 0 && hdr.systemID != systemID {
		return 0, fmt.Errorf("%w: %s belongs to %d, the set to %d", ErrForeignSegment, name, hdr.systemID, systemID)
	}

	if hdr.segSize == 0 {
		return 0, fmt.Errorf("%s: no segment size in its header", name)
	}

	// a new timeline's first segment starts with the old timeline's WAL,
	// under page headers that name the old timeline
	want := fmt.Sprintf("%08X%08X%08X", hdr.timeline, hdr.pageAddr>>32, (hdr.pageAddr&0xFFFFFFFF)/uint64(hdr.segSize))
	if want[8:] != name[8:] || hdr.timeline > segmentTimeline(name) {
		return 0, fmt.Errorf("%s: its header places it at %s", name, want)
	}

	return hdr.systemID, nil
}

// checkContiguous reports a gap between consecutive segments of a
// timeline, unless a base backup starts in the segment after it, and a
// timeline's partial segment that is not its only one or skips a segment
// after its last whole one. names must be sorted; segSize is the cluster's
// segment size; starts are the segments base backups started in.
func checkContiguous(names []string, segSize uint64, starts map[string]bool) error {
	last := make(map[string]uint64)
	partials := make(map[string]string)

	for _, name := range names {
		if segment, ok := isPartial(name); ok {
			timeline := name[:8]
			if other, ok := partials[timeline]; ok {
				return fmt.Errorf("%w: %s%s and %s are both open on timeline %s", ErrGap, other, partialSuffix, name, timeline)
			}

			partials[timeline] = segment

			continue
		}

		if !isSegment(name) {
			continue
		}

		n := segmentNumber(name, segSize)

		timeline := name[:8]
		if prev, ok := last[timeline]; ok && n != prev+1 && !starts[name] {
			return fmt.Errorf("%w: %s follows segment %d of timeline %s", ErrGap, name, prev, timeline)
		}

		last[timeline] = n
	}

	for timeline, segment := range partials {
		if prev, ok := last[timeline]; ok && segmentNumber(segment, segSize) > prev+1 && !starts[segment] {
			return fmt.Errorf("%w: %s%s follows segment %d of timeline %s", ErrGap, segment, partialSuffix, prev, timeline)
		}
	}

	return nil
}

// FormatLSN prints an LSN as Postgres does.
func FormatLSN(lsn uint64) string {
	return fmt.Sprintf("%X/%X", lsn>>32, lsn&0xFFFFFFFF)
}

// ParseLSN reads an LSN as Postgres prints it.
func ParseLSN(s string) (uint64, error) {
	hi, lo, ok := strings.Cut(s, "/")
	if !ok {
		return 0, fmt.Errorf("%q is not an LSN", s)
	}

	h, err := strconv.ParseUint(hi, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("%q is not an LSN", s)
	}

	l, err := strconv.ParseUint(lo, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("%q is not an LSN", s)
	}

	return h<<32 | l, nil
}
