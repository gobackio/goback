package postgres

import (
	"encoding/binary"
	"strconv"
	"time"
)

const (
	recordHeaderLen = 24
	// a record header, then a short main-data header of one id and one
	// length byte, then the backup's start LSN
	backupEndLen = recordHeaderLen + 2 + 8
	// a record header, an origin, a top-level xid and a long main-data
	// header, then a transaction's time
	xactTimeLen = recordHeaderLen + 3 + 5 + 5 + 8
	recordKeep  = max(backupEndLen, xactTimeLen)

	rmXLOG = 0
	rmXact = 1

	xlogBackupEnd = 0x50
	xlogSwitch    = 0x40

	xactOpMask         = 0x70
	xactCommit         = 0x00
	xactAbort          = 0x20
	xactCommitPrepared = 0x30
	xactAbortPrepared  = 0x40

	xlpFirstIsContRecord = 0x0001
	xlpLongHeader        = 0x0002
	shortPageHeaderLen   = 24
	longPageHeaderLen    = 40

	blockIDDataShort   = 255
	blockIDDataLong    = 254
	blockIDOrigin      = 253
	blockIDTopLevelXid = 252
)

// postgresEpoch is the zero of Postgres timestamps.
var postgresEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// walScan reads the records of WAL fed to it segment by segment, in order,
// through segment. stops finds where base backups stopped in the WAL a base
// backup's tar holds: at the end of the end-of-backup record that names a
// backup's start, which is what Postgres reports as the stop location.
// xactTime is the time of the last transaction commit or abort read, which
// a recovery target time is judged by.
type walScan struct {
	stops    map[uint64]uint64
	xactTime time.Time
	done     bool

	pageSize uint64
	segSize  uint64

	next uint64

	synced     bool
	remaining  uint32
	header     []byte
	skipToNext bool
}

// segment returns the writer the content of the WAL segment name goes to.
func (e *walScan) segment(name string) *walSegment {
	return &walSegment{end: e, name: name}
}

// walSegment cuts a segment's content into pages for its walScan.
type walSegment struct {
	end    *walScan
	name   string
	lsn    uint64
	placed bool
	page   []byte
}

func (s *walSegment) Write(p []byte) (int, error) {
	n := len(p)

	for len(p) > 0 && !s.end.done {
		want := s.end.pageSize
		if !s.placed {
			want = longPageHeaderLen
		}

		take := min(uint64(len(p)), want-uint64(len(s.page)))
		s.page = append(s.page, p[:take]...)
		p = p[take:]

		switch {
		case uint64(len(s.page)) < want:
		case !s.placed:
			s.place()
		default:
			s.end.page(s.page, s.lsn)
			s.lsn += s.end.pageSize
			s.page = s.page[:0]
		}
	}

	return n, nil
}

// place learns the segment's position and the cluster's page and segment
// sizes from its long page header.
func (s *walSegment) place() {
	s.placed = true

	hdr := s.page
	e := s.end
	e.segSize = uint64(binary.LittleEndian.Uint32(hdr[32:]))
	e.pageSize = uint64(binary.LittleEndian.Uint32(hdr[36:]))

	log, err1 := strconv.ParseUint(s.name[8:16], 16, 32)
	seg, err2 := strconv.ParseUint(s.name[16:24], 16, 32)

	if err1 != nil || err2 != nil || e.segSize == 0 || e.pageSize < longPageHeaderLen || e.pageSize&(e.pageSize-1) != 0 || e.segSize%e.pageSize != 0 {
		e.done = true
		return
	}

	s.lsn = log<<32 | seg*e.segSize
}

// page reads the records of one WAL page that starts at lsn.
func (e *walScan) page(p []byte, lsn uint64) {
	if e.next != 0 && lsn != e.next {
		e.done = true
		return
	}

	e.next = lsn + e.pageSize

	if lsn%e.segSize == 0 {
		e.skipToNext = false
	}

	if e.skipToNext {
		return
	}

	if binary.LittleEndian.Uint64(p[8:]) != lsn {
		e.done = true
		return
	}

	info := binary.LittleEndian.Uint16(p[2:])

	pos := uint64(shortPageHeaderLen)
	if info&xlpLongHeader != 0 {
		pos = longPageHeaderLen
	}

	switch {
	case e.remaining > 0 && info&xlpFirstIsContRecord == 0:
		e.done = true
		return
	case !e.synced && info&xlpFirstIsContRecord != 0:
		rest := uint64(binary.LittleEndian.Uint32(p[16:]))
		if pos+rest >= e.pageSize {
			return
		}

		pos = align(pos + rest)
	}

	for pos < e.pageSize && !e.done && !e.skipToNext {
		if e.remaining == 0 {
			total := binary.LittleEndian.Uint32(p[pos:])
			if total < recordHeaderLen {
				e.done = true
				return
			}

			e.synced, e.remaining, e.header = true, total, e.header[:0]
		}

		take := min(uint64(e.remaining), e.pageSize-pos)
		if keep := min(take, uint64(recordKeep-len(e.header))); keep > 0 {
			e.header = append(e.header, p[pos:pos+keep]...)
		}

		e.remaining -= uint32(take)
		pos += take

		if e.remaining == 0 {
			e.endRecord(align(lsn + pos))
			pos = align(pos)
		}
	}
}

// endRecord looks at the record that just ended at end.
func (e *walScan) endRecord(end uint64) {
	h := e.header
	if len(h) < recordHeaderLen {
		return
	}

	if h[17] == rmXact {
		e.xactRecord(h)
		return
	}

	if h[17] != rmXLOG {
		return
	}

	switch h[16] & 0xF0 {
	case xlogSwitch:
		e.skipToNext = true
	case xlogBackupEnd:
		if len(h) == backupEndLen && h[recordHeaderLen] == blockIDDataShort && h[recordHeaderLen+1] == 8 {
			if e.stops == nil {
				e.stops = make(map[uint64]uint64)
			}

			e.stops[binary.LittleEndian.Uint64(h[recordHeaderLen+2:])] = end
		}
	}
}

// xactRecord takes the time of a transaction's commit or abort from the
// head of its record, h: the first field of its main data, which follows
// the record's headers as such a record refers to no blocks.
func (e *walScan) xactRecord(h []byte) {
	switch h[16] & xactOpMask {
	case xactCommit, xactAbort, xactCommitPrepared, xactAbortPrepared:
	default:
		return
	}

	pos := recordHeaderLen

	for pos < len(h) {
		switch h[pos] {
		case blockIDOrigin:
			pos += 3
			continue
		case blockIDTopLevelXid:
			pos += 5
			continue
		case blockIDDataShort:
			pos += 2
		case blockIDDataLong:
			pos += 5
		default:
			return
		}

		if pos+8 <= len(h) {
			e.xactTime = postgresEpoch.Add(time.Duration(int64(binary.LittleEndian.Uint64(h[pos:]))) * time.Microsecond)
		}

		return
	}
}

// align rounds an LSN up to the 8 bytes Postgres aligns records to.
func align(lsn uint64) uint64 {
	return (lsn + 7) &^ 7
}
