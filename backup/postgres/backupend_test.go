package postgres

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// walWriter lays records out in WAL pages as Postgres does, in segments of
// four small pages.
type walWriter struct {
	wal []byte
}

const (
	testPageSize = 256
	testSegSize  = 4 * testPageSize
	testSegStart = 3 * testSegSize
)

func (w *walWriter) lsn() uint64 {
	return testSegStart + uint64(len(w.wal))
}

func (w *walWriter) pageHeader(rest int) {
	info := uint16(0)
	if rest > 0 {
		info |= xlpFirstIsContRecord
	}

	size := shortPageHeaderLen
	if w.lsn()%testSegSize == 0 {
		info |= xlpLongHeader
		size = longPageHeaderLen
	}

	hdr := make([]byte, size)
	binary.LittleEndian.PutUint16(hdr[2:], info)
	binary.LittleEndian.PutUint32(hdr[4:], 1)
	binary.LittleEndian.PutUint64(hdr[8:], w.lsn())
	binary.LittleEndian.PutUint32(hdr[16:], uint32(rest))

	if size == longPageHeaderLen {
		binary.LittleEndian.PutUint64(hdr[24:], 42)
		binary.LittleEndian.PutUint32(hdr[32:], testSegSize)
		binary.LittleEndian.PutUint32(hdr[36:], testPageSize)
	}

	w.wal = append(w.wal, hdr...)
}

// record appends an XLOG record of info with data, and returns where it
// ends.
func (w *walWriter) record(info byte, data []byte) uint64 {
	rec := make([]byte, recordHeaderLen, recordHeaderLen+len(data))
	binary.LittleEndian.PutUint32(rec, uint32(recordHeaderLen+len(data)))
	rec[16] = info
	rec = append(rec, data...)

	for rest := 0; len(rec) > 0; rest = len(rec) {
		if w.lsn()%testPageSize == 0 {
			w.pageHeader(rest)
		}

		n := min(len(rec), testPageSize-int(w.lsn()%testPageSize))
		w.wal = append(w.wal, rec[:n]...)
		rec = rec[n:]
	}

	for w.lsn()%8 != 0 {
		w.wal = append(w.wal, 0)
	}

	return w.lsn()
}

func (w *walWriter) backupEnd(start uint64) uint64 {
	data := []byte{blockIDDataShort, 8, 0, 0, 0, 0, 0, 0, 0, 0}
	binary.LittleEndian.PutUint64(data[2:], start)

	return w.record(xlogBackupEnd, data)
}

func (w *walWriter) fill(upTo uint64) {
	for w.lsn() < upTo {
		if w.lsn()%testPageSize == 0 {
			w.pageHeader(0)
		}

		w.wal = append(w.wal, 0)
	}
}

func scan(t *testing.T, ends *backupEnd, name string, wal []byte) {
	t.Helper()

	segment := ends.segment(name)
	for len(wal) > 0 {
		n := min(len(wal), 100)
		_, err := segment.Write(wal[:n])
		require.NoError(t, err)

		wal = wal[n:]
	}
}

func TestTheEndOfABackupIsFoundAcrossPagesAndSwitches(t *testing.T) {
	var w walWriter

	w.pageHeader(0)
	start := w.lsn()
	w.record(0x10, make([]byte, 300))
	w.record(xlogSwitch, nil)
	w.wal = append(w.wal, make([]byte, testSegSize-len(w.wal))...)

	w.record(0x10, make([]byte, 500))
	w.backupEnd(start - 8)
	stop := w.backupEnd(start)
	w.fill(testSegStart + 2*testSegSize)

	ends := &backupEnd{}
	scan(t, ends, "000000010000000000000003", w.wal[:testSegSize])
	scan(t, ends, "000000010000000000000004", w.wal[testSegSize:])

	require.Equal(t, stop, ends.stops[start])
	require.Len(t, ends.stops, 2)
}

func TestWALThatSkipsASegmentEndsTheSearch(t *testing.T) {
	var w walWriter

	w.pageHeader(0)
	start := w.lsn()
	w.record(xlogSwitch, nil)
	w.wal = append(w.wal, make([]byte, testSegSize-len(w.wal))...)
	w.fill(testSegStart + 2*testSegSize)
	w.backupEnd(start)
	w.fill(testSegStart + 3*testSegSize)

	ends := &backupEnd{}
	scan(t, ends, "000000010000000000000003", w.wal[:testSegSize])
	scan(t, ends, "000000010000000000000005", w.wal[2*testSegSize:])

	require.Empty(t, ends.stops)
}

func TestAPageSizeThatIsNoPowerOfTwoEndsTheSearch(t *testing.T) {
	page := make([]byte, 42)
	binary.LittleEndian.PutUint16(page[2:], xlpLongHeader)
	binary.LittleEndian.PutUint64(page[8:], 42000)
	binary.LittleEndian.PutUint32(page[32:], 42000)
	binary.LittleEndian.PutUint32(page[36:], 42)

	ends := &backupEnd{}
	scan(t, ends, "000000010000000000000001", page)

	require.True(t, ends.done)
	require.Empty(t, ends.stops)
}

func TestTheEndOfABackupIsWhatPostgresReports(t *testing.T) {
	for file, stop := range map[string]uint64{"testdata/wal-9.6": 0x30000F8, "testdata/wal-17": 0x3000120} {
		wal, err := os.ReadFile(file)
		require.NoError(t, err)

		ends := &backupEnd{}
		scan(t, ends, "000000010000000000000003", wal)
		require.Equal(t, map[uint64]uint64{0x3000028: stop}, ends.stops, file)
	}
}
