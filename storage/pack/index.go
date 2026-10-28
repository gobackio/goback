package pack

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sort"
	"time"

	"github.com/gobackio/goback/proto"
)

// IndexFile is an archive's index; a stored one is sorted by Sum.
type IndexFile []IndexRecord

var indexEndianness = binary.BigEndian

// increment when you make backwards-incompatible changes
var indexFileMagicBytes = []byte("GOBACKIDX_0003")

// unversionedMagicBytes heads the index files written before records
// carried their versions; they are read with every version unset.
var unversionedMagicBytes = []byte("GOBACKIDX_0002")

var errIndexHeaderMismatch = errors.New("received unexpected index file header")

// unversionedRecord is the record layout of an unversioned index file.
type unversionedRecord struct {
	Sum    [proto.HashSize]byte
	Offset uint32
	Length uint32
	Type   uint32
}

// readIndexHeader reads an index file's magic and record count, and
// returns how to read each record that follows.
func readIndexHeader(r io.Reader) (func(io.Reader, *IndexRecord) error, uint32, error) {
	magic := make([]byte, len(indexFileMagicBytes))
	if _, err := io.ReadFull(r, magic); err != nil {
		return nil, 0, err
	}

	read := readRecord
	switch {
	case bytes.Equal(magic, indexFileMagicBytes):
	case bytes.Equal(magic, unversionedMagicBytes):
		read = readUnversionedRecord
	default:
		return nil, 0, errIndexHeaderMismatch
	}

	var count uint32
	if err := binary.Read(r, indexEndianness, &count); err != nil {
		return nil, 0, err
	}

	return read, count, nil
}

const (
	unversionedRecordSize = proto.HashSize + 12
	recordSize            = unversionedRecordSize + 12
)

func readRecord(r io.Reader, record *IndexRecord) error {
	var buf [recordSize]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return err
	}

	decodeUnversioned(buf[:], record)
	record.CarriedTime = int64(indexEndianness.Uint64(buf[unversionedRecordSize:]))
	record.CarriedOffset = indexEndianness.Uint32(buf[unversionedRecordSize+8:])

	return nil
}

func readUnversionedRecord(r io.Reader, record *IndexRecord) error {
	var buf [unversionedRecordSize]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return err
	}

	*record = IndexRecord{}
	decodeUnversioned(buf[:], record)

	return nil
}

// encodeRecord lays the record out as readRecord reads it.
func encodeRecord(buf *[recordSize]byte, record *IndexRecord) {
	copy(buf[:proto.HashSize], record.Sum[:])
	indexEndianness.PutUint32(buf[proto.HashSize:], record.Offset)
	indexEndianness.PutUint32(buf[proto.HashSize+4:], record.Length)
	indexEndianness.PutUint32(buf[proto.HashSize+8:], record.Type)
	indexEndianness.PutUint64(buf[unversionedRecordSize:], uint64(record.CarriedTime))
	indexEndianness.PutUint32(buf[unversionedRecordSize+8:], record.CarriedOffset)
}

func decodeUnversioned(buf []byte, record *IndexRecord) {
	copy(record.Sum[:], buf[:proto.HashSize])
	record.Offset = indexEndianness.Uint32(buf[proto.HashSize:])
	record.Length = indexEndianness.Uint32(buf[proto.HashSize+4:])
	record.Type = indexEndianness.Uint32(buf[proto.HashSize+8:])
}

// Len implements sort.Interface.
func (idx IndexFile) Len() int { return len(idx) }

// Swap implements sort.Interface.
func (idx IndexFile) Swap(i, j int) { idx[i], idx[j] = idx[j], idx[i] }

// Less implements sort.Interface.
func (idx IndexFile) Less(i, j int) bool { return bytes.Compare(idx[i].Sum[:], idx[j].Sum[:]) < 0 }

// position returns the index of the record for hash, or -1.
func (idx IndexFile) position(hash []byte) int {
	i := sort.Search(len(idx), func(i int) bool { return bytes.Compare(idx[i].Sum[:], hash) >= 0 })
	if i < len(idx) && bytes.Equal(idx[i].Sum[:], hash) {
		return i
	}

	return -1
}

// indexScanner yields an archive index's records in stored order, which is
// by Sum. A scanner is used once and closed.
type indexScanner interface {
	// next returns the next record, or nil at the end.
	next() (*IndexRecord, error)
	close() error
}

// fileScanner streams an index straight off its file, so a caller that
// only walks it never holds it.
type fileScanner struct {
	file File
	buf  *bufio.Reader
	read func(io.Reader, *IndexRecord) error
	left uint32
}

func newFileScanner(file File) (*fileScanner, error) {
	buf := bufio.NewReader(file)

	read, count, err := readIndexHeader(buf)
	if err != nil {
		return nil, err
	}

	return &fileScanner{file: file, buf: buf, read: read, left: count}, nil
}

func (s *fileScanner) next() (*IndexRecord, error) {
	if s.left == 0 {
		return nil, nil
	}

	record := new(IndexRecord)
	if err := s.read(s.buf, record); err != nil {
		return nil, err
	}

	s.left--

	return record, nil
}

func (s *fileScanner) close() error { return s.file.Close() }

// sliceScanner serves an index that is already in memory, which is what an
// archive whose index had to be recovered has.
type sliceScanner struct {
	idx IndexFile
	pos int
}

func (s *sliceScanner) next() (*IndexRecord, error) {
	if s.pos >= len(s.idx) {
		return nil, nil
	}

	record := &s.idx[s.pos]
	s.pos++

	return record, nil
}

func (s *sliceScanner) close() error { return nil }

// indexReserve bounds the records ReadFrom reserves room for before it
// reads them.
const indexReserve = 1 << 16

// ReadFrom implements io.ReaderFrom.
func (idx *IndexFile) ReadFrom(reader io.Reader) (int64, error) {
	buf := bufio.NewReader(reader)
	byteCounter := &countingWriter{}

	source := io.TeeReader(buf, byteCounter)

	read, count, err := readIndexHeader(source)
	if err != nil {
		return byteCounter.count, err
	}

	// the count is only as good as the file, so it bounds what is reserved
	// up front but not what is read
	*idx = make([]IndexRecord, 0, min(count, indexReserve))

	for range count {
		var record IndexRecord
		if err := read(source, &record); errors.Is(err, io.EOF) {
			return 0, io.ErrUnexpectedEOF
		} else if err != nil {
			return 0, err
		}

		*idx = append(*idx, record)
	}

	return byteCounter.count, nil
}

// WriteTo implements io.WriterTo.
func (idx IndexFile) WriteTo(writer io.Writer) (int64, error) {
	buf := bufio.NewWriter(writer)
	count := uint32(len(idx))
	byteCounter := &countingWriter{}

	target := io.MultiWriter(buf, byteCounter)

	n, err := target.Write(indexFileMagicBytes)
	if err != nil {
		return int64(n), err
	}

	err = binary.Write(target, indexEndianness, count)
	if err != nil {
		return 0, err
	}

	var record [recordSize]byte
	for i := range idx {
		encodeRecord(&record, &idx[i])
		if _, err := target.Write(record[:]); err != nil {
			return 0, err
		}
	}

	return byteCounter.count, buf.Flush()
}

// IndexRecord locates one object in its archive.
type IndexRecord struct {
	Sum [proto.HashSize]byte
	// CarriedTime and CarriedOffset are the version a rewrite carried the
	// record over with; a zero CarriedTime means the record has its
	// archive's version.
	CarriedTime   int64
	Offset        uint32
	Length        uint32
	Type          uint32
	CarriedOffset uint32
}

// Version is the version of the record in an archive created at created.
func (r IndexRecord) Version(created time.Time) Version {
	if r.CarriedTime != 0 {
		return Version{Time: time.Unix(0, r.CarriedTime), Offset: r.CarriedOffset}
	}

	return Version{Time: created, Offset: r.Offset}
}

// Carry returns the record, at a new place, keeping the version v.
func (r IndexRecord) Carry(v Version) IndexRecord {
	r.CarriedTime, r.CarriedOffset = v.Time.UnixNano(), v.Offset

	return r
}

var _ io.WriterTo = (IndexFile)(nil)
var _ io.ReaderFrom = (*IndexFile)(nil)

type countingWriter struct {
	count int64
}

func (c *countingWriter) Write(data []byte) (int, error) {
	c.count += int64(len(data))
	return len(data), nil
}
