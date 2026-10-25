package pack

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"math"
	"path"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gobackio/goback/proto"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var errAlreadyClosed = errors.New("Writer is already closed")

const (
	archiveFormatVersion uint16 = 2
	archiveHashSHA256    uint16 = 1
	archiveHeaderSize           = 16
)

var archiveMagic = []byte("GOBACKPACK")

// archiveHeader is the fixed 16-byte prefix of every archive: magic, format
// version, hash algorithm, two reserved bytes.
func archiveHeader() []byte {
	hdr := make([]byte, archiveHeaderSize)
	copy(hdr, archiveMagic)
	binary.BigEndian.PutUint16(hdr[10:], archiveFormatVersion)
	binary.BigEndian.PutUint16(hdr[12:], archiveHashSHA256)

	return hdr
}

func checkArchiveHeader(hdr []byte) error {
	if len(hdr) < archiveHeaderSize || !bytes.Equal(hdr[:len(archiveMagic)], archiveMagic) {
		return errors.New("not a goback archive")
	}

	if v := binary.BigEndian.Uint16(hdr[10:]); v != archiveFormatVersion {
		return errors.Errorf("archive format version %d, want %d", v, archiveFormatVersion)
	}

	if h := binary.BigEndian.Uint16(hdr[12:]); h != archiveHashSHA256 {
		return errors.Errorf("archive hash algorithm %d, want %d", h, archiveHashSHA256)
	}

	return nil
}

type readFile interface {
	io.ReadSeeker
	io.Closer
}

type writeFile interface {
	io.WriteCloser
}

type archive struct {
	writeFile  writeFile
	readFile   readFile
	readOnly   bool
	size       uint64
	writeIndex map[string]*IndexRecord
	gc         *gcFile
	mtx        sync.RWMutex
	last       *proto.Ref
	storage    ArchiveStorage
	name       string
	atRest     *archiveKey
	logger     *slog.Logger
	// listed is what a listing said of the archive file, nil when no
	// listing did
	listed *ListedFile

	// owner is the session writing this archive, nil once finalized or
	// when opened from storage
	owner   *writeSession
	session string
	state   ArchiveState
	// created versions the archive's records, zero until its index file
	// is stored
	created time.Time
	// indexModified is when the storage said it stored the index file this
	// process wrote, zero when it did not say
	indexModified time.Time

	// rows indexes the objects of a claimed archive as they are written,
	// nil for an archive no claim was taken on; opened is when the claim
	// was taken, by this process's clock, and closing finalizes the archive
	// once it has been open for as long as it may be
	rows    *rowWriter
	opened  time.Time
	closing *time.Timer

	// stored, when set, is told the bytes the archive and its index take
	// up in the storage once the writer has put both there
	stored func(a *archive, bytes int64)

	// headerChecked is set once the archive's header was found to be
	// goback's; headerMtx keeps concurrent first reads to one check
	headerChecked atomic.Bool
	headerMtx     sync.Mutex
}

// newArchive opens a writable archive named by a fresh uuid under dir,
// sealing its payloads under atRest when that is set.
func newArchive(storage ArchiveStorage, dir string, atRest *AtRestKey, logger *slog.Logger) (*archive, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	a := &archive{
		storage:  storage,
		name:     path.Join(dir, id.String()),
		readOnly: false,
		logger:   logger,
	}

	return a, a.openWith(atRest)
}

func openArchive(storage ArchiveStorage, name string, listed *ListedFile, atRest *AtRestKey, logger *slog.Logger) (*archive, error) {
	a := &archive{
		storage:  storage,
		name:     name,
		readOnly: true,
		logger:   logger,
		listed:   listed,
	}

	return a, a.openWith(atRest)
}

// openWith opens the archive sealing and opening under its own key,
// derived from atRest when that is set.
func (a *archive) openWith(atRest *AtRestKey) error {
	if atRest != nil {
		key, err := atRest.archive(a.name)
		if err != nil {
			return err
		}

		a.atRest = key
	}

	return a.open()
}

func (a *archive) recoverIndex(err error) (IndexFile, error) {
	a.logger.Warn("recovering the index from the archive", "archive", a.name, "err", err)

	recoveredIndex := make(IndexFile, 0)

	err = a.foreach(loadNone, func(o *proto.ObjectHeader, _ []byte, offset, length uint32) error {
		record := IndexRecord{
			Offset: offset,
			Length: length,
			Type:   uint32(o.Type),
		}
		copy(record.Sum[:], o.Ref.Hash)

		recoveredIndex = append(recoveredIndex, record)

		return nil
	})

	if err != nil {
		return nil, errors.Wrap(err, "Couldn't read archive to recover index")
	}

	a.logger.Info("recovered index", "archive", a.name, "records", len(recoveredIndex))

	sort.Sort(recoveredIndex)

	return recoveredIndex, nil
}

// getIndex reads the archive's index file. Only a missing one is recovered
// from the archive, and only in memory: the creation time of a stored
// index file versions the archive's records, so none is written after the
// fact, and an index file that cannot be read is an error.
func (a *archive) getIndex() (IndexFile, error) {
	idxFile, err := a.storage.Open(a.indexName())
	if notExist(err) {
		// TODO: recovery may take very long; make it configurable
		return a.recoverIndex(err)
	}

	if err != nil {
		return nil, errors.Wrapf(err, "opening the index of %s", a.name)
	}
	defer idxFile.Close()

	idxBuf := bytes.NewBuffer(nil)
	if _, err := io.Copy(idxBuf, idxFile); err != nil {
		return nil, errors.Wrapf(err, "reading the index of %s", a.name)
	}

	var index IndexFile
	if _, err := (&index).ReadFrom(idxBuf); err != nil {
		return nil, errors.Wrapf(err, "reading the index of %s", a.name)
	}

	return index, nil
}

func (a *archive) open() (err error) {
	a.mtx.Lock()
	defer a.mtx.Unlock()

	if !a.readOnly {
		file, err := a.storage.Create(a.archiveName())
		if err != nil {
			return errors.Wrap(err, "Failed creating archive file")
		}

		a.writeFile = newHashedWriter(file)

		_, err = a.writeFile.Write(archiveHeader())
		if err != nil {
			return errors.Wrap(err, "Failed writing archive header")
		}

		a.size = archiveHeaderSize
		a.writeIndex = make(map[string]*IndexRecord)
		a.headerChecked.Store(true)
	}

	var readFile File
	if a.readOnly && a.listed != nil {
		readFile, err = OpenListed(a.storage, *a.listed)
	} else {
		readFile, err = a.storage.Open(a.archiveName())
	}

	if err != nil {
		return errors.Wrap(err, "Failed opening archive for reading")
	}
	a.readFile = readFile

	if a.readOnly {
		info, err := readFile.Stat()
		if err != nil {
			readFile.Close()
			return err
		}

		a.size = uint64(info.Size())
	}

	return nil
}

// checkHeader rejects an archive whose header is not goback's, reading the
// header until it has passed once.
func (a *archive) checkHeader() error {
	if a.headerChecked.Load() {
		return nil
	}

	a.headerMtx.Lock()
	defer a.headerMtx.Unlock()

	if a.headerChecked.Load() {
		return nil
	}

	hdr, _, err := a.readAt(0, archiveHeaderSize)
	if err == nil {
		err = checkArchiveHeader(hdr)
	}

	if err != nil {
		return errors.Wrapf(err, "archive %s", a.name)
	}

	a.headerChecked.Store(true)

	return nil
}

// indexLocation answers from the write index, which stays alive after the
// writer is closed until releaseWriteIndex is called.
func (a *archive) indexLocation(ref *proto.Ref) *IndexRecord {
	a.mtx.RLock()
	defer a.mtx.RUnlock()

	return a.writeIndex[string(ref.Hash)]
}

func (a *archive) gcResult() *gcFile {
	a.mtx.RLock()
	defer a.mtx.RUnlock()

	return a.gc
}

func (a *archive) setGCResult(g *gcFile) {
	a.mtx.Lock()
	a.gc = g
	a.mtx.Unlock()
}

// candidate reports whether the last completed generation found the object
// unreachable, so a presence check must not rely on this copy.
func (a *archive) candidate(hash []byte) bool {
	a.mtx.RLock()
	defer a.mtx.RUnlock()

	return a.gc != nil && a.gc.candidate(hash)
}

func (a *archive) releaseWriteIndex() {
	a.mtx.Lock()
	a.writeIndex = nil
	a.mtx.Unlock()
}

func (a *archive) archiveName() string {
	return a.name + ArchiveSuffix
}

func (a *archive) indexName() string {
	return a.name + IndexExt
}

func (a *archive) setCreated(created time.Time) {
	a.mtx.Lock()
	a.created = created
	a.mtx.Unlock()
}

// version returns the version of rec, one of the archive's records.
func (a *archive) version(rec IndexRecord) Version {
	a.mtx.RLock()
	defer a.mtx.RUnlock()

	return rec.Version(a.created)
}

// newerThan reports whether rec, one of the archive's records, is newer
// than v.
func (a *archive) newerThan(rec IndexRecord, v Version) bool {
	return v.Before(a.version(rec))
}

// indexCreated returns when the storage created the archive's index file,
// or the archive itself when the index file is gone.
func (a *archive) indexCreated() (time.Time, error) {
	a.mtx.RLock()
	modified := a.indexModified
	a.mtx.RUnlock()

	if !modified.IsZero() {
		return modified, nil
	}

	file, err := a.storage.Open(a.indexName())
	if notExist(err) {
		file, err = a.storage.Open(a.archiveName())
	}

	if err != nil {
		return time.Time{}, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return time.Time{}, err
	}

	// every index keeps it to the microsecond
	return info.ModTime().Truncate(time.Microsecond), nil
}

// readRecord returns the raw bytes of one index record.
func (a *archive) readRecord(loc *IndexRecord) ([]byte, bool, error) {
	return a.readSpan(int64(loc.Offset), int64(loc.Length))
}

// scanIndex streams the archive's index. An index file that cannot be
// read is recovered into memory first, as any read of it would.
func (a *archive) scanIndex() (indexScanner, error) {
	file, err := a.storage.Open(a.indexName())
	if err == nil {
		scanner, err := newFileScanner(file)
		if err == nil {
			return scanner, nil
		}

		_ = file.Close()
	}

	idx, err := a.getIndex()
	if err != nil {
		return nil, err
	}

	return &sliceScanner{idx: idx}, nil
}

// readOnlyNow reports whether the archive is closed to writes, and so
// whether its bytes can be read without finalizing it first.
func (a *archive) readOnlyNow() bool {
	a.mtx.RLock()
	defer a.mtx.RUnlock()

	return a.readOnly
}

// readSpan reads length bytes at offset, which may cover several records,
// once the archive's header is checked. The bool reports whether the read
// was lock-free.
func (a *archive) readSpan(offset, length int64) ([]byte, bool, error) {
	if err := a.checkHeader(); err != nil {
		return nil, false, err
	}

	return a.readAt(offset, length)
}

func (a *archive) readAt(offset, length int64) ([]byte, bool, error) {
	buf := make([]byte, length)

	if readerAt, ok := a.readFile.(io.ReaderAt); ok {
		_, err := readerAt.ReadAt(buf, offset)
		if err != nil {
			return nil, true, errors.Wrap(err, "Failed filling buffer")
		}

		return buf, true, nil
	}

	// Seek and Read share the file position, so the fallback needs the
	// exclusive lock
	a.mtx.Lock()
	defer a.mtx.Unlock()

	_, err := a.readFile.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, false, errors.Wrap(err, "Failed seeking in file")
	}

	_, err = io.ReadFull(a.readFile, buf)
	if err != nil {
		return nil, false, errors.Wrap(err, "Failed filling buffer")
	}

	return buf, false, nil
}

// maxHeaderSize bounds the object header of a record; the ones goback
// writes take a few hundred bytes.
const maxHeaderSize = 64 << 10

// payloadReserve bounds what readPayload reserves before the bytes arrive.
const payloadReserve = 64 << 20

// splitRecord splits a record into its object header and payload bytes.
func splitRecord(record []byte) ([]byte, []byte, error) {
	size, consumed := proto.DecodeVarint(record)
	if consumed <= 0 || uint64(len(record)-consumed) < size {
		return nil, nil, errors.New("record is truncated")
	}

	end := consumed + int(size)

	return record[consumed:end], record[end:], nil
}

// readPayload reads the size bytes of a payload from r, growing what it
// holds as they arrive once they exceed payloadReserve.
func readPayload(r io.Reader, size uint64) ([]byte, error) {
	if size <= payloadReserve {
		buf := make([]byte, size)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}

		return buf, nil
	}

	var buf bytes.Buffer
	n, err := io.CopyN(&buf, r, int64(size))
	if uint64(n) != size {
		if err == nil || err == io.EOF {
			err = io.ErrUnexpectedEOF
		}

		return nil, err
	}

	return buf.Bytes(), nil
}

// readHeader decodes the object header of one index record.
func (a *archive) readHeader(loc *IndexRecord) (*proto.ObjectHeader, error) {
	buf, _, err := a.readRecord(loc)
	if err != nil {
		return nil, err
	}

	header, _, err := splitRecord(buf)
	if err != nil {
		return nil, errors.Wrapf(err, "reading a header in %s", a.name)
	}

	return proto.NewObjectHeaderFromBytes(header)
}

func (a *archive) getRaw(ctx context.Context, ref *proto.Ref, loc *IndexRecord) (*proto.Object, error) {
	ctx, span := tracer.Start(ctx, "archive.getRaw")
	defer span.End()

	start := time.Now()
	buf, lockFree, err := a.readRecord(loc)
	if err != nil {
		return nil, err
	}
	span.SetAttributes(attribute.Bool("lock-free", lockFree))

	return a.objectFromRecord(ctx, ref, loc, buf, float64(time.Since(start))/float64(time.Millisecond))
}

// objectFromRecord decodes a record already read from the archive, which
// is how a caller that fetched several records at once opens each one.
func (a *archive) objectFromRecord(ctx context.Context, ref *proto.Ref, loc *IndexRecord, buf []byte, readLatency float64) (*proto.Object, error) {
	header, payload, err := splitRecord(buf)
	if err != nil {
		return nil, errors.Wrapf(err, "reading object %x from archive %s", ref.Hash, a.name)
	}

	hdr, err := proto.NewObjectHeaderFromBytes(header)
	if err != nil {
		return nil, errors.Wrap(err, "Failed parsing object header")
	}

	if !bytes.Equal(hdr.Ref.Hash, ref.Hash) {
		return nil, errors.New("Object doesn't match Ref, index probably corrupted")
	}

	if hdr.Type == proto.ObjectType_TOMBSTONE {
		return nil, errors.Errorf("ref %x is a tombstone and has no object", ref.Hash)
	}

	attrs := metric.WithAttributes(keyObjectType.String(hdr.Type.String()))
	getObjectSize.Record(ctx, int64(hdr.Size), attrs)
	archiveReadLatency.Record(ctx, readLatency, attrs)
	archiveReadSize.Record(ctx, int64(loc.Length), attrs)

	stored, err := openAtRest(a.atRest, hdr, payload)
	if err != nil {
		return nil, errors.Wrapf(err, "reading object %x from archive %s", ref.Hash, a.name)
	}

	obj, err := proto.ObjectFromStored(hdr, stored)
	if err != nil {
		return nil, errors.Wrapf(err, "reading object %x from archive %s", ref.Hash, a.name)
	}

	return obj, nil
}

func (a *archive) putTombstone(ctx context.Context, ref *proto.Ref, erase bool) error {
	hdr := &proto.ObjectHeader{
		Ref:          proto.TombstoneRef(ref),
		TombstoneFor: ref,
		Type:         proto.ObjectType_TOMBSTONE,
		Erase:        erase,
	}

	return a.putRaw(ctx, hdr, nil)
}

func (a *archive) putRaw(ctx context.Context, hdr *proto.ObjectHeader, bytes []byte) error {
	return a.putVersioned(ctx, hdr, bytes, nil)
}

// putVersioned writes an object whose record keeps the version carried,
// or gets the archive's own when carried is nil.
func (a *archive) putVersioned(ctx context.Context, hdr *proto.ObjectHeader, bytes []byte, carried *Version) error {
	a.mtx.Lock()
	defer a.mtx.Unlock()

	if a.readOnly {
		panic("Cannot write to readonly archive")
	}

	ref := hdr.Ref

	if hdr.Type == proto.ObjectType_INVALID {
		return errors.New("object header has no type")
	}

	if !ref.Valid() {
		return errors.Errorf("object header ref has %d bytes, want %d", len(ref.GetHash()), proto.HashSize)
	}

	hdr.Predecessor = a.last
	hdr.AtRestKeyId = nil

	if a.atRest != nil && len(bytes) > 0 {
		sealed, err := a.atRest.seal(hdr, bytes)
		if err != nil {
			return err
		}

		bytes, hdr.AtRestKeyId = sealed, a.atRest.id
	}

	hdr.Size = uint64(len(bytes))

	// an existing timestamp says when the object first entered the store;
	// rewrites carry it over
	if hdr.Timestamp == nil {
		hdr.Timestamp = timestamppb.Now()
	}

	hdrBytes := proto.Bytes(hdr)
	hdrBytesSize := uint64(len(hdrBytes))

	// record layout: varint header size, header, payload
	hdrBytes = append(proto.EncodeVarint(nil, hdrBytesSize), hdrBytes...)
	data := append(hdrBytes, bytes...)

	start := time.Now()
	_, err := a.writeFile.Write(data)
	if err != nil {
		return errors.Wrap(err, "Failed writing header")
	}

	writeLatency := float64(time.Since(start)) / float64(time.Millisecond)

	record := &IndexRecord{
		Offset: uint32(a.size),
		Length: uint32(len(hdrBytes) + len(bytes)),
		Type:   uint32(hdr.Type),
	}

	copy(record.Sum[:], ref.Hash)

	if carried != nil {
		*record = record.Carry(*carried)
	}

	a.writeIndex[string(ref.Hash)] = record

	a.size += uint64(record.Length)
	a.last = ref

	attrs := metric.WithAttributes(keyObjectType.String(hdr.Type.String()))
	putObjectSize.Record(ctx, int64(hdr.Size), attrs)
	archiveWriteLatency.Record(ctx, writeLatency, attrs)
	archiveWriteSize.Record(ctx, int64(len(data)), attrs)

	return nil
}

// exactAhead is the most readExactly allocates before the bytes arrive.
const exactAhead = 4 << 20

// readExactly reads n bytes of r, allocating for those past exactAhead only
// as they arrive, so a corrupt n fails at the end of r.
func readExactly(r io.Reader, n uint64) ([]byte, error) {
	buf := make([]byte, min(n, exactAhead))
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	if n == uint64(len(buf)) {
		return buf, nil
	}

	rest, err := io.ReadAll(io.LimitReader(r, int64(min(n-uint64(len(buf)), math.MaxInt64))))
	if err != nil {
		return nil, err
	}

	if uint64(len(buf)+len(rest)) != n {
		return nil, io.ErrUnexpectedEOF
	}

	return append(buf, rest...), nil
}

type loadPredicate func(*proto.ObjectHeader) bool

func loadAll(hdr *proto.ObjectHeader) bool  { return true }
func loadNone(hdr *proto.ObjectHeader) bool { return false }
func loadType(t proto.ObjectType) loadPredicate {
	return func(hdr *proto.ObjectHeader) bool {
		return hdr.Type == t
	}
}

func (a *archive) foreachReader(reader io.Reader, load loadPredicate, callback func(hdr *proto.ObjectHeader, bytes []byte, offset uint32, length uint32) error) error {
	return a.walkReader(reader, load, true, callback)
}

// foreachStoredReader is foreachReader handing over the bytes as stored,
// still sealed at rest; the caller opens them with openAtRest.
func (a *archive) foreachStoredReader(reader io.Reader, load loadPredicate, callback func(hdr *proto.ObjectHeader, bytes []byte, offset uint32, length uint32) error) error {
	return a.walkReader(reader, load, false, callback)
}

func (a *archive) walkReader(reader io.Reader, load loadPredicate, open bool, callback func(hdr *proto.ObjectHeader, bytes []byte, offset uint32, length uint32) error) error {
	bufReader := bufio.NewReaderSize(reader, 1024*16)

	fileHeader := make([]byte, archiveHeaderSize)
	_, err := io.ReadFull(bufReader, fileHeader)
	if err != nil {
		return errors.Wrap(err, "reading archive header")
	}

	err = checkArchiveHeader(fileHeader)
	if err != nil {
		return err
	}

	offset := uint32(archiveHeaderSize)

	for {
		hdrSizeBytes, err := bufReader.Peek(varIntMaxSize)
		if len(hdrSizeBytes) != varIntMaxSize {
			if err != io.EOF {
				return err
			}

			// a record is longer than a varint, so only nothing at all
			// left is the end
			if len(hdrSizeBytes) == 0 {
				break
			}
		}

		hdrSize, consumed := proto.DecodeVarint(hdrSizeBytes)
		if consumed <= 0 || hdrSize > maxHeaderSize {
			return errors.Errorf("Failed reading object header: malformed size at %d", offset)
		}

		_, err = bufReader.Discard(consumed)
		if err != nil {
			return err
		}

		hdrBytes, err := readExactly(bufReader, hdrSize)
		if err != nil {
			return errors.Wrap(err, "Failed reading object header")
		}

		hdr, err := proto.NewObjectHeaderFromBytes(hdrBytes)
		if err != nil {
			return errors.Wrap(err, "Failed parsing object header")
		}

		var objOffset = offset
		var objectBytes []byte
		if hdr.Size > math.MaxUint32 {
			return errors.Errorf("Failed reading object: %d bytes at %d", hdr.Size, offset)
		}

		if load(hdr) {
			objectBytes, err = readPayload(bufReader, hdr.Size)
			if err != nil {
				return errors.Wrap(err, "Failed reading object")
			}

			if open {
				objectBytes, err = openAtRest(a.atRest, hdr, objectBytes)
				if err != nil {
					return err
				}
			}
		} else if n, err := bufReader.Discard(int(hdr.Size)); uint64(n) != hdr.Size {
			if err == nil || err == io.EOF {
				err = io.ErrUnexpectedEOF
			}

			return errors.Wrap(err, "Failed skipping object")
		}

		offset += uint32(consumed) + uint32(hdr.Size) + uint32(hdrSize)
		length := uint32(consumed) + uint32(hdr.Size) + uint32(hdrSize)

		err = callback(hdr, objectBytes, objOffset, length)
		if err != nil {
			return err
		}
	}

	return nil
}

// foreachOfType calls back for every object of type t the archive's index
// lists, reading only their records, and with their bytes when load is
// set. It reports false, having called nothing, when the archive cannot be
// read that way: it is still being written, or its index does not record
// types.
func (a *archive) foreachOfType(t proto.ObjectType, load bool, callback func(hdr *proto.ObjectHeader, bytes []byte, offset uint32, length uint32) error) (bool, error) {
	if !a.readOnlyNow() {
		return false, nil
	}

	idx, err := a.getIndex()
	if err != nil {
		return false, err
	}

	var records []*IndexRecord
	for i := range idx {
		if idx[i].Type == uint32(proto.ObjectType_INVALID) {
			return false, nil
		}

		if idx[i].Type == uint32(t) {
			records = append(records, &idx[i])
		}
	}

	sort.Slice(records, func(i, j int) bool { return records[i].Offset < records[j].Offset })

	for start := 0; start < len(records); {
		end, span := start+1, int64(records[start].Length)
		from := int64(records[start].Offset)

		for end < len(records) {
			previous := int64(records[end-1].Offset) + int64(records[end-1].Length)
			grown := int64(records[end].Offset) + int64(records[end].Length) - from

			if int64(records[end].Offset)-previous > walkGap || grown > walkSpan {
				break
			}

			span = grown
			end++
		}

		buf, _, err := a.readSpan(from, span)
		if err != nil {
			return true, errors.Wrapf(err, "reading %d bytes at %d of %s", span, from, a.name)
		}

		for _, rec := range records[start:end] {
			record := buf[int64(rec.Offset)-from : int64(rec.Offset)-from+int64(rec.Length)]
			header, payload, err := splitRecord(record)
			if err != nil {
				return true, errors.Wrapf(err, "reading a header in %s", a.name)
			}

			hdr, err := proto.NewObjectHeaderFromBytes(header)
			if err != nil {
				return true, errors.Wrapf(err, "parsing a header in %s", a.name)
			}

			var data []byte
			if load {
				data, err = openAtRest(a.atRest, hdr, payload)
				if err != nil {
					return true, err
				}
			}

			if err := callback(hdr, data, rec.Offset, rec.Length); err != nil {
				return true, err
			}
		}

		start = end
	}

	return true, nil
}

// walkGap is how far apart two records of a typed walk may sit and still
// be read together; walkSpan bounds what one such read covers.
const (
	walkGap  = 64 << 10
	walkSpan = 8 << 20
)

func (a *archive) foreach(load loadPredicate, callback func(hdr *proto.ObjectHeader, bytes []byte, offset uint32, length uint32) error) error {
	return a.walk(load, true, callback)
}

// foreachStored is foreach handing over the bytes as stored, still sealed
// at rest; the caller opens them with openAtRest.
func (a *archive) foreachStored(load loadPredicate, callback func(hdr *proto.ObjectHeader, bytes []byte, offset uint32, length uint32) error) error {
	return a.walk(load, false, callback)
}

func (a *archive) walk(load loadPredicate, open bool, callback func(hdr *proto.ObjectHeader, bytes []byte, offset uint32, length uint32) error) error {
	file, err := a.storage.Open(a.archiveName())
	if err != nil {
		return errors.Wrap(err, "Couldn't open file for streaming")
	}

	defer file.Close()

	if writerTo, ok := file.(io.WriterTo); ok {
		pReader, pWriter := io.Pipe()
		var grp errgroup.Group

		grp.Go(func() error {
			_, wErr := writerTo.WriteTo(pWriter)
			pWriter.CloseWithError(wErr)

			return wErr
		})

		grp.Go(func() error {
			// closing the read side unblocks the writer if the walk stops early
			rErr := a.walkReader(pReader, load, open, callback)
			pReader.CloseWithError(rErr)

			return rErr
		})

		wErr := grp.Wait()

		if wErr != nil {
			return errors.Wrap(wErr, "Couldn't stream file")
		}

		return nil
	}

	return a.walkReader(file, load, open, callback)
}

// readAll returns the archive file's bytes, read in one request where the
// storage streams files.
func (a *archive) readAll() ([]byte, error) {
	file, err := a.storage.Open(a.archiveName())
	if err != nil {
		return nil, errors.Wrapf(err, "opening archive %s", a.name)
	}
	defer file.Close()

	// a growing buffer would double to read the end of the file
	buf := make([]byte, a.size)
	if _, err := io.ReadFull(file, buf); err != nil {
		return nil, errors.Wrapf(err, "reading archive %s", a.name)
	}

	return buf, nil
}

// storeReadIndex writes idx as the archive's index file and returns the
// bytes it took. The caller holds the archive's lock.
func (a *archive) storeReadIndex(idx IndexFile) (int64, error) {
	sort.Sort(idx)

	idxFile, err := a.storage.Create(a.indexName())
	if err != nil {
		return 0, errors.Wrap(err, "Failed creating index file")
	}

	n, err := idx.WriteTo(idxFile)
	if err != nil {
		return 0, errors.Wrap(err, "Couldn't encode index file")
	}

	if err := idxFile.Close(); err != nil {
		return n, errors.Wrap(err, "Failed closing index file")
	}

	if info, _, ok := storedInfo(idxFile); ok {
		// every index keeps it to the microsecond
		a.indexModified = info.ModTime().Truncate(time.Microsecond)
	}

	return n, nil
}

func (a *archive) storeIndex() (IndexFile, int64, error) {
	idx := make(IndexFile, 0, len(a.writeIndex))

	for _, loc := range a.writeIndex {
		idx = append(idx, *loc)
	}

	n, err := a.storeReadIndex(idx)

	return idx, n, err
}

// Close finalizes a still-open writer, which reopens the reader, and then
// closes the reader.
func (a *archive) Close() error {
	_, err := a.CloseWriter()
	if err != nil && err != errAlreadyClosed {
		return err
	}

	return a.CloseReader()
}

func (a *archive) CloseReader() error {
	return a.readFile.Close()
}

func (a *archive) CloseWriter() (IndexFile, error) {
	index, stored, err := a.closeWriter()
	if err == nil && a.stored != nil {
		a.stored(a, stored)
	}

	return index, err
}

// closeWriter closes the writer and stores the index, returning the bytes
// both take up in the storage.
func (a *archive) closeWriter() (IndexFile, int64, error) {
	a.mtx.Lock()
	defer a.mtx.Unlock()

	if a.readOnly {
		return nil, 0, errAlreadyClosed
	}

	err := a.writeFile.Close()
	if err != nil {
		return nil, 0, errors.Wrap(err, "Failed closing file")
	}

	a.readOnly = true

	// the read handle may have been opened against the upload in flight;
	// reads must now come from what the storage holds
	_ = a.readFile.Close()
	if written, ok := a.writeFile.(*hashedWriter); ok {
		// what was uploaded says how big it is without asking the storage
		a.readFile, err = OpenListed(a.storage, ListedFile{Name: a.archiveName(), Size: written.written})
	} else {
		a.readFile, err = a.storage.Open(a.archiveName())
	}
	if err != nil {
		return nil, 0, errors.Wrap(err, "Failed reopening archive for reading")
	}

	err = a.verifyUpload()
	if err != nil {
		return nil, 0, err
	}

	index, stored, err := a.storeIndex()
	if written, ok := a.writeFile.(*hashedWriter); ok {
		stored += written.written
	}

	if err == nil {
		err = a.sync()
	}

	return index, stored, err
}

// sync makes the archive and its index durable on a storage that needs
// it, before anything records what they hold.
func (a *archive) sync() error {
	syncer, ok := a.storage.(Syncer)
	if !ok {
		return nil
	}

	for _, name := range []string{a.archiveName(), a.indexName()} {
		if err := syncer.Sync(name); err != nil {
			return errors.Wrapf(err, "syncing %s", name)
		}
	}

	return nil
}
