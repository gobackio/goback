package backup

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/gobackio/goback/backup/chunker"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"
)

// SeedMap locates chunks on the local disk by the ref they would be
// stored under, so a restore takes them from there instead of the store:
// chunks of the seeds Add and AddSeed name and of every file the restore
// itself has written. It is safe for concurrent use.
type SeedMap struct {
	key *storekey.Key

	mtx     sync.Mutex
	entries map[string]seedEntry
	listing seedListing
	indexed map[string]bool
	// pending holds the parts a restore is fetching, each closed once it
	// lands or the fetch gives up
	pending map[string]chan struct{}
	// written holds the refs of the files a restore wrote whole
	written map[string]bool
}

// seedEntry is about 100 bytes of memory with its key, per distinct part.
type seedEntry struct {
	file   *seedFile
	offset int64
	length int64
}

// seedFile is a file entries are read from; a restored file moves from its
// partial file to its own path once it is renamed into place.
type seedFile struct {
	path     string
	restored bool
}

// NewSeedMap returns an empty map; key is the store key the files were
// backed up under, nil for a plaintext store.
func NewSeedMap(key *storekey.Key) *SeedMap {
	return &SeedMap{
		key:     key,
		entries: make(map[string]seedEntry),
		indexed: make(map[string]bool),
		pending: make(map[string]chan struct{}),
		written: make(map[string]bool),
	}
}

// Len is the number of chunks the map locates.
func (s *SeedMap) Len() int {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return len(s.entries)
}

// Add indexes path, or every regular file under it, cut with the chunker
// the backup uses.
func (s *SeedMap) Add(path string) error {
	return filepath.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.Type().IsRegular() {
			return nil
		}

		return s.addFile(path)
	})
}

func (s *SeedMap) addFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	seed := &seedFile{path: path}

	var cdc chunker.FastCDC
	buf := make([]byte, chunker.MaxSize)
	filled, scanned := 0, 0
	var offset int64
	eof := false

	for {
		if !eof && filled < len(buf) {
			n, err := io.ReadFull(file, buf[filled:])
			filled += n

			switch err {
			case nil:
			case io.EOF, io.ErrUnexpectedEOF:
				eof = true
			default:
				return err
			}
		}

		if filled == 0 {
			return nil
		}

		cut := cdc.Scan(buf[:filled], scanned)
		if cut == 0 {
			if !eof {
				scanned = filled
				continue
			}

			cut = filled
			cdc.Reset()
		}

		s.record(seed, offset, buf[:cut])

		offset += int64(cut)
		copy(buf, buf[cut:filled])
		filled -= cut
		scanned = 0
	}
}

func (s *SeedMap) record(file *seedFile, offset int64, chunk []byte) {
	var ref *proto.Ref
	if s.key != nil {
		ref = BlobRef(s.key, chunk)
	} else {
		ref = proto.NewObject(&proto.Blob{Data: chunk}).Ref()
	}

	s.landed(string(ref.Hash), file, offset, int64(len(chunk)))
}

// landed records that file holds the part hash names at offset and lets
// whoever waits for the part go on.
func (s *SeedMap) landed(hash string, file *seedFile, offset, length int64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if _, seen := s.entries[hash]; !seen {
		s.entries[hash] = seedEntry{file: file, offset: offset, length: length}
	}

	if ch, ok := s.pending[hash]; ok {
		close(ch)
		delete(s.pending, hash)
	}
}

// read returns the bytes recorded for the part's ref, unverified, and
// whether a restore wrote them.
func (s *SeedMap) read(part *proto.FilePart) ([]byte, bool, bool) {
	key := string(part.Ref.Hash)

	// a restored file can be renamed between the lookup and the open
	for range 2 {
		s.mtx.Lock()
		entry, ok := s.entries[key]
		var path string
		if ok {
			path = entry.file.path
		}
		s.mtx.Unlock()

		if !ok || uint64(entry.length) != part.Length {
			return nil, false, false
		}

		buf, err := readAt(path, entry.offset, entry.length)
		if err == nil {
			return buf, entry.file.restored, true
		}

		if !errors.Is(err, fs.ErrNotExist) {
			break
		}
	}

	return nil, false, false
}

func readAt(path string, offset, length int64) ([]byte, error) {
	file, err := openShared(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	buf := make([]byte, length)
	if _, err := file.ReadAt(buf, offset); err != nil {
		return nil, err
	}

	return buf, nil
}

// forget drops the entry for a part whose bytes no longer hash to it, so
// the next copy of it to land is recorded instead.
func (s *SeedMap) forget(part *proto.FilePart) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	delete(s.entries, string(part.Ref.Hash))
}

var landedAlready = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)

	return ch
}()

// claim reserves the fetch of the part hash names for the caller, who
// calls release once the fetch ends, landed or not. When the map holds the
// part, or another fetch holds it, it returns a channel closed once that
// fetch ends instead, and mine is false.
func (s *SeedMap) claim(hash string) (ch chan struct{}, mine bool) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if _, ok := s.entries[hash]; ok {
		return landedAlready, false
	}

	if ch, ok := s.pending[hash]; ok {
		return ch, false
	}

	ch = make(chan struct{})
	s.pending[hash] = ch

	return ch, true
}

// release ends a claim whose part did not land.
func (s *SeedMap) release(hash string, ch chan struct{}) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.pending[hash] == ch {
		close(ch)
		delete(s.pending, hash)
	}
}

// restoring is the file a restore assembles at partial.
func (s *SeedMap) restoring(partial string) *seedFile {
	return &seedFile{path: partial, restored: true}
}

// moved records that file was renamed to path.
func (s *SeedMap) moved(file *seedFile, path string) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	file.path = path
}

// wrote records that the file ref names was written whole.
func (s *SeedMap) wrote(ref *proto.Ref) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.written[string(ref.GetHash())] = true
}

// wasWritten reports whether a restore wrote the file ref names whole.
func (s *SeedMap) wasWritten(ref *proto.Ref) bool {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return s.written[string(ref.GetHash())]
}
