package pack

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/bits-and-blooms/bitset"
)

// GCExt is the extension of the mark result stored beside an archive's index.
const GCExt = ".gc"

const (
	gcStateName = "gc-state.json"
	gcMagic     = "GOBACKGC_0002"
)

// gcState is the store's record of its last completed generation.
type gcState struct {
	Generation uint64    `json:"generation"`
	Snapshot   time.Time `json:"snapshot"`
	Swept      bool      `json:"swept"`
	// Condemned are the versions of the committed archives there were once
	// the generation's tombstones were stored, and of the tombstones its
	// snapshot held; only tombstones of these versions, wherever compaction
	// moved them, let a later generation drop a copy.
	Condemned []time.Time `json:"condemned,omitempty"`
	// Horizon are the sessions that had begun and not ended once the
	// generation's tombstones were stored. A later generation drops
	// nothing until every one of them has ended.
	Horizon []string `json:"horizon,omitempty"`
}

// gcFile is one archive's mark result: which index positions were reachable
// in the generation that wrote it and in the one before.
type gcFile struct {
	Generation uint64
	Snapshot   time.Time
	// Current holds one bit per .idx position, set when the object was reachable.
	Current *bitset.BitSet
	// Previous is the Current of generation-1, nil when the archive was not
	// part of that generation's snapshot.
	Previous    *bitset.BitSet
	DeadObjects uint64
	DeadBytes   uint64
	// DeadSince is the snapshot time since which the archive, or the archives
	// it was rewritten from, has continuously held unmarked objects; zero
	// when everything is marked.
	DeadSince time.Time
	// Dead holds the sorted 8-byte prefixes of the refs unmarked in Current.
	Dead []uint64
	// Erase is set when the archive holds objects only an erased commit
	// names; the sweep selects it as soon as they are dead.
	Erase bool
	// Death is how soon the generation expected the object at each .idx
	// position to die.
	Death deaths
}

// death is how soon the object at pos was expected to die.
func (g *gcFile) death(pos int) deathClass {
	return g.Death.at(pos)
}

// deaths holds the deathClass of each index position, two to a byte; nil
// holds never for every one.
type deaths []byte

func (d deaths) at(pos int) deathClass {
	if pos < 0 || pos/2 >= len(d) {
		return never
	}

	return deathClass(d[pos/2] >> (4 * (pos % 2)) & 0xf)
}

// set records class c at pos of count positions.
func (d *deaths) set(pos, count int, c deathClass) {
	if c == never && *d == nil {
		return
	}

	if *d == nil {
		*d = make(deaths, (count+1)/2)
	}

	shift := 4 * (pos % 2)
	(*d)[pos/2] = (*d)[pos/2]&^(0xf<<shift) | byte(c)<<shift
}

// dead reports whether the object at pos was unmarked in both generations.
func (g *gcFile) dead(pos int) bool {
	return g.Previous != nil && !g.Previous.Test(uint(pos)) && !g.Current.Test(uint(pos))
}

// candidate reports whether the ref may be unmarked in Current.
func (g *gcFile) candidate(hash []byte) bool {
	if len(hash) < 8 {
		return false
	}

	prefix := binary.BigEndian.Uint64(hash[:8])
	i := sort.Search(len(g.Dead), func(i int) bool { return g.Dead[i] >= prefix })

	return i < len(g.Dead) && g.Dead[i] == prefix
}

func unixNano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}

	return t.UnixNano()
}

func fromUnixNano(ns int64) time.Time {
	if ns == 0 {
		return time.Time{}
	}

	return time.Unix(0, ns).UTC()
}

func (g *gcFile) WriteTo(w io.Writer) (int64, error) {
	cw := &gcCounter{writer: w}
	bw := bufio.NewWriter(cw)

	write := func(v interface{}) error {
		return binary.Write(bw, binary.BigEndian, v)
	}

	if _, err := bw.WriteString(gcMagic); err != nil {
		return cw.count, err
	}

	hasPrevious := uint8(0)
	if g.Previous != nil {
		hasPrevious = 1
	}

	erase := uint8(0)
	if g.Erase {
		erase = 1
	}

	for _, v := range []interface{}{
		g.Generation, unixNano(g.Snapshot), g.DeadObjects, g.DeadBytes, unixNano(g.DeadSince), hasPrevious, erase, uint32(len(g.Dead)),
	} {
		if err := write(v); err != nil {
			return cw.count, err
		}
	}

	if _, err := g.Current.WriteTo(bw); err != nil {
		return cw.count, err
	}

	if g.Previous != nil {
		if _, err := g.Previous.WriteTo(bw); err != nil {
			return cw.count, err
		}
	}

	for _, prefix := range g.Dead {
		if err := write(prefix); err != nil {
			return cw.count, err
		}
	}

	if err := write(uint32(len(g.Death))); err != nil {
		return cw.count, err
	}

	if _, err := bw.Write(g.Death); err != nil {
		return cw.count, err
	}

	return cw.count, bw.Flush()
}

func (g *gcFile) ReadFrom(r io.Reader) (int64, error) {
	cr := &countingReader{reader: bufio.NewReader(r)}

	magic := make([]byte, len(gcMagic))
	if _, err := io.ReadFull(cr, magic); err != nil {
		return cr.count, err
	}

	if !bytes.Equal(magic, []byte(gcMagic)) {
		return cr.count, errors.New("bad gc file magic")
	}

	var snapshot, deadSince int64
	var hasPrevious, erase uint8
	var dead uint32

	for _, v := range []interface{}{&g.Generation, &snapshot, &g.DeadObjects, &g.DeadBytes, &deadSince, &hasPrevious, &erase, &dead} {
		if err := binary.Read(cr, binary.BigEndian, v); err != nil {
			return cr.count, err
		}
	}

	g.Snapshot = fromUnixNano(snapshot)
	g.DeadSince = fromUnixNano(deadSince)
	g.Erase = erase == 1

	g.Current = bitset.New(0)
	if _, err := g.Current.ReadFrom(cr); err != nil {
		return cr.count, err
	}

	if hasPrevious == 1 {
		g.Previous = bitset.New(0)
		if _, err := g.Previous.ReadFrom(cr); err != nil {
			return cr.count, err
		}
	}

	g.Dead = make([]uint64, dead)
	for i := range g.Dead {
		if err := binary.Read(cr, binary.BigEndian, &g.Dead[i]); err != nil {
			return cr.count, err
		}
	}

	// deaths come last, so a result may end without them and a reader
	// that knows none stops before them
	var n uint32
	if err := binary.Read(cr, binary.BigEndian, &n); errors.Is(err, io.EOF) {
		return cr.count, nil
	} else if err != nil {
		return cr.count, err
	}

	if n > 0 {
		g.Death = make(deaths, n)
		if _, err := io.ReadFull(cr, g.Death); err != nil {
			return cr.count, err
		}
	}

	return cr.count, nil
}

type gcCounter struct {
	writer io.Writer
	count  int64
}

func (c *gcCounter) Write(p []byte) (int, error) {
	n, err := c.writer.Write(p)
	c.count += int64(n)

	return n, err
}

type countingReader struct {
	reader io.Reader
	count  int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.reader.Read(p)
	c.count += int64(n)

	return n, err
}

func notExist(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, ErrFileNotFound)
}

// readGCFile loads the archive's mark result, nil when it has none.
func readGCFile(storage ArchiveStorage, name string) (*gcFile, error) {
	file, err := storage.Open(name + GCExt)

	return readGCFrom(file, err)
}

func readGCFrom(file File, err error) (*gcFile, error) {
	if err != nil {
		if notExist(err) {
			return nil, nil
		}

		return nil, err
	}
	defer file.Close()

	g := &gcFile{}
	if _, err := g.ReadFrom(file); err != nil {
		return nil, err
	}

	return g, nil
}

// markListing is what one listing found of the mark results, by archive.
type markListing map[string]ListedFile

// listMarks lists the mark results the storage holds.
func listMarks(storage ArchiveStorage) (markListing, error) {
	var files []ListedFile

	if lister, ok := storage.(InfoLister); ok {
		listed, err := lister.ListInfo(GCExt)
		if err != nil {
			return nil, err
		}

		files = listed
	} else {
		names, err := storage.List(GCExt)
		if err != nil {
			return nil, err
		}

		for _, name := range names {
			files = append(files, ListedFile{Name: name})
		}
	}

	marks := make(markListing, len(files))
	for _, file := range files {
		marks[strings.TrimSuffix(file.Name, GCExt)] = file
	}

	return marks, nil
}

// read loads the archive's mark result as the listing found it, nil when
// it found none; a nil listing asks the storage.
func (m markListing) read(storage ArchiveStorage, name string) (*gcFile, error) {
	if m == nil {
		return readGCFile(storage, name)
	}

	file, ok := m[name]
	if !ok {
		return nil, nil
	}

	return readGCFrom(OpenListed(storage, file))
}

func writeGCFile(storage ArchiveStorage, name string, g *gcFile) error {
	file, err := storage.Create(name + GCExt)
	if err != nil {
		return err
	}

	if _, err := g.WriteTo(file); err != nil {
		_ = file.Close()
		return err
	}

	return file.Close()
}

// LastCollected returns when the newest collection marked the store, zero
// when none has.
func (ps *PackStorage) LastCollected() (time.Time, error) {
	state, err := loadGCState(ps.storage)
	if err != nil || state == nil {
		return time.Time{}, err
	}

	return state.Snapshot, nil
}

func loadGCState(storage ArchiveStorage) (*gcState, error) {
	file, err := storage.Open(gcStateName)
	if err != nil {
		if notExist(err) {
			return nil, nil
		}

		return nil, err
	}
	defer file.Close()

	state := &gcState{}
	if err := json.NewDecoder(file).Decode(state); err != nil {
		return nil, err
	}

	return state, nil
}

func storeGCState(storage ArchiveStorage, state *gcState) error {
	file, err := storage.Create(gcStateName)
	if err != nil {
		return err
	}

	if err := json.NewEncoder(file).Encode(state); err != nil {
		_ = file.Close()
		return err
	}

	return file.Close()
}
