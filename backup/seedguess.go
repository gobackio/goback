package backup

import (
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/gobackio/goback/proto"
)

// SeedMode says how a seed directory is read.
type SeedMode int

const (
	// SeedGuess lists the directory's names and sizes up front and reads,
	// just before each file is restored, the few files most likely to hold
	// its parts: the one at the same path, those of the same name, then
	// those of the same extension and about the same size.
	SeedGuess SeedMode = iota
	// SeedFull reads every file under the directory up front, so a part is
	// found whatever file holds it.
	SeedFull
)

// DefaultSeedMode is the SeedMode used when nothing says otherwise.
const DefaultSeedMode = SeedGuess

// String is the mode's name, as ParseSeedMode reads it.
func (m SeedMode) String() string {
	if m == SeedFull {
		return "full"
	}

	return "guess"
}

// ParseSeedMode reads "guess" or "full".
func ParseSeedMode(name string) (SeedMode, error) {
	switch name {
	case "guess":
		return SeedGuess, nil
	case "full":
		return SeedFull, nil
	}

	return 0, fmt.Errorf("unknown seed mode %q: use guess or full", name)
}

// AddSeed adds the seed at path: a directory as mode says, a file always
// read up front.
func (s *SeedMap) AddSeed(path string, mode SeedMode) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}

	if !info.IsDir() || mode == SeedFull {
		return s.Add(path)
	}

	var listed []seedStat

	err = filepath.WalkDir(path, func(file string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.Type().IsRegular() {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		rel, err := filepath.Rel(path, file)
		if err != nil {
			return err
		}

		listed = append(listed, newSeedStat(file, filepath.ToSlash(rel), info.Size()))

		return nil
	})
	if err != nil {
		return err
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.listing.add(listed...)

	return nil
}

// guessesPerFile bounds the seed files read for one restored file.
const guessesPerFile = 3

// guess reads the seed files most likely to hold the parts of the file at
// the slash-separated rel under the directory restored into, recorded as
// stat, each seed file once. It stops at a file of the same size and the
// same path or name.
func (s *SeedMap) guess(rel string, stat *proto.FileInfo) {
	if s == nil {
		return
	}

	name := string(stat.GetName())
	if name == "" {
		name = path.Base(rel)
	}

	size := stat.GetSize()

	s.mtx.Lock()
	candidates := s.listing.candidates(rel, name, size, guessesPerFile)
	s.mtx.Unlock()

	for _, c := range candidates {
		s.mtx.Lock()
		seen := s.indexed[c.path]
		s.indexed[c.path] = true
		s.mtx.Unlock()

		if !seen {
			_ = s.addFile(c.path)
		}

		if c.size == size && (c.rel == rel || c.name == name) {
			return
		}
	}
}

// seedStat is a file of a seed directory, as listed.
type seedStat struct {
	path string
	// rel is the slash-separated path under its seed directory
	rel  string
	name string
	ext  string
	size int64
}

func newSeedStat(file, rel string, size int64) seedStat {
	name := path.Base(rel)

	return seedStat{path: file, rel: rel, name: name, ext: strings.ToLower(path.Ext(name)), size: size}
}

// seedListing is the files of the seed directories a guess picks from.
type seedListing struct {
	files  []seedStat
	byRel  map[string][]int
	byName map[string][]int
	// byExt holds each extension's files by size
	byExt map[string][]int
}

func (l *seedListing) add(files ...seedStat) {
	if l.byRel == nil {
		l.byRel, l.byName, l.byExt = map[string][]int{}, map[string][]int{}, map[string][]int{}
	}

	for _, f := range files {
		i := len(l.files)
		l.files = append(l.files, f)
		l.byRel[f.rel] = append(l.byRel[f.rel], i)
		l.byName[f.name] = append(l.byName[f.name], i)
		l.byExt[f.ext] = append(l.byExt[f.ext], i)
	}

	for _, same := range l.byExt {
		sort.SliceStable(same, func(a, b int) bool { return l.files[same[a]].size < l.files[same[b]].size })
	}
}

// candidates returns up to limit seed files, best first, for a file at
// rel named name of size bytes: those at the same path, those of the same
// name, then those of the same extension within a tenth of its size, the
// closest first.
func (l *seedListing) candidates(rel, name string, size int64, limit int) []seedStat {
	var picked []seedStat

	taken := map[int]bool{}

	take := func(i int) bool {
		if !taken[i] {
			taken[i] = true
			picked = append(picked, l.files[i])
		}

		return len(picked) < limit
	}

	for _, i := range l.byRel[rel] {
		if !take(i) {
			return picked
		}
	}

	for _, i := range l.byName[name] {
		if !take(i) {
			return picked
		}
	}

	same := l.byExt[strings.ToLower(path.Ext(name))]
	slack := size / 10

	// walk outwards from size, nearest first, while within the slack
	hi := sort.Search(len(same), func(k int) bool { return l.files[same[k]].size >= size })
	lo := hi - 1

	for lo >= 0 || hi < len(same) {
		var k int

		switch {
		case lo < 0:
			k, hi = hi, hi+1
		case hi >= len(same):
			k, lo = lo, lo-1
		case l.files[same[hi]].size-size <= size-l.files[same[lo]].size:
			k, hi = hi, hi+1
		default:
			k, lo = lo, lo-1
		}

		if d := l.files[same[k]].size - size; d > slack || -d > slack {
			break
		}

		if !take(same[k]) {
			break
		}
	}

	return picked
}
