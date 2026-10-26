package pack

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

// RetiredExt marks one retired archive: <archive>.retired names the day
// it was retired. The store no longer loads the archive, a collection
// deletes it once the quarantine period has passed, and
// RestoreQuarantined puts it back until then.
const RetiredExt = ".retired"

// RetirementExt names what one rewrite retired: <id>.retirement lists the
// archives and the day they were retired, each retired as a RetiredExt
// marker of its own would have it.
const RetirementExt = ".retirement"

type retirement struct {
	Day string `json:"day"`
	// Stored is when the storage stored the retirement this one was cut
	// from, zero for one written by a rewrite
	Stored   time.Time `json:"stored,omitzero"`
	Archives []string  `json:"archives"`
}

// QuarantineExt names a retired archive's file moved out of the way:
// <archive>.goback.quarantine and so on, which the store neither lists
// nor loads, until a collection purges it or RestoreQuarantined moves it
// back.
const QuarantineExt = ".quarantine"

const quarantineDay = "2006-01-02"

// DefaultQuarantine is how long a retired archive is kept by default, so
// reads already under way, in this process or another, finish against it.
const DefaultQuarantine = 24 * time.Hour

// movedAfter is how long a retired archive's files stay where readers
// that loaded it before it was retired look for them.
const movedAfter = 24 * time.Hour

// archiveExts are the extensions of an archive's files. The data file
// moves into the quarantine last and out of it first, so an archive
// whose data file is quarantined has all its files there.
var archiveExts = []string{IndexExt, GCExt, CommittedExt, ArchiveSuffix}

// archiveNames lists the archives in the storage that are not retired,
// and, when the storage is an InfoLister, what the listing said of each
// archive file.
func (ps *PackStorage) archiveNames() ([]string, map[string]*ListedFile, error) {
	var files []ListedFile

	if lister, ok := ps.storage.(InfoLister); ok {
		listed, err := lister.ListInfo(ArchiveSuffix)
		if err != nil {
			return nil, nil, err
		}

		files = listed
	} else {
		matches, err := ps.storage.List(ArchiveSuffix)
		if err != nil {
			return nil, nil, err
		}

		for _, match := range matches {
			files = append(files, ListedFile{Name: match})
		}
	}

	retired, err := ps.retiredArchives()
	if err != nil {
		return nil, nil, err
	}

	_, lister := ps.storage.(InfoLister)
	names := make([]string, 0, len(files))
	listed := make(map[string]*ListedFile)

	for i, file := range files {
		name := strings.TrimSuffix(file.Name, ArchiveSuffix)
		if retired[name] {
			continue
		}

		names = append(names, name)
		if lister {
			listed[name] = &files[i]
		}
	}

	return names, listed, nil
}

// retiredArchives returns the names of the retired archives, by their own
// markers and by the retirements that list them.
func (ps *PackStorage) retiredArchives() (map[string]bool, error) {
	retired, err := ps.markerIDs(RetiredExt)
	if err != nil {
		return nil, err
	}

	retirements, err := ps.retirements()
	if err != nil {
		return nil, err
	}

	for _, stored := range retirements {
		for _, name := range stored.Archives {
			retired[name] = true
		}
	}

	return retired, nil
}

// storedRetirement is a retirement and the file it is stored in.
type storedRetirement struct {
	retirement
	file ListedFile
}

// since is the earliest the storage stored the archives as retired.
func (r storedRetirement) since() time.Time {
	if r.Stored.IsZero() {
		return r.file.Modified
	}

	return r.Stored
}

// retirements reads every retirement the storage holds. A retirement
// never changes once written, so each is read once.
func (ps *PackStorage) retirements() ([]storedRetirement, error) {
	files, err := ListInfo(ps.storage, RetirementExt)
	if err != nil {
		return nil, err
	}

	read := make([]storedRetirement, len(files))
	found := make([]bool, len(files))

	grp := new(errgroup.Group)
	grp.SetLimit(fileWorkers)

	for i, file := range files {
		grp.Go(func() error {
			r, err := ps.readRetirement(file)
			if notExist(err) {
				return nil
			}

			read[i], found[i] = storedRetirement{retirement: r, file: file}, err == nil

			return err
		})
	}

	if err := grp.Wait(); err != nil {
		return nil, err
	}

	kept := make(map[string]retirement, len(files))
	stored := read[:0]

	for i, r := range read {
		if found[i] {
			kept[r.file.Name] = r.retirement
			stored = append(stored, r)
		}
	}

	ps.retiredMtx.Lock()
	ps.retiredRead = kept
	ps.retiredMtx.Unlock()

	return stored, nil
}

func (ps *PackStorage) readRetirement(file ListedFile) (retirement, error) {
	ps.retiredMtx.Lock()
	r, ok := ps.retiredRead[file.Name]
	ps.retiredMtx.Unlock()

	if ok {
		return r, nil
	}

	opened, err := OpenListed(ps.storage, file)
	if err != nil {
		return retirement{}, err
	}
	defer opened.Close()

	if err := json.NewDecoder(opened).Decode(&r); err != nil {
		return retirement{}, fmt.Errorf("reading retirement %s: %w", file.Name, err)
	}

	return r, nil
}

// quarantineArchives retires the archives together, keeping their files
// for the quarantine period.
func (ps *PackStorage) quarantineArchives(names []string, now time.Time) error {
	return ps.storeRetirement(retirement{Day: now.UTC().Format(quarantineDay), Archives: names})
}

func (ps *PackStorage) storeRetirement(r retirement) error {
	id, err := uuid.NewRandom()
	if err != nil {
		return err
	}

	data, err := json.Marshal(r)
	if err != nil {
		return err
	}

	if err := ps.storage.CreateNew(id.String()+RetirementExt, data); err != nil {
		return fmt.Errorf("retiring %d archives: %w", len(r.Archives), err)
	}

	return nil
}

// cutRetirement takes the archives in out from a stored retirement. The
// rest stay retired as before, under a retirement of their own.
func (ps *PackStorage) cutRetirement(r storedRetirement, out map[string]bool) error {
	rest := retirement{Day: r.Day, Stored: r.since()}
	for _, name := range r.Archives {
		if !out[name] {
			rest.Archives = append(rest.Archives, name)
		}
	}

	if len(rest.Archives) == len(r.Archives) {
		return nil
	}

	if len(rest.Archives) > 0 {
		if err := ps.storeRetirement(rest); err != nil {
			return err
		}
	}

	if err := ps.storage.Delete(r.file.Name); err != nil && !notExist(err) {
		return err
	}

	return nil
}

// fileWorkers bounds the requests for single files a purge or listing has
// under way at once.
const fileWorkers = 16

// moveRetired moves the files of the archives retired at least movedAfter
// before now into the quarantine and returns how many archives it moved.
func (ps *PackStorage) moveRetired(now time.Time) (int, error) {
	retirements, err := ps.retirements()
	if err != nil {
		return 0, err
	}

	listed, err := ps.storage.List(ArchiveSuffix)
	if err != nil {
		return 0, err
	}

	inPlace := make(map[string]bool, len(listed))
	for _, name := range listed {
		inPlace[strings.TrimSuffix(name, ArchiveSuffix)] = true
	}

	var moved atomic.Int64

	grp := new(errgroup.Group)
	grp.SetLimit(fileWorkers)

	for _, r := range retirements {
		if now.Sub(r.since()) < movedAfter {
			continue
		}

		for _, name := range r.Archives {
			if !inPlace[name] {
				continue
			}

			delete(inPlace, name)

			grp.Go(func() error {
				if ps.quarantineFiles(name) {
					moved.Add(1)
				}

				return nil
			})
		}
	}

	_ = grp.Wait()

	return int(moved.Load()), nil
}

// quarantineFiles moves the archive's files into the quarantine and
// reports whether all of them moved.
func (ps *PackStorage) quarantineFiles(name string) bool {
	for _, ext := range archiveExts {
		err := ps.storage.Move(name+ext, name+ext+QuarantineExt)
		if err != nil && !notExist(err) {
			ps.logger.Warn("moving a retired archive's file into the quarantine failed", "file", name+ext, "err", err)
			return false
		}
	}

	return true
}

// unquarantineFiles moves the archive's files back out of the quarantine.
func (ps *PackStorage) unquarantineFiles(name string) error {
	for _, ext := range slices.Backward(archiveExts) {
		err := ps.storage.Move(name+ext+QuarantineExt, name+ext)
		if err != nil && !notExist(err) {
			return fmt.Errorf("moving %s out of the quarantine: %w", name+ext, err)
		}
	}

	return nil
}

// quarantined lists the files in the quarantine with their sizes.
func (ps *PackStorage) quarantined() (map[string]int64, error) {
	files, err := ListInfo(ps.storage, QuarantineExt)
	if err != nil {
		return nil, err
	}

	sizes := make(map[string]int64, len(files))
	for _, file := range files {
		sizes[file.Name] = file.Size
	}

	return sizes, nil
}

// purgeArchive deletes a retired archive's files that quarantined lists
// and, unless its data file is among them, those in place, and reports
// whether its index and data are gone.
func (ps *PackStorage) purgeArchive(name string, quarantined map[string]int64) bool {
	_, moved := quarantined[name+ArchiveSuffix+QuarantineExt]
	gone := moved || ps.deleteArchiveFiles(name)

	for _, ext := range archiveExts {
		file := name + ext + QuarantineExt
		if _, ok := quarantined[file]; !ok {
			continue
		}

		err := ps.storage.Delete(file)
		if err != nil && !notExist(err) {
			ps.logger.Warn("deleting a quarantined file failed", "file", file, "err", err)
			gone = gone && ext != IndexExt && ext != ArchiveSuffix
		}

		if err == nil && ext == ArchiveSuffix {
			ps.archivesDeleted([]string{name})
		}
	}

	return gone
}

// PurgeQuarantine deletes the archives retired longer than period ago and
// returns how many it deleted.
func (ps *PackStorage) PurgeQuarantine(period time.Duration, now time.Time) (int, error) {
	markers, err := ListInfo(ps.storage, RetiredExt)
	if err != nil {
		return 0, err
	}

	due := func(day time.Time) bool { return now.Sub(day) >= period+24*time.Hour }

	var purged atomic.Int64

	grp := new(errgroup.Group)
	grp.SetLimit(fileWorkers)

	for _, marker := range markers {
		// a marker is stored no earlier than the day it names, so only one
		// its storage day makes due is read; the day named still decides,
		// as the clock that named it may run ahead of the storage's
		if !due(dayOf(marker.Modified)) {
			continue
		}

		name := strings.TrimSuffix(marker.Name, RetiredExt)

		grp.Go(func() error {
			day, err := ps.retiredOn(name)
			if err != nil {
				ps.logger.Warn("reading when an archive was retired failed", "archive", name, "err", err)
				return nil
			}

			if !due(day) {
				return nil
			}

			// the marker is what brings a failed delete back next run
			if !ps.deleteArchiveFiles(name) {
				return nil
			}

			if err := ps.storage.Delete(name + RetiredExt); err != nil && !notExist(err) {
				return err
			}

			purged.Add(1)

			return nil
		})
	}

	if err := grp.Wait(); err != nil {
		return int(purged.Load()), err
	}

	quarantined, err := ps.quarantined()
	if err != nil {
		return int(purged.Load()), err
	}

	n, err := ps.purgeRetirements(due, quarantined)

	return int(purged.Load()) + n, err
}

// purgeRetirements deletes the archives of every retirement due by both
// the day it names and the day the storage stored it, quarantined or not.
func (ps *PackStorage) purgeRetirements(due func(day time.Time) bool, quarantined map[string]int64) (int, error) {
	retirements, err := ps.retirements()
	if err != nil {
		return 0, err
	}

	var purging []storedRetirement
	archives := make(map[string]bool)

	for _, r := range retirements {
		day, err := time.Parse(quarantineDay, r.Day)
		if err != nil {
			ps.logger.Warn("reading when archives were retired failed", "retirement", r.file.Name, "err", err)
			continue
		}

		if !due(day) || !due(dayOf(r.since())) {
			continue
		}

		purging = append(purging, r)
		for _, name := range r.Archives {
			archives[name] = true
		}
	}

	var mtx sync.Mutex
	gone := make(map[string]bool, len(archives))

	grp := new(errgroup.Group)
	grp.SetLimit(fileWorkers)

	for name := range archives {
		grp.Go(func() error {
			if ps.purgeArchive(name, quarantined) {
				mtx.Lock()
				gone[name] = true
				mtx.Unlock()
			}

			return nil
		})
	}

	_ = grp.Wait()

	// what is left is what brings a failed delete back next run
	for _, r := range purging {
		if err := ps.cutRetirement(r, gone); err != nil {
			return len(gone), err
		}
	}

	return len(gone), nil
}

func dayOf(t time.Time) time.Time {
	y, m, d := t.UTC().Date()

	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// retiredBytes sums the sizes of the retired archives still stored.
func (ps *PackStorage) retiredBytes() (uint64, error) {
	retired, err := ps.retiredArchives()
	if err != nil {
		return 0, err
	}

	archives, err := ListInfo(ps.storage, ArchiveSuffix)
	if err != nil {
		return 0, err
	}

	quarantined, err := ps.quarantined()
	if err != nil {
		return 0, err
	}

	var total uint64

	for _, archive := range archives {
		if retired[strings.TrimSuffix(archive.Name, ArchiveSuffix)] {
			total += uint64(archive.Size)
		}
	}

	for name, size := range quarantined {
		if strings.HasSuffix(name, ArchiveSuffix+QuarantineExt) {
			total += uint64(size)
		}
	}

	return total, nil
}

func (ps *PackStorage) retiredOn(name string) (time.Time, error) {
	file, err := ps.storage.Open(name + RetiredExt)
	if err != nil {
		return time.Time{}, err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return time.Time{}, err
	}

	return time.Parse(quarantineDay, string(data))
}

// ErrNotQuarantined is RestoreQuarantined's answer for an archive the
// quarantine does not hold.
var ErrNotQuarantined = errors.New("the quarantine holds no such archive")

// RestoreQuarantined puts a retired archive back, where the store picks
// it up as committed.
func (ps *PackStorage) RestoreQuarantined(name string) error {
	if !ps.hasIndexFile(name) && !ps.stored(name+IndexExt+QuarantineExt) {
		return fmt.Errorf("%w: %s", ErrNotQuarantined, name)
	}

	if err := ps.unquarantineFiles(name); err != nil {
		return err
	}

	err := ps.storage.Delete(name + RetiredExt)
	if notExist(err) {
		err = ps.unretire(name)
	}

	if err != nil {
		return err
	}

	ps.mtx.Lock()
	delete(ps.retired, name)
	ps.mtx.Unlock()

	return ps.refreshArchives()
}

// unretire takes the archive out of every retirement that lists it.
func (ps *PackStorage) unretire(name string) error {
	retirements, err := ps.retirements()
	if err != nil {
		return err
	}

	listed := false

	for _, r := range retirements {
		if !slices.Contains(r.Archives, name) {
			continue
		}

		listed = true

		if err := ps.cutRetirement(r, map[string]bool{name: true}); err != nil {
			return err
		}
	}

	if !listed {
		return fmt.Errorf("%w: %s", ErrNotQuarantined, name)
	}

	return nil
}
