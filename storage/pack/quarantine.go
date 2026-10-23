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

const quarantineDay = "2006-01-02"

// DefaultQuarantine is how long a retired archive is kept by default, so
// reads already under way, in this process or another, finish against it.
const DefaultQuarantine = 24 * time.Hour

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

	n, err := ps.purgeRetirements(due)

	return int(purged.Load()) + n, err
}

// purgeRetirements deletes the archives of every retirement due by both
// the day it names and the day the storage stored it.
func (ps *PackStorage) purgeRetirements(due func(day time.Time) bool) (int, error) {
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
			if ps.deleteArchiveFiles(name) {
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

	var total uint64

	for _, archive := range archives {
		if retired[strings.TrimSuffix(archive.Name, ArchiveSuffix)] {
			total += uint64(archive.Size)
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
	if !ps.hasIndexFile(name) {
		return fmt.Errorf("%w: %s", ErrNotQuarantined, name)
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
