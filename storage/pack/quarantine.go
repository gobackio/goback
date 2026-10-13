package pack

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

// RetiredExt marks an archive a rewrite retired: <archive>.retired names
// the day it was retired. The store no longer loads the archive, a
// collection deletes it once the quarantine period has passed, and
// RestoreQuarantined puts it back until then.
const RetiredExt = ".retired"

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

	retired, err := ps.markerIDs(RetiredExt)
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

// quarantineArchive marks an archive retired, keeping its files for the
// quarantine period.
func (ps *PackStorage) quarantineArchive(name string, now time.Time) error {
	err := ps.storage.CreateNew(name+RetiredExt, []byte(now.UTC().Format(quarantineDay)))
	if err != nil && !errors.Is(err, ErrFileExists) {
		return fmt.Errorf("retiring %s: %w", name, err)
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

	err = grp.Wait()

	return int(purged.Load()), err
}

func dayOf(t time.Time) time.Time {
	y, m, d := t.UTC().Date()

	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// retiredBytes sums the sizes of the retired archives still stored.
func (ps *PackStorage) retiredBytes() (uint64, error) {
	retired, err := ps.markerIDs(RetiredExt)
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
		return fmt.Errorf("%w: %s", ErrNotQuarantined, name)
	}

	if err != nil {
		return err
	}

	ps.mtx.Lock()
	delete(ps.retired, name)
	ps.mtx.Unlock()

	return ps.refreshArchives()
}
