package pack

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestARewriteKeepsWhatItRetiredUntilTheQuarantineEnds(t *testing.T) {
	base := t.TempDir()
	store := newGCStore(t, base)
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	gone, kept := retiredChain(t, store)

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	// the run's clock is ahead of the retirement, so it would purge what
	// it retired under the default quarantine
	opts := gcOptions(t, 48*time.Hour)
	opts.Quarantine = 7 * 24 * time.Hour

	second, err := store.Collect(ctx, opts)
	require.NoError(t, err)
	require.NotZero(t, second.Swept)
	require.NotZero(t, second.RetiredBytes, "the retired archives are still stored")
	require.NotZero(t, second.ArchiveBytes)
	requireStored(t, store, gone, false)

	retired, err := store.retiredArchives()
	require.NoError(t, err)
	require.NotEmpty(t, retired)

	for name := range retired {
		require.True(t, store.hasIndexFile(name), "the files of %s stay", name)
	}

	// another process loads none of them
	other := newGCStore(t, base)
	t.Cleanup(func() { _ = other.Close() })
	names, _, err := other.archiveNames()
	require.NoError(t, err)
	for _, name := range names {
		require.NotContains(t, retired, name)
	}

	early, err := store.PurgeQuarantine(DefaultQuarantine, time.Now().Add(DefaultQuarantine-24*time.Hour))
	require.NoError(t, err)
	require.Zero(t, early)

	purged, err := store.PurgeQuarantine(DefaultQuarantine, time.Now().Add(DefaultQuarantine+48*time.Hour))
	require.NoError(t, err)
	require.Equal(t, len(retired), purged)

	for name := range retired {
		require.False(t, store.hasIndexFile(name))
	}

	left, err := store.retiredArchives()
	require.NoError(t, err)
	require.Empty(t, left)

	requireStored(t, store, kept, true)
}

func TestACollectionWhoseClockRunsAheadOfTheStoragePurgesNothingEarly(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	retiredChain(t, store)

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)
	second, err := store.Collect(ctx, gcOptions(t, 48*time.Hour))
	require.NoError(t, err)
	require.NotZero(t, second.Swept)
	require.Zero(t, second.Purged, "what it retired was stored moments ago")

	retired, err := store.retiredArchives()
	require.NoError(t, err)
	require.NotEmpty(t, retired)

	third, err := store.Collect(ctx, gcOptions(t, 96*time.Hour))
	require.NoError(t, err)
	require.Zero(t, third.Purged)

	for name := range retired {
		require.True(t, store.hasIndexFile(name), "the files of %s stay", name)
	}
}

func TestRestoringAQuarantinedArchiveBringsItsObjectsBack(t *testing.T) {
	for _, moved := range []bool{false, true} {
		t.Run(fmt.Sprintf("moved=%t", moved), func(t *testing.T) {
			store := newGCStore(t, t.TempDir())
			t.Cleanup(func() { _ = store.Close() })
			ctx := context.Background()

			gone, _ := retiredChain(t, store)

			_, err := store.Collect(ctx, gcOptions(t, 0))
			require.NoError(t, err)
			opts := gcOptions(t, 48*time.Hour)
			opts.Quarantine = 7 * 24 * time.Hour
			_, err = store.Collect(ctx, opts)
			require.NoError(t, err)
			requireStored(t, store, gone, false)

			retired, err := store.retiredArchives()
			require.NoError(t, err)

			if moved {
				n, err := store.moveRetired(time.Now().Add(movedAfter))
				require.NoError(t, err)
				require.Len(t, retired, n)

				for name := range retired {
					require.False(t, store.hasIndexFile(name))
				}
			}

			for name := range retired {
				require.NoError(t, store.RestoreQuarantined(name))
			}

			requireStored(t, store, gone, true)
			require.ErrorIs(t, store.RestoreQuarantined("no-such-archive"), ErrNotQuarantined)
		})
	}
}

func TestThePurgeGoesByTheDayAMarkerNamesAndNeverBeforeItWasStored(t *testing.T) {
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	due := day.Add(DefaultQuarantine + 24*time.Hour)

	type marker struct {
		names, stored time.Time
	}

	markers := map[string]marker{
		"same-day":     {names: day, stored: day.Add(23*time.Hour + 59*time.Minute)},
		"stored-later": {names: day, stored: day.Add(24*time.Hour + time.Minute)},
		"named-ahead":  {names: day.Add(24 * time.Hour), stored: day.Add(24*time.Hour - time.Second)},
	}

	open := func() (*PackStorage, *memBucket) {
		bucket := newMemBucket()
		store, err := NewPackStorage(WithArchiveStorage(bucket.view()), WithArchiveIndex(NewInMemoryIndex()))
		require.NoError(t, err)
		require.NoError(t, store.Open())
		t.Cleanup(func() { _ = store.Close() })

		for name, m := range markers {
			bucket.files[name+RetiredExt] = memObject{data: []byte(m.names.Format(quarantineDay)), created: m.stored}
			bucket.files[name+ArchiveSuffix] = memObject{data: make([]byte, len(name)), created: m.stored}
		}
		bucket.files["live"+ArchiveSuffix] = memObject{data: make([]byte, 1000), created: day}

		return store, bucket
	}

	store, bucket := open()
	retired, err := store.retiredBytes()
	require.NoError(t, err)
	require.Equal(t, uint64(len("same-day")+len("stored-later")+len("named-ahead")), retired)

	listed, err := bucket.ListInfo(RetiredExt)
	require.NoError(t, err)
	opened, err := ListInfo(&indexCounting{ArchiveStorage: bucket.view()}, RetiredExt)
	require.NoError(t, err)
	require.ElementsMatch(t, listed, opened, "a storage that cannot list sizes and times has each file opened")

	for _, tc := range []struct {
		now    time.Time
		purged []string
	}{
		{now: due.Add(-time.Nanosecond)},
		{now: due, purged: []string{"same-day"}},
		{now: due.Add(24*time.Hour - time.Nanosecond), purged: []string{"same-day"}},
		{now: due.Add(24 * time.Hour), purged: []string{"same-day", "stored-later", "named-ahead"}},
	} {
		store, bucket := open()

		purged, err := store.PurgeQuarantine(DefaultQuarantine, tc.now)
		require.NoError(t, err)
		require.Equal(t, len(tc.purged), purged, "at %s", tc.now)

		for name := range markers {
			_, kept := bucket.files[name+RetiredExt]
			_, archiveKept := bucket.files[name+ArchiveSuffix]
			require.Equal(t, !slices.Contains(tc.purged, name), kept, "%s at %s", name, tc.now)
			require.Equal(t, kept, archiveKept, "%s at %s", name, tc.now)
		}
	}
}

// failingArchiveDeletes refuses to delete archive data while set.
type failingArchiveDeletes struct {
	*memView

	failing bool
}

func (f *failingArchiveDeletes) Delete(name string) error {
	if f.failing && strings.HasSuffix(name, ArchiveSuffix) {
		return errors.New("the bucket is unreachable")
	}

	return f.memView.Delete(name)
}

func TestAPurgeThatCannotDeleteTheArchiveTriesAgainNextRun(t *testing.T) {
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	bucket := newMemBucket()
	storage := &failingArchiveDeletes{memView: bucket.view(), failing: true}

	store, err := NewPackStorage(WithArchiveStorage(storage), WithArchiveIndex(NewInMemoryIndex()))
	require.NoError(t, err)
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })

	bucket.files["old"+RetiredExt] = memObject{data: []byte(day.Format(quarantineDay)), created: day}
	bucket.files["old"+ArchiveSuffix] = memObject{data: []byte("data"), created: day}

	due := day.Add(DefaultQuarantine + 24*time.Hour)

	purged, err := store.PurgeQuarantine(DefaultQuarantine, due)
	require.NoError(t, err)
	require.Zero(t, purged)
	require.Contains(t, bucket.files, "old"+RetiredExt, "the marker stays for the next run")

	storage.failing = false

	purged, err = store.PurgeQuarantine(DefaultQuarantine, due)
	require.NoError(t, err)
	require.Equal(t, 1, purged)
	require.NotContains(t, bucket.files, "old"+ArchiveSuffix)
	require.NotContains(t, bucket.files, "old"+RetiredExt)
}

func storeRetirementAt(t *testing.T, bucket *memBucket, name string, r retirement, stored time.Time) {
	t.Helper()

	data, err := json.Marshal(r)
	require.NoError(t, err)

	bucket.files[name+RetirementExt] = memObject{data: data, created: stored}

	for _, archive := range r.Archives {
		bucket.files[archive+ArchiveSuffix] = memObject{data: []byte(archive), created: stored}
		bucket.files[archive+IndexExt] = memObject{created: stored}
	}
}

func TestAPurgeGoesByTheDayARetirementNamesAndNeverBeforeItWasFirstStored(t *testing.T) {
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	due := day.Add(DefaultQuarantine + 24*time.Hour)

	open := func() (*PackStorage, *memBucket) {
		bucket := newMemBucket()
		store, err := NewPackStorage(WithArchiveStorage(bucket.view()), WithArchiveIndex(NewInMemoryIndex()))
		require.NoError(t, err)
		require.NoError(t, store.Open())
		t.Cleanup(func() { _ = store.Close() })

		named := retirement{Day: day.Format(quarantineDay), Archives: []string{"same-day-1", "same-day-2"}}
		storeRetirementAt(t, bucket, "same-day", named, day.Add(23*time.Hour+59*time.Minute))

		later := retirement{Day: day.Format(quarantineDay), Archives: []string{"stored-later"}}
		storeRetirementAt(t, bucket, "stored-later", later, day.Add(24*time.Hour+time.Minute))

		bucket.files["live"+ArchiveSuffix] = memObject{data: make([]byte, 1000), created: day}

		return store, bucket
	}

	store, _ := open()
	retired, err := store.retiredBytes()
	require.NoError(t, err)
	require.Equal(t, uint64(len("same-day-1")+len("same-day-2")+len("stored-later")), retired)

	for _, tc := range []struct {
		now    time.Time
		purged []string
	}{
		{now: due.Add(-time.Nanosecond)},
		{now: due, purged: []string{"same-day-1", "same-day-2"}},
		{now: due.Add(24 * time.Hour), purged: []string{"same-day-1", "same-day-2", "stored-later"}},
	} {
		store, bucket := open()

		purged, err := store.PurgeQuarantine(DefaultQuarantine, tc.now)
		require.NoError(t, err)
		require.Equal(t, len(tc.purged), purged, "at %s", tc.now)

		for _, name := range []string{"same-day-1", "same-day-2", "stored-later"} {
			_, kept := bucket.files[name+ArchiveSuffix]
			require.Equal(t, !slices.Contains(tc.purged, name), kept, "%s at %s", name, tc.now)
		}

		retired, err := store.retiredArchives()
		require.NoError(t, err)
		require.Len(t, retired, 3-len(tc.purged), "at %s", tc.now)
	}

	// what a restore leaves retired keeps the time it was first stored
	store, bucket := open()
	require.NoError(t, store.unretire("same-day-1"))
	require.ErrorIs(t, store.unretire("same-day-1"), ErrNotQuarantined)

	purged, err := store.PurgeQuarantine(DefaultQuarantine, due)
	require.NoError(t, err)
	require.Equal(t, 1, purged)
	require.Contains(t, bucket.files, "same-day-1"+ArchiveSuffix)
	require.NotContains(t, bucket.files, "same-day-2"+ArchiveSuffix)

	names, _, err := store.archiveNames()
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"live", "same-day-1"}, names)
}

func TestAPurgeDeletesTheArchivesRetiredWithAGraceOnceItIsOver(t *testing.T) {
	day := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	bucket := newMemBucket()

	store, err := NewPackStorage(WithArchiveStorage(bucket.view()), WithArchiveIndex(NewInMemoryIndex()))
	require.NoError(t, err)
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })

	storeRetirementAt(t, bucket, "graced", retirement{Day: day.Format(quarantineDay), Grace: time.Hour, Archives: []string{"a"}}, day)
	storeRetirementAt(t, bucket, "quarantined", retirement{Day: day.Format(quarantineDay), Archives: []string{"b"}}, day)

	purged, err := store.PurgeQuarantine(DefaultQuarantine, day.Add(59*time.Minute))
	require.NoError(t, err)
	require.Zero(t, purged)

	purged, err = store.PurgeQuarantine(DefaultQuarantine, day.Add(time.Hour))
	require.NoError(t, err)
	require.Equal(t, 1, purged)
	require.NotContains(t, bucket.files, "a"+ArchiveSuffix)
	require.Contains(t, bucket.files, "b"+ArchiveSuffix)
}

func TestArchivesRetiredADayAgoMoveIntoTheQuarantineUntilThePurge(t *testing.T) {
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	bucket := newMemBucket()

	store, err := NewPackStorage(WithArchiveStorage(bucket.view()), WithArchiveIndex(NewInMemoryIndex()))
	require.NoError(t, err)
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })

	storeRetirementAt(t, bucket, "old", retirement{Day: day.Format(quarantineDay), Archives: []string{"a", "half"}}, day)
	storeRetirementAt(t, bucket, "new", retirement{Day: day.Format(quarantineDay), Archives: []string{"b"}}, day.Add(25*time.Hour))
	bucket.files["a"+GCExt] = memObject{created: day}
	bucket.files["live"+ArchiveSuffix] = memObject{data: make([]byte, 1000), created: day}

	// a move cut short after the index
	require.NoError(t, bucket.Move("half"+IndexExt, "half"+IndexExt+QuarantineExt))

	moved, err := store.moveRetired(day.Add(24 * time.Hour))
	require.NoError(t, err)
	require.Equal(t, 2, moved)

	for _, file := range []string{"a" + ArchiveSuffix, "a" + IndexExt, "a" + GCExt, "half" + ArchiveSuffix, "half" + IndexExt} {
		require.NotContains(t, bucket.files, file)
		require.Contains(t, bucket.files, file+QuarantineExt)
	}

	listed, err := bucket.List(ArchiveSuffix)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"b" + ArchiveSuffix, "live" + ArchiveSuffix}, listed)

	retired, err := store.retiredBytes()
	require.NoError(t, err)
	require.Equal(t, uint64(len("a")+len("half")+len("b")), retired)

	again, err := store.moveRetired(day.Add(24 * time.Hour))
	require.NoError(t, err)
	require.Zero(t, again)

	purged, err := store.PurgeQuarantine(DefaultQuarantine, day.Add(DefaultQuarantine+24*time.Hour))
	require.NoError(t, err)
	require.Equal(t, 2, purged)

	for name := range bucket.files {
		require.False(t, strings.HasPrefix(name, "a.") || strings.HasPrefix(name, "half."), "%s is purged", name)
	}

	require.Contains(t, bucket.files, "b"+ArchiveSuffix)
}

func TestAPurgeThatCannotDeleteARetiredArchiveTriesAgainNextRun(t *testing.T) {
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	bucket := newMemBucket()
	storage := &failingArchiveDeletes{memView: bucket.view(), failing: true}

	store, err := NewPackStorage(WithArchiveStorage(storage), WithArchiveIndex(NewInMemoryIndex()))
	require.NoError(t, err)
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })

	storeRetirementAt(t, bucket, "old", retirement{Day: day.Format(quarantineDay), Archives: []string{"a", "b"}}, day)

	due := day.Add(DefaultQuarantine + 24*time.Hour)

	purged, err := store.PurgeQuarantine(DefaultQuarantine, due)
	require.NoError(t, err)
	require.Zero(t, purged)
	require.Contains(t, bucket.files, "old"+RetirementExt, "the retirement stays for the next run")

	storage.failing = false

	purged, err = store.PurgeQuarantine(DefaultQuarantine, due)
	require.NoError(t, err)
	require.Equal(t, 2, purged)
	require.NotContains(t, bucket.files, "a"+ArchiveSuffix)
	require.NotContains(t, bucket.files, "b"+ArchiveSuffix)
	require.NotContains(t, bucket.files, "old"+RetirementExt)
}
