package pack

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// damageRecord flips the last byte of the record holding ref, which is
// always inside its stored payload.
func damageRecord(t *testing.T, base string, ref *proto.Ref) {
	t.Helper()

	store := newTestStore(t, base)
	loc, err := store.index.LocateObject(ref, Scope{})
	require.NoError(t, err)
	require.NoError(t, store.Close())

	name := filepath.Join(base, loc.Archive+ArchiveSuffix)
	data, err := os.ReadFile(name)
	require.NoError(t, err)

	data[int(loc.Record.Offset)+int(loc.Record.Length)-1] ^= 0xff
	require.NoError(t, os.WriteFile(name, data, 0644))
}

func TestRepairDropsWhatItCannotRead(t *testing.T) {
	base := t.TempDir()
	ctx := context.Background()

	store := newTestStore(t, base)
	objects := makeTestData(t, 20)
	for _, obj := range objects {
		require.NoError(t, store.Put(ctx, obj))
	}
	require.NoError(t, store.Close())

	damaged := objects[7]
	damageRecord(t, base, damaged.Ref())

	store = newTestStore(t, base)
	t.Cleanup(func() { _ = store.Close() })

	report, err := store.Repair(ctx)
	require.NoError(t, err)
	require.Len(t, report.Lost, 1)
	require.True(t, report.Lost[0].Equal(damaged.Ref()))
	require.Empty(t, report.Recovered)

	has, err := store.Has(ctx, damaged.Ref())
	require.NoError(t, err)
	require.False(t, has, "the store still claims an object it cannot read")

	for _, obj := range objects {
		if obj.Ref().Equal(damaged.Ref()) {
			continue
		}

		got, err := store.Get(ctx, obj.Ref())
		require.NoError(t, err)
		require.Equal(t, obj.GetBlob().Data, got.GetBlob().Data)
	}

	after, err := store.Scrub(ctx)
	require.NoError(t, err)
	require.Empty(t, after.Corrupt)
}

func TestRepairDropsARecordSealedAtRestThatNoLongerOpens(t *testing.T) {
	base := t.TempDir()
	key := atRestKey(t)
	ctx := context.Background()

	store := newTestStore(t, base, WithAtRestKey(key))
	objects := makeTestData(t, 20)
	for _, obj := range objects {
		require.NoError(t, store.Put(ctx, obj))
	}
	require.NoError(t, store.Close())

	damaged := objects[7]
	damageRecord(t, base, damaged.Ref())

	store = newTestStore(t, base, WithAtRestKey(key))
	t.Cleanup(func() { _ = store.Close() })

	scrub, err := store.Scrub(ctx)
	require.NoError(t, err)
	require.EqualValues(t, len(objects), scrub.Objects)
	require.Len(t, scrub.Corrupt, 1)
	require.True(t, scrub.Corrupt[0].Ref.Equal(damaged.Ref()))

	report, err := store.Repair(ctx)
	require.NoError(t, err)
	require.Len(t, report.Lost, 1)

	for _, obj := range objects {
		if obj.Ref().Equal(damaged.Ref()) {
			continue
		}

		got, err := store.Get(ctx, obj.Ref())
		require.NoError(t, err)
		require.Equal(t, obj.GetBlob().Data, got.GetBlob().Data)
	}

	after, err := store.Scrub(ctx)
	require.NoError(t, err)
	require.Empty(t, after.Corrupt)
}

func TestScrubWithoutTheAtRestKeyFails(t *testing.T) {
	base := t.TempDir()
	ctx := context.Background()

	store := newTestStore(t, base, WithAtRestKey(atRestKey(t)))
	require.NoError(t, store.Put(ctx, makeTestData(t, 1)[0]))
	require.NoError(t, store.Close())

	store = newTestStore(t, base)
	t.Cleanup(func() { _ = store.Close() })

	_, err := store.Scrub(ctx)
	require.ErrorIs(t, err, ErrAtRestKeyMissing)
}

func TestScrubPassesOverArchivesStillBeingWritten(t *testing.T) {
	store, err := NewPackStorage(WithArchiveStorage(newMemBucket().view()), WithArchiveIndex(NewInMemoryIndex()))
	require.NoError(t, err)
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })

	stored := makeTestData(t, 5)
	putAll(t, store, stored)

	session, _ := beginSession(t, store, "agent")
	require.NoError(t, store.Put(session, makeTestData(t, 1)[0]))

	report, err := store.Scrub(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, len(stored), report.Objects)
	require.Empty(t, report.Corrupt)
}

func TestRepairKeepsAnObjectAnotherArchiveHolds(t *testing.T) {
	base := t.TempDir()
	ctx := context.Background()

	store := newTestStore(t, base)
	obj := makeTestData(t, 1)[0]
	require.NoError(t, store.Put(ctx, obj))
	require.NoError(t, store.Flush())
	require.NoError(t, store.Put(ctx, obj))
	require.NoError(t, store.Close())

	damageRecord(t, base, obj.Ref())

	store = newTestStore(t, base)
	t.Cleanup(func() { _ = store.Close() })

	report, err := store.Repair(ctx)
	require.NoError(t, err)
	require.Empty(t, report.Lost)
	require.Len(t, report.Recovered, 1)

	got, err := store.Get(ctx, obj.Ref())
	require.NoError(t, err)
	require.Equal(t, obj.GetBlob().Data, got.GetBlob().Data)
}

func TestRepairOfAHealthyStoreChangesNothing(t *testing.T) {
	base := t.TempDir()
	ctx := context.Background()

	store := newTestStore(t, base)
	t.Cleanup(func() { _ = store.Close() })

	for _, obj := range makeTestData(t, 5) {
		require.NoError(t, store.Put(ctx, obj))
	}
	require.NoError(t, store.Flush())

	report, err := store.Repair(ctx)
	require.NoError(t, err)
	require.Zero(t, report.Archives)
	require.Empty(t, report.Lost)
	require.Empty(t, report.Recovered)
}
