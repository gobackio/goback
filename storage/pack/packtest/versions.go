package packtest

import (
	"testing"
	"time"

	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/pack"

	"github.com/stretchr/testify/require"
)

// TestArchiveVersions checks that idx keeps each archive's creation time,
// fills in one it lacked, and keeps the versions records were carried with.
func TestArchiveVersions(t *testing.T, idx pack.ArchiveIndex) {
	created := time.Unix(1_790_000_000, 123_456_000)

	moved := RandomArchive(3)
	moved.index[1] = moved.index[1].Carry(pack.Version{Time: created.Add(-time.Hour), Offset: 77})
	require.NoError(t, idx.IndexArchive(pack.ArchiveInfo{Name: moved.name, Created: created}, moved.index))

	info, known, err := idx.LookupArchive(moved.name)
	require.NoError(t, err)
	require.True(t, known)
	require.True(t, created.Equal(info.Created), "created %s, want %s", info.Created, created)

	for i, record := range moved.index {
		location, err := idx.LocateObject(&proto.Ref{Hash: record.Sum[:]}, pack.Scope{})
		require.NoError(t, err)
		want, got := record.Version(created), location.Record.Version(info.Created)
		require.True(t, want.Time.Equal(got.Time), "record %d: time %s, want %s", i, got.Time, want.Time)
		require.Equal(t, want.Offset, got.Offset, "record %d", i)
	}

	require.NoError(t, idx.IndexArchive(pack.ArchiveInfo{Name: moved.name, Created: created.Add(time.Hour)}, moved.index))

	info, _, err = idx.LookupArchive(moved.name)
	require.NoError(t, err)
	require.True(t, created.Equal(info.Created), "a known creation time stays")

	older := RandomArchive(2)
	require.NoError(t, idx.IndexArchive(pack.ArchiveInfo{Name: older.name}, older.index))

	info, _, err = idx.LookupArchive(older.name)
	require.NoError(t, err)
	require.True(t, info.Created.IsZero())

	require.NoError(t, idx.IndexArchive(pack.ArchiveInfo{Name: older.name, Created: created}, older.index))

	info, _, err = idx.LookupArchive(older.name)
	require.NoError(t, err)
	require.True(t, created.Equal(info.Created), "a missing creation time is filled in")
}
