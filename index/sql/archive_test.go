package sql

import (
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/pack"
	"github.com/gobackio/goback/storage/pack/packtest"
	"github.com/gobackio/goback/testing/testpg"

	"github.com/stretchr/testify/require"
)

func TestArchiveIndex(t *testing.T) {
	packtest.TestArchiveIndex(t, openIndex(t, newMemStore()))
}

func TestArchiveIndexExclusion(t *testing.T) {
	packtest.TestArchiveIndexExclusion(t, openIndex(t, newMemStore()))
}

func TestArchiveIndexCopies(t *testing.T) {
	packtest.TestArchiveIndexCopies(t, openIndex(t, newMemStore()))
}

func TestArchiveIndexTombstones(t *testing.T) {
	packtest.TestArchiveIndexTombstones(t, openIndex(t, newMemStore()))
}

func TestArchiveIndexTombstonesOnPostgres(t *testing.T) {
	x := New(testpg.Start(t), newMemStore())
	require.NoError(t, x.Open())
	t.Cleanup(func() { _ = x.Close() })

	packtest.TestArchiveIndexTombstones(t, x)
}

func TestArchiveVersions(t *testing.T) {
	packtest.TestArchiveVersions(t, openIndex(t, newMemStore()))
}

func TestArchiveIndexSessions(t *testing.T) {
	packtest.TestArchiveIndexSessions(t, openIndex(t, newMemStore()))
}

func TestArchiveIndexCountsAndRestoreSessions(t *testing.T) {
	x := openIndex(t, newMemStore())

	archive := packtest.RandomArchive(10)
	require.NoError(t, x.IndexArchive(pack.ArchiveInfo{Name: archive.Name()}, archive.Index()))
	other := packtest.RandomArchive(5)
	require.NoError(t, x.IndexArchive(pack.ArchiveInfo{Name: other.Name()}, append(other.Index(), archive.Index()[0])))

	total, unique, err := x.CountObjects()
	require.NoError(t, err)
	require.EqualValues(t, 16, total)
	require.EqualValues(t, 15, unique)

	restore := archive.Index()[1].Sum
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	require.NoError(t, x.BeginSession(&backup.Session{ID: "r", AgentID: "a", Set: "world", Restore: &proto.Ref{Hash: restore[:]}, Started: now, LastSeen: now}))

	session, err := x.GetSession("r")
	require.NoError(t, err)
	require.Equal(t, "world", session.Set)
	require.Equal(t, restore[:], session.Restore.GetHash())
	require.True(t, session.Started.Equal(now))

	sessions, err := x.ListSessions()
	require.NoError(t, err)
	require.Len(t, sessions, 1)

	require.NoError(t, x.TouchSession("r", now.Add(time.Minute)))
	session, err = x.GetSession("r")
	require.NoError(t, err)
	require.True(t, session.LastSeen.Equal(now.Add(time.Minute)))

	require.ErrorIs(t, x.TouchSession("nope", now), backup.ErrNoSession)

	require.NoError(t, x.DeleteArchives([]string{"long-gone", other.Name()}))
	total, _, err = x.CountObjects()
	require.NoError(t, err)
	require.EqualValues(t, 10, total, "the archive's objects go with it")
}

func BenchmarkLookup(b *testing.B) {
	idx := openIndex(b, newMemStore())

	b.Run("lookup", func(b *testing.B) {
		packtest.BenchmarkLookup(b, idx)
	})
}

func BenchmarkIndex(b *testing.B) {
	idx := openIndex(b, newMemStore())

	b.Run("insert", func(b *testing.B) {
		packtest.BenchmarkIndex(b, idx)
	})
}

func TestClaimIndex(t *testing.T) {
	packtest.TestClaimIndex(t, openIndex(t, newMemStore()))
}
