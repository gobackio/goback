package pack

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"

	"github.com/stretchr/testify/require"
)

func readMarker(t *testing.T, store *PackStorage, name string) string {
	t.Helper()

	file, err := store.storage.Open(name)
	require.NoError(t, err)
	defer file.Close()

	data, err := io.ReadAll(file)
	require.NoError(t, err)

	return string(data)
}

func TestASessionLeavesABeginAndTheOutcomeItEndedWith(t *testing.T) {
	store := newTestStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })

	ctx, committed := beginSession(t, store, "agent-a")
	require.Contains(t, readMarker(t, store, committed.ID+SessionBeginExt), "agent-a")

	require.NoError(t, store.Put(ctx, makeTestData(t, 1)[0]))
	require.NoError(t, store.Put(ctx, commitObject()))
	require.Equal(t, string(sessionCommitted), readMarker(t, store, committed.ID+SessionEndExt))

	require.NoError(t, store.EndSession(ctx))
	require.Equal(t, string(sessionCommitted), readMarker(t, store, committed.ID+SessionEndExt), "an end is never replaced")

	ctx, aborted := beginSession(t, store, "agent-b")
	require.NoError(t, store.EndSession(ctx))
	require.Equal(t, string(sessionAborted), readMarker(t, store, aborted.ID+SessionEndExt))
}

func TestACommitLosesToAnEndAlreadyTaken(t *testing.T) {
	store := newTestStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })

	ctx, session := beginSession(t, store, "agent-a")
	obj := makeTestData(t, 1)[0]
	require.NoError(t, store.Put(ctx, obj))
	require.NoError(t, store.Flush())

	// another process's reaper ended the session
	require.NoError(t, store.storage.CreateNew(session.ID+SessionEndExt, []byte(sessionAborted)))

	require.ErrorIs(t, store.Put(ctx, commitObject()), backup.ErrNoSession)
	requireVisible(t, store, context.Background(), obj, false)
}

func TestACommittedEndKeepsTheArchivesTheIndexStillHadPending(t *testing.T) {
	store := newTestStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })

	ctx, session := beginSession(t, store, "agent-a")
	obj := makeTestData(t, 1)[0]
	require.NoError(t, store.Put(ctx, obj))
	require.NoError(t, store.Flush())

	// the commit won the end and stopped before it marked its archives
	require.NoError(t, store.storage.CreateNew(session.ID+SessionEndExt, []byte(sessionCommitted)))

	require.NoError(t, store.EndSession(ctx))
	requireVisible(t, store, context.Background(), obj, true)
}

func TestOpeningAStoreBringsTheIndexInLineWithTheMarkers(t *testing.T) {
	base := t.TempDir()

	first := newTestStore(t, base)
	_, lost := beginSession(t, first, "agent-a")
	_, ended := beginSession(t, first, "agent-b")
	require.NoError(t, first.Close())

	require.NoError(t, first.storage.CreateNew(ended.ID+SessionEndExt, []byte(sessionAborted)))

	index := NewInMemoryIndex()
	require.NoError(t, index.BeginSession(ended))

	second := newTestStore(t, base, WithArchiveIndex(index))
	t.Cleanup(func() { _ = second.Close() })

	found, err := second.LookupSession(context.Background(), lost.ID)
	require.NoError(t, err, "a session the index lost is indexed again")
	require.Equal(t, "agent-a", found.AgentID)

	_, err = second.LookupSession(context.Background(), ended.ID)
	require.ErrorIs(t, err, backup.ErrNoSession, "an indexed session that ended is ended")
}

func requireMarkers(t *testing.T, store *PackStorage, id string, begin, end bool) {
	t.Helper()

	require.Equal(t, begin, store.stored(id+SessionBeginExt), "begin of %s", id)
	require.Equal(t, end, store.stored(id+SessionEndExt), "end of %s", id)
}

// pastClaims is far enough ahead for every claim to have lapsed.
const pastClaims = DefaultClaimOpen + DefaultClaimGrace + time.Minute

func TestACollectionKeepsTheMarkersOfASessionThatEndedWithinAClaimsLimit(t *testing.T) {
	store := newTestStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	sctx, session := beginSession(t, store, "agent-a")
	require.NoError(t, store.EndSession(sctx))

	_, err := store.Collect(ctx, gcOptions(t, DefaultClaimOpen+DefaultClaimGrace-time.Minute))
	require.NoError(t, err)
	requireMarkers(t, store, session.ID, true, true)

	_, err = store.Collect(ctx, gcOptions(t, pastClaims))
	require.NoError(t, err)
	requireMarkers(t, store, session.ID, false, false)
}

func TestAnArchiveAStalledWriterStoresAfterItsSessionAbortedIsNotTakenAsCommitted(t *testing.T) {
	base := t.TempDir()
	store := newTestStore(t, base)
	ctx := context.Background()

	sctx, session := beginSession(t, store, "agent-a")
	require.NoError(t, store.EndSession(sctx))

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)
	require.NoError(t, store.Close())

	// a writer elsewhere that still held the session stores its archive late
	stalledIndex := NewInMemoryIndex()
	stalled := newTestStore(t, base, WithArchiveIndex(stalledIndex))
	require.NoError(t, stalledIndex.BeginSession(session))
	late := makeTestData(t, 1)[0]
	require.NoError(t, stalled.Put(backup.WithSession(ctx, session), late))
	require.NoError(t, stalled.Flush())
	require.NoError(t, stalled.Close())

	rebuilt := newTestStore(t, base)
	t.Cleanup(func() { _ = rebuilt.Close() })
	requireVisible(t, rebuilt, ctx, late, false)

	archives, err := rebuilt.storage.List(ArchiveSuffix)
	require.NoError(t, err)
	require.Empty(t, archives, "the rebuild deleted what the aborted session left")
}

func TestACollectionDeletesASessionsMarkersOnceNoArchiveOrHorizonHoldsIt(t *testing.T) {
	base := t.TempDir()
	store := newTestStore(t, base, WithCompaction(CompactionConfig{MinimumCandidates: 0}))
	ctx := context.Background()

	chain := makeChain(makeTestData(t, 10))
	sctx, committed := beginSession(t, store, "agent-a")
	for _, obj := range chain {
		require.NoError(t, store.Put(sctx, obj))
	}

	actx, empty := beginSession(t, store, "agent-b")
	require.NoError(t, store.EndSession(actx))

	hctx, spanning := beginSession(t, store, "agent-c")

	_, err := store.Collect(ctx, gcOptions(t, pastClaims))
	require.NoError(t, err)
	requireMarkers(t, store, empty.ID, false, false)
	requireMarkers(t, store, committed.ID, true, true)
	requireMarkers(t, store, spanning.ID, true, false)

	// it ended after the generation listed it in its horizon
	require.NoError(t, store.EndSession(hctx))
	_, err = store.Collect(ctx, gcOptions(t, pastClaims))
	require.NoError(t, err)
	requireMarkers(t, store, spanning.ID, true, true)

	// the committed session's archives are retired, then purged
	compact(t, ctx, store)
	_, err = store.Collect(ctx, gcOptions(t, pastClaims))
	require.NoError(t, err)
	requireMarkers(t, store, spanning.ID, false, false)
	requireMarkers(t, store, committed.ID, true, true)

	_, err = store.Collect(ctx, gcOptions(t, 72*time.Hour))
	require.NoError(t, err)
	requireMarkers(t, store, committed.ID, false, false)

	archives, err := store.storage.List(ArchiveSuffix)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	rebuilt := newTestStore(t, base)
	t.Cleanup(func() { _ = rebuilt.Close() })

	after, err := rebuilt.storage.List(ArchiveSuffix)
	require.NoError(t, err)
	require.ElementsMatch(t, archives, after, "a rebuild deletes nothing")

	for _, obj := range chain {
		requireVisible(t, rebuilt, ctx, obj, true)
	}
}

// crashingMarkerDeletes refuses every marker delete after the first while
// set, as a process that dies between them leaves the storage.
type crashingMarkerDeletes struct {
	*localArchiveStorage

	crashing bool
	deleted  int
}

func (c *crashingMarkerDeletes) Delete(name string) error {
	marker := strings.HasSuffix(name, SessionBeginExt) || strings.HasSuffix(name, SessionEndExt)
	if c.crashing && marker {
		if c.deleted++; c.deleted > 1 {
			return errors.New("crashed")
		}
	}

	return c.localArchiveStorage.Delete(name)
}

func TestACrashWhileDeletingASessionsMarkersLeavesNoSessionLive(t *testing.T) {
	base := t.TempDir()
	storage := &crashingMarkerDeletes{localArchiveStorage: newLocal(base)}
	store, err := NewPackStorage(WithArchiveStorage(storage), WithArchiveIndex(NewInMemoryIndex()))
	require.NoError(t, err)
	require.NoError(t, store.Open())
	ctx := context.Background()

	chain := makeChain(makeTestData(t, 5))
	putAll(t, store, chain)

	actx, aborted := beginSession(t, store, "agent-a")
	require.NoError(t, store.Put(actx, makeTestData(t, 1)[0]))
	require.NoError(t, store.EndSession(actx))

	storage.crashing = true
	_, err = store.Collect(ctx, gcOptions(t, pastClaims))
	require.NoError(t, err)
	require.NoError(t, store.Close())

	begun := store.stored(aborted.ID + SessionBeginExt)
	ended := store.stored(aborted.ID + SessionEndExt)
	require.NotEqual(t, begun, ended, "the crash came between the two deletes")
	require.False(t, begun && !ended, "a session never looks begun without its end")

	storage.crashing = false
	reopened, err := NewPackStorage(WithArchiveStorage(storage), WithArchiveIndex(NewInMemoryIndex()))
	require.NoError(t, err)
	require.NoError(t, reopened.Open())
	t.Cleanup(func() { _ = reopened.Close() })

	_, err = reopened.LookupSession(ctx, aborted.ID)
	require.ErrorIs(t, err, backup.ErrNoSession)

	live, err := newGCRun(reopened, CollectOptions{}, nil, newGCReads()).liveSessions()
	require.NoError(t, err)
	require.False(t, live[aborted.ID])

	for _, obj := range chain {
		requireVisible(t, reopened, ctx, obj, true)
	}

	_, err = reopened.Collect(ctx, gcOptions(t, pastClaims))
	require.NoError(t, err)
	requireMarkers(t, reopened, aborted.ID, false, false)
}
