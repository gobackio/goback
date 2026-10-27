package pack

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"

	"github.com/stretchr/testify/require"
)

// memMarkers keeps session markers in memory, as an index shared by every
// process would.
type memMarkers struct {
	mtx    sync.Mutex
	begun  map[string]SessionBegin
	ended  map[string]SessionOutcome
	endsAt map[string]time.Time
}

func newMemMarkers() *memMarkers {
	return &memMarkers{begun: map[string]SessionBegin{}, ended: map[string]SessionOutcome{}, endsAt: map[string]time.Time{}}
}

// inBucketKey carries whether a session keeps its markers in the bucket.
type inBucketKey struct{}

func markedInBucket(ctx context.Context) bool {
	in, _ := ctx.Value(inBucketKey{}).(bool)
	return in
}

func (m *memMarkers) MarkBegun(begin SessionBegin) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if _, ok := m.begun[begin.ID]; ok {
		return ErrFileExists
	}

	m.begun[begin.ID] = begin

	return nil
}

func (m *memMarkers) MarkEnded(id string, want SessionOutcome) (SessionOutcome, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if outcome, ok := m.ended[id]; ok {
		return outcome, nil
	}

	m.ended[id], m.endsAt[id] = want, time.Now()

	return want, nil
}

func (m *memMarkers) SessionEnd(id string) (SessionOutcome, bool, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	outcome, ok := m.ended[id]

	return outcome, ok, nil
}

func (m *memMarkers) SessionBegin(id string) (*SessionBegin, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	begin, ok := m.begun[id]
	if !ok {
		return nil, ErrFileNotFound
	}

	return &begin, nil
}

func (m *memMarkers) Begun() (map[string]bool, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	ids := make(map[string]bool, len(m.begun))
	for id := range m.begun {
		ids[id] = true
	}

	return ids, nil
}

func (m *memMarkers) Ended() (map[string]time.Time, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	ended := make(map[string]time.Time, len(m.endsAt))
	for id, at := range m.endsAt {
		ended[id] = at
	}

	return ended, nil
}

func (m *memMarkers) DeleteMarkers(id string) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	delete(m.begun, id)
	delete(m.ended, id)
	delete(m.endsAt, id)

	return nil
}

func TestASessionKeepsItsMarkersWhereItBegan(t *testing.T) {
	base := t.TempDir()
	markers := newMemMarkers()
	routed := WithSessionMarkers(markers, markedInBucket)

	store := newTestStore(t, base, routed)
	blob := makeTestData(t, 1)[0]

	session := func(inBucket bool, commit bool) *backup.Session {
		s := &backup.Session{AgentID: "agent", Set: "world"}
		ctx, err := store.BeginSession(context.WithValue(context.Background(), inBucketKey{}, inBucket), s)
		require.NoError(t, err)
		require.NoError(t, store.Put(ctx, blob))

		if commit {
			require.NoError(t, store.Put(ctx, commitObject(blob.Ref())))
		} else {
			require.NoError(t, store.Flush())
		}

		return s
	}

	bucket, kept, aborted := session(true, true), session(false, true), session(false, false)
	require.NoError(t, store.Close())

	// another process's reaper ended it
	_, err := markers.MarkEnded(aborted.ID, SessionAborted)
	require.NoError(t, err)

	requireMarkers(t, store, bucket.ID, true, true)
	requireMarkers(t, store, kept.ID, false, false)
	requireMarkers(t, store, aborted.ID, false, false)

	for id, want := range map[string]SessionOutcome{kept.ID: SessionCommitted, aborted.ID: SessionAborted} {
		outcome, ended, err := markers.SessionEnd(id)
		require.NoError(t, err)
		require.True(t, ended)
		require.Equal(t, want, outcome)
	}

	rebuilt := closing(t, newTestStore(t, base, routed, WithArchiveIndex(NewInMemoryIndex())))
	for _, s := range []*backup.Session{bucket, kept} {
		got, err := rebuilt.index.PendingArchives(s.ID)
		require.NoError(t, err)
		require.Empty(t, got, "a rebuild finds the archives of %s committed", s.ID)
	}

	requireVisible(t, rebuilt, context.Background(), blob, true)

	var names []string
	names, err = rebuilt.storage.List(ArchiveSuffix)
	require.NoError(t, err)

	require.NotEmpty(t, names)
	for _, name := range names {
		require.NotEqual(t, aborted.ID, ParsePlacement(strings.TrimSuffix(name, ArchiveSuffix)).Session, "a rebuild deletes what an aborted session left")
	}
}
