package pack

import (
	"sync"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// clock is a settable time for an index's claims.
type clock struct {
	mtx sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	return c.now
}

func (c *clock) add(d time.Duration) {
	c.mtx.Lock()
	c.now = c.now.Add(d)
	c.mtx.Unlock()
}

// twoProcesses is two stores over one bucket and one index, the way two
// instances of a server share a deployment.
func twoProcesses(t *testing.T, opts ...PackOption) (*PackStorage, *PackStorage, *clock) {
	t.Helper()

	base := t.TempDir()
	at := &clock{now: time.Now()}

	index := NewInMemoryIndex()
	index.Now = at.Now

	opts = append([]PackOption{WithArchiveIndex(index)}, opts...)

	return closing(t, newTestStore(t, base, opts...)), closing(t, newTestStore(t, base, opts...)), at
}

// closing closes the store when the test ends, whatever state it is in.
func closing(t *testing.T, store *PackStorage) *PackStorage {
	t.Cleanup(func() { _ = store.Close() })

	return store
}

func TestAnObjectAnotherProcessIsWritingCountsAsStoredButCannotBeRead(t *testing.T) {
	a, b, _ := twoProcesses(t, WithClaims(time.Hour, time.Hour))

	ctx, _ := beginSession(t, a, "agent")
	blob := makeTestData(t, 1)[0]
	require.NoError(t, a.Put(ctx, blob))

	has, err := b.Has(ctx, blob.Ref())
	require.NoError(t, err)
	require.True(t, has, "a file put on b may reference a chunk a holds open")

	_, err = b.Get(ctx, blob.Ref())
	require.ErrorIs(t, err, backup.ErrNotFound, "its bytes are in a's upload in flight")

	require.NoError(t, a.Flush())
	requireVisible(t, b, ctx, blob, true)
}

func TestACommitWaitsForTheArchiveAnotherProcessHoldsOpen(t *testing.T) {
	a, b, _ := twoProcesses(t, WithClaims(200*time.Millisecond, time.Hour))

	ctx, _ := beginSession(t, a, "agent")
	blob := makeTestData(t, 1)[0]
	require.NoError(t, a.Put(ctx, blob))

	start := time.Now()
	require.NoError(t, b.Put(ctx, commitObject()))
	require.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond, "the commit waited for a to finalize")

	has, err := b.Has(ctx, blob.Ref())
	require.NoError(t, err)
	require.True(t, has)

	got, err := b.Get(t.Context(), blob.Ref())
	require.NoError(t, err, "committed, so readable without the session")
	require.Equal(t, blob.Bytes(), got.Bytes())
}

// An archive held open past its claim belongs to a process that died or
// stalled. The commit stops waiting for it, refuses, and takes a second
// copy of what it held; the process that held it can no longer finalize it.
func TestACommitRefusesWhatALapsedArchiveHeldUntilItIsStoredAgain(t *testing.T) {
	a, b, at := twoProcesses(t, WithClaims(time.Hour, time.Minute))

	ctx, _ := beginSession(t, a, "agent")
	blob := makeTestData(t, 1)[0]
	require.NoError(t, a.Put(ctx, blob))

	at.add(2 * time.Hour)

	err := b.Put(ctx, commitObject())
	require.ErrorIs(t, err, backup.ErrSessionLost)

	has, err := b.Has(ctx, blob.Ref())
	require.NoError(t, err)
	require.False(t, has, "the lost copy no longer counts")

	err = a.Flush()
	require.ErrorIs(t, err, ErrClaimLapsed, "a stalled process cannot bring the archive back")

	require.NoError(t, b.Put(ctx, blob))
	require.NoError(t, b.Put(ctx, commitObject()))

	got, err := b.Get(t.Context(), blob.Ref())
	require.NoError(t, err)
	require.Equal(t, blob.Bytes(), got.Bytes())
}

func TestACommitOnTheProcessWhoseOwnArchiveLapsedRefusesWhatItHeld(t *testing.T) {
	a, _, at := twoProcesses(t, WithClaims(time.Hour, time.Minute))

	ctx, _ := beginSession(t, a, "agent")
	blob := makeTestData(t, 1)[0]
	require.NoError(t, a.Put(ctx, blob))

	at.add(2 * time.Hour)

	require.ErrorIs(t, a.Put(ctx, commitObject()), backup.ErrSessionLost)

	has, err := a.Has(ctx, blob.Ref())
	require.NoError(t, err)
	require.False(t, has, "the process forgets what its lapsed archive held")

	require.NoError(t, a.Put(ctx, blob))
	require.NoError(t, a.Put(ctx, commitObject()))
}

func TestAWriteToAnArchiveACommitElsewhereGaveUpIsRefusedAsLost(t *testing.T) {
	a, b, at := twoProcesses(t, WithClaims(time.Hour, time.Minute))

	ctx, _ := beginSession(t, a, "agent")
	objects := makeTestData(t, 2)
	require.NoError(t, a.Put(ctx, objects[0]))

	at.add(2 * time.Hour)
	require.ErrorIs(t, b.Put(ctx, commitObject()), backup.ErrSessionLost)

	require.ErrorIs(t, a.Put(ctx, objects[1]), backup.ErrSessionLost, "a wakes up writing an archive that is gone")

	has, err := a.Has(ctx, objects[0].Ref())
	require.NoError(t, err)
	require.False(t, has, "and forgets what it held")

	for _, obj := range objects {
		require.NoError(t, a.Put(ctx, obj))
	}

	require.NoError(t, a.Flush())
	require.NoError(t, b.Put(ctx, commitObject()))

	for _, obj := range objects {
		requireVisible(t, b, t.Context(), obj, true)
	}
}

func TestARefusedCommitStaysAbsentWhenItsSessionCommitsAndThroughARebuild(t *testing.T) {
	base := t.TempDir()
	at := &clock{now: time.Now()}
	index := NewInMemoryIndex()
	index.Now = at.Now

	a := closing(t, newTestStore(t, base, WithArchiveIndex(index), WithClaims(time.Hour, time.Minute)))
	b := closing(t, newTestStore(t, base, WithArchiveIndex(index), WithClaims(time.Hour, time.Minute)))

	ctx, _ := beginSession(t, a, "agent")
	blob := makeTestData(t, 1)[0]
	require.NoError(t, a.Put(ctx, blob))

	at.add(2 * time.Hour)

	refused := commitObject()
	require.ErrorIs(t, b.Put(ctx, refused), backup.ErrSessionLost)

	require.NoError(t, b.Put(ctx, blob))
	require.NoError(t, b.Put(ctx, commitObject()))

	requirePresent(t, b, []*proto.Object{refused}, false)
	require.NoError(t, b.Close())

	rebuilt := closing(t, newTestStore(t, base))
	requirePresent(t, rebuilt, []*proto.Object{refused}, false)
	requireVisible(t, rebuilt, t.Context(), blob, true)
}

func TestAnArchiveIsFinalizedOnceItHasBeenOpenForAsLongAsItMayBe(t *testing.T) {
	a, b, _ := twoProcesses(t, WithClaims(100*time.Millisecond, time.Hour))

	ctx, _ := beginSession(t, a, "agent")
	blob := makeTestData(t, 1)[0]
	require.NoError(t, a.Put(ctx, blob))

	require.Eventually(t, func() bool {
		_, err := b.Get(ctx, blob.Ref())
		return err == nil
	}, 5*time.Second, 20*time.Millisecond, "the timer finalized a's archive without another write")
}

func TestAWriteToAnArchivePastItsTimeGoesToANewOne(t *testing.T) {
	store := closing(t, newTestStore(t, t.TempDir(), WithClaims(time.Hour, time.Hour)))

	ctx, s := beginSession(t, store, "agent")
	objects := makeTestData(t, 2)
	require.NoError(t, store.Put(ctx, objects[0]))

	ws := store.lookupWriteSession(s.ID)
	first := ws.archive
	first.opened = first.opened.Add(-2 * time.Hour)

	require.NoError(t, store.Put(ctx, objects[1]))
	require.NotSame(t, first, ws.archive)

	for _, obj := range objects {
		requireVisible(t, store, ctx, obj, true)
	}
}

// Rows written together still each answer their own writer.
func TestRowsOfConcurrentWritesAreIndexedBeforeEachWriteReturns(t *testing.T) {
	a, b, _ := twoProcesses(t, WithClaims(time.Hour, time.Hour))

	ctx, _ := beginSession(t, a, "agent")
	objects := makeTestData(t, 50)

	var wg sync.WaitGroup
	for _, obj := range objects {
		wg.Add(1)
		go func(obj *proto.Object) {
			defer wg.Done()

			require.NoError(t, a.Put(ctx, obj))

			has, err := b.Has(ctx, obj.Ref())
			require.NoError(t, err)
			require.True(t, has)
		}(obj)
	}

	wg.Wait()
}
