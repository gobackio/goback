package pack

import (
	"context"
	"errors"
	"testing"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/progress"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// rebuiltRecord reports what a rebuild of base without the index finds of
// a commit: whether its record is stored, and whether a tombstone names it.
func rebuiltRecord(t *testing.T, base string, ref *proto.Ref) (stored, tombstoned bool) {
	t.Helper()

	rebuilt := newTestStore(t, base)
	t.Cleanup(func() { _ = rebuilt.Close() })

	ctx := context.Background()

	require.NoError(t, rebuilt.WalkHeaders(ctx, proto.ObjectType_COMMIT, func(hdr *proto.ObjectHeader) error {
		stored = stored || hdr.GetRef().Equal(ref)
		return nil
	}))

	require.NoError(t, rebuilt.WalkHeaders(ctx, proto.ObjectType_TOMBSTONE, func(hdr *proto.ObjectHeader) error {
		tombstoned = tombstoned || hdr.GetTombstoneFor().Equal(ref)
		return nil
	}))

	return stored, tombstoned
}

// TestACommitRefusedAsItsRequestEndsStaysDead cancels a commit's request
// while the store checks it, as a client's deadline does, and expects the
// refused record to stay dead once the session commits another commit.
func TestACommitRefusedAsItsRequestEndsStaysDead(t *testing.T) {
	base := t.TempDir()
	store := newTestStore(t, base)

	ctx, _ := beginSession(t, store, "agent-a")
	blob := makeTestData(t, 1)[0]

	request, cancel := context.WithCancel(ctx)
	defer cancel()

	request = progress.NewContext(request, func(u progress.Update) {
		if u.Phase == progress.PhaseCheck {
			cancel()
		}
	})

	refused := commitObject(blob.Ref())
	require.ErrorIs(t, store.Put(request, refused), backup.ErrDanglingRef)

	require.NoError(t, store.Put(ctx, blob))
	require.NoError(t, store.Put(ctx, commitObject(blob.Ref())))
	require.NoError(t, store.Close())

	stored, tombstoned := rebuiltRecord(t, base, refused.Ref())
	require.True(t, !stored || tombstoned, "a refused commit is stored without a tombstone")
}

// endFailingIndex fails EndSession while failEnd is set.
type endFailingIndex struct {
	*InMemoryIndex
	failEnd bool
}

func (f *endFailingIndex) EndSession(id string) ([]string, error) {
	if f.failEnd {
		return nil, errors.New("index unavailable")
	}

	return f.InMemoryIndex.EndSession(id)
}

// TestACommitThatFailsOnceCommittedStaysDead lets the index fail to end a
// session the commit has committed, so the commit is answered as failed,
// and expects its record not to stay a commit.
func TestACommitThatFailsOnceCommittedStaysDead(t *testing.T) {
	base := t.TempDir()
	index := &endFailingIndex{InMemoryIndex: NewInMemoryIndex()}
	store := newTestStore(t, base, WithArchiveIndex(index))

	ctx, _ := beginSession(t, store, "agent-a")
	blob := makeTestData(t, 1)[0]
	require.NoError(t, store.Put(ctx, blob))

	index.failEnd = true

	refused := commitObject(blob.Ref())
	require.Error(t, store.Put(ctx, refused))

	index.failEnd = false
	require.NoError(t, store.Close())

	stored, tombstoned := rebuiltRecord(t, base, refused.Ref())
	require.True(t, stored, "the session committed the record")
	require.True(t, tombstoned, "a commit answered as failed is stored without a tombstone")
}
