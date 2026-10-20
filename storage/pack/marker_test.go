package pack

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// committedSession writes objects and a commit through one session and
// returns the names of the session's archives.
func committedSession(t *testing.T, base string) (*backup.Session, []*proto.Object, []string) {
	t.Helper()

	store := newTestStore(t, base, WithArchiveIndex(NewInMemoryIndex()))
	ctx, session := beginSession(t, store, "agent-a")

	objects := makeTestData(t, 5)
	for _, obj := range objects {
		require.NoError(t, store.Put(ctx, obj))
	}
	require.NoError(t, store.Put(ctx, commitObject(objects[0].Ref())))
	require.NoError(t, store.Close())

	listed, err := store.storage.List(ArchiveSuffix)
	require.NoError(t, err)

	var names []string
	for _, name := range listed {
		name = strings.TrimSuffix(filepath.ToSlash(name), ArchiveSuffix)
		if ParsePlacement(name).Session == session.ID {
			names = append(names, name)
		}
	}
	require.NotEmpty(t, names)

	return session, objects, names
}

func TestAnIndexFromBeforeACommitLosesNoneOfItsArchives(t *testing.T) {
	base := t.TempDir()
	session, objects, names := committedSession(t, base)

	// the index as a backup taken before the commit has it: the session
	// live and long idle, its archives pending
	stale := NewInMemoryIndex()
	require.NoError(t, stale.BeginSession(session))
	for _, name := range names {
		a, err := openArchive(newLocal(base), name, nil, nil, slog.Default())
		require.NoError(t, err)
		idx, err := a.getIndex()
		require.NoError(t, err)
		require.NoError(t, a.Close())
		require.NoError(t, stale.IndexArchive(ArchiveInfo{Name: name, Session: session.ID, State: ArchivePending}, idx))
	}

	store := newTestStore(t, base, WithArchiveIndex(stale), WithSessionLease(time.Nanosecond))
	t.Cleanup(func() { _ = store.Close() })

	store.Sweep(time.Now())

	_, err := store.LookupSession(context.Background(), session.ID)
	require.ErrorIs(t, err, backup.ErrNoSession, "the session is over")

	for _, name := range names {
		_, err := os.Stat(filepath.Join(base, filepath.FromSlash(name)+ArchiveSuffix))
		require.NoError(t, err, "a committed archive is never deleted")
	}

	for _, obj := range objects {
		requireVisible(t, store, context.Background(), obj, true)
	}
}

func TestACommittedArchiveThatLostItsIndexFileIsKept(t *testing.T) {
	base := t.TempDir()
	_, objects, names := committedSession(t, base)

	for _, name := range names {
		require.NoError(t, os.Remove(filepath.Join(base, filepath.FromSlash(name)+IndexExt)))
	}

	store := newTestStore(t, base, WithArchiveIndex(NewInMemoryIndex()))
	t.Cleanup(func() { _ = store.Close() })

	for _, obj := range objects {
		requireVisible(t, store, context.Background(), obj, true)
	}
}
