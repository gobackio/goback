package pack

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// syncingStorage records what the pack store syncs.
type syncingStorage struct {
	*localArchiveStorage
	mtx    sync.Mutex
	synced []string
}

func (s *syncingStorage) Sync(name string) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.synced = append(s.synced, name)

	return nil
}

func TestAFinishedArchiveAndItsIndexAreSyncedOnAStorageThatNeedsIt(t *testing.T) {
	storage := &syncingStorage{localArchiveStorage: newLocal(t.TempDir())}

	store, err := NewPackStorage(WithArchiveStorage(storage), WithArchiveIndex(NewInMemoryIndex()))
	require.NoError(t, err)
	require.NoError(t, store.Open())

	blob := makeTestData(t, 1)[0]
	require.NoError(t, store.Put(context.Background(), blob))
	require.NoError(t, store.Flush())

	archives, err := storage.List(ArchiveSuffix)
	require.NoError(t, err)
	require.Len(t, archives, 1)

	name := archives[0]
	require.ElementsMatch(t, []string{name, name[:len(name)-len(ArchiveSuffix)] + IndexExt}, storage.synced)
	require.NoError(t, store.Close())
}
