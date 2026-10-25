package pack

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// recordOf finds where the store keeps the first record of the type.
func recordOf(t *testing.T, store *PackStorage, typ proto.ObjectType) (string, IndexRecord) {
	t.Helper()

	store.mtx.RLock()
	archives := append([]*archive(nil), store.archives...)
	store.mtx.RUnlock()

	for _, a := range archives {
		idx, err := a.getIndex()
		require.NoError(t, err)

		for _, rec := range idx {
			if proto.ObjectType(rec.Type) == typ {
				return a.name, rec
			}
		}
	}

	require.FailNow(t, "no record of the type", "%s", typ)

	return "", IndexRecord{}
}

func TestACollectionReportsATombstoneWhoseHeaderIsDamaged(t *testing.T) {
	base := t.TempDir()
	store := newGCStore(t, base)
	ctx := context.Background()

	chain := makeChain(makeTestData(t, 5))
	putAll(t, store, chain)
	require.NoError(t, store.Delete(ctx, chain[len(chain)-1].Ref()))
	require.NoError(t, store.Flush())

	name, rec := recordOf(t, store, proto.ObjectType_TOMBSTONE)
	require.NoError(t, store.Close())

	file, err := os.OpenFile(filepath.Join(base, name+ArchiveSuffix), os.O_RDWR, 0o644)
	require.NoError(t, err)
	_, err = file.WriteAt(bytes.Repeat([]byte{0xff}, 10), int64(rec.Offset))
	require.NoError(t, err)
	require.NoError(t, file.Close())

	store = newGCStore(t, base)
	t.Cleanup(func() { _ = store.Close() })

	_, err = store.Collect(ctx, gcOptions(t, 0))
	require.Error(t, err)
}

// failingGCWrite refuses one write of a mark result once armed.
type failingGCWrite struct {
	*localArchiveStorage

	armed atomic.Bool
}

func (f *failingGCWrite) Create(name string) (File, error) {
	if strings.HasSuffix(name, GCExt) && f.armed.CompareAndSwap(true, false) {
		return nil, errors.New("503 service unavailable")
	}

	return f.localArchiveStorage.Create(name)
}

func TestACollectionThatFailsAfterItsMarkMarksAgainWhenRetried(t *testing.T) {
	storage := &failingGCWrite{localArchiveStorage: newLocal(t.TempDir())}
	store, err := NewPackStorage(WithArchiveStorage(storage), WithArchiveIndex(NewInMemoryIndex()), WithMaxSize(256*1024))
	require.NoError(t, err)
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	reachable, unreachable := makeGCTestData(t)
	putAll(t, store, append(append([]*proto.Object{}, reachable...), unreachable...))

	_, err = store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	opts := gcOptions(t, 48*time.Hour)
	storage.armed.Store(true)
	_, err = store.Collect(ctx, opts)
	require.Error(t, err)

	retried, err := store.Collect(ctx, opts)
	require.NoError(t, err)
	require.Zero(t, retried.Resumed)
	require.Zero(t, retried.Condemned)
	requirePresent(t, store, reachable, true)

	_, err = store.Collect(ctx, gcOptions(t, 96*time.Hour))
	require.NoError(t, err)
	requireStored(t, store, reachable, true)
}
