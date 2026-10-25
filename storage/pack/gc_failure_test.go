package pack

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

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
