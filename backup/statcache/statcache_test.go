package statcache

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gobackio/goback/proto"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestASecondOpenOfACacheFailsAsLocked(t *testing.T) {
	dir := t.TempDir()

	first, err := Open(dir)
	require.NoError(t, err)
	defer first.Close()

	_, err = Open(dir)
	require.ErrorIs(t, err, ErrLocked)
}

func TestAnEntryMatchesOnlyTheRefItWasStoredWith(t *testing.T) {
	cache, err := Open(t.TempDir())
	require.NoError(t, err)
	defer cache.Close()

	path := filepath.Join(t.TempDir(), "a.txt")
	require.NoError(t, os.WriteFile(path, []byte("a"), 0o644))
	info, err := os.Stat(path)
	require.NoError(t, err)

	read := func(seed string) *proto.Ref {
		sum := sha256.Sum256([]byte(seed))
		return &proto.Ref{Hash: sum[:]}
	}

	require.NoError(t, cache.Store(path, info, read("committed")))

	present, matched := cache.Lookup(path, info, read("committed"))
	require.True(t, present)
	require.True(t, matched)

	present, matched = cache.Lookup(path, info, read("another"))
	require.True(t, present)
	require.False(t, matched)
}

func TestAnEntryExpiresUnlessAMatchRenewsIt(t *testing.T) {
	cache, err := Open(t.TempDir())
	require.NoError(t, err)
	defer cache.Close()

	path := filepath.Join(t.TempDir(), "a.txt")
	require.NoError(t, os.WriteFile(path, []byte("a"), 0o644))
	info, err := os.Stat(path)
	require.NoError(t, err)

	sum := sha256.Sum256([]byte("committed"))
	ref := &proto.Ref{Hash: sum[:]}
	other := &proto.Ref{Hash: make([]byte, proto.HashSize)}

	expires := func() time.Duration {
		var at uint64
		require.NoError(t, cache.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(key(path))
			if err != nil {
				return err
			}
			at = item.ExpiresAt()
			return nil
		}))
		return time.Until(time.Unix(int64(at), 0))
	}
	age := func() {
		require.NoError(t, cache.db.Update(func(txn *badger.Txn) error {
			return txn.SetEntry(badger.NewEntry(key(path), encode(info, ref)).WithTTL(2 * 24 * time.Hour))
		}))
	}

	require.NoError(t, cache.Store(path, info, ref))
	require.InDelta(t, TTL.Seconds(), expires().Seconds(), 5)

	age()
	cache.Lookup(path, info, other)
	require.Less(t, expires(), 3*24*time.Hour, "a mismatch does not renew")

	cache.Lookup(path, info, ref)
	require.InDelta(t, TTL.Seconds(), expires().Seconds(), 5)
}
