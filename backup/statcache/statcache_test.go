package statcache

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/gobackio/goback/proto"

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
