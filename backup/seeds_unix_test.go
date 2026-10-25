//go:build !windows

package backup

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestASeedFileThatCannotBeReadIsPassedOver(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads every file")
	}

	dir := t.TempDir()
	data := randomData(3*maxBlobSize, 1)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "readable"), data, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "locked"), data, 0o000))

	seeds := NewSeedMap(nil)
	require.NoError(t, seeds.AddSeed(dir, SeedFull))
	require.NotZero(t, seeds.Len())
}

func TestASeedSwappedForAFIFOIsNotWaitedOn(t *testing.T) {
	dir := t.TempDir()
	data := randomData(100, 1)
	seed := filepath.Join(dir, "a")
	require.NoError(t, os.WriteFile(seed, data, 0o644))

	seeds := NewSeedMap(nil)
	require.NoError(t, seeds.AddSeed(dir, SeedGuess))

	require.NoError(t, os.Remove(seed))
	require.NoError(t, syscall.Mkfifo(seed, 0o644))

	guessed := make(chan struct{})
	go func() {
		seeds.guess("a", statFor(data))
		close(guessed)
	}()

	select {
	case <-guessed:
	case <-time.After(10 * time.Second):
		t.Fatal("the guess waits on the FIFO")
	}

	require.Zero(t, seeds.Len())
}
