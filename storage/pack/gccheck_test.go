package pack

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTheCheckBeforeASweepHaltsCollectionsThatWouldDropWhatIsReachable(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	putAll(t, store, makeChain(makeTestData(t, 20)[10:]))
	chain := unreferencedChain(t, store)

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	// a writer that takes nothing back commits over the condemned chain
	// while the next mark runs
	gcAfterBatch = func(int) error {
		gcAfterBatch = nil
		return store.Put(ctx, chain[len(chain)-1])
	}
	t.Cleanup(func() { gcAfterBatch = nil })

	_, err = store.Collect(ctx, gcOptions(t, 48*time.Hour))
	require.ErrorIs(t, err, ErrHalted)
	requireStored(t, store, chain, true)

	report, err := store.Collect(ctx, gcOptions(t, 72*time.Hour))
	require.NoError(t, err)
	require.Contains(t, report.SweepSkipped, "halted")

	reason, err := store.Halted()
	require.NoError(t, err)
	require.Contains(t, reason, "halted")

	require.NoError(t, store.Resume())
	require.NoError(t, store.Resume(), "resuming what runs is nothing")

	reason, err = store.Halted()
	require.NoError(t, err)
	require.Empty(t, reason)

	for _, ahead := range []time.Duration{96 * time.Hour, 120 * time.Hour} {
		report, err = store.Collect(ctx, gcOptions(t, ahead))
		require.NoError(t, err)
		require.Empty(t, report.SweepSkipped)
	}

	requireStored(t, store, chain, true)
}

func TestTheCheckBeforeASweepLetsADeadCopyGoWhenAnotherCopyStays(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	putAll(t, store, makeChain(makeTestData(t, 2)))

	chain := unreferencedChain(t, store)
	sctx, _ := beginSession(t, store, "agent-a")
	requirePresent(t, store, chain[:len(chain)-1], true)

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	// the commit copies what it relied on while the next mark runs
	gcAfterBatch = func(int) error {
		gcAfterBatch = nil
		return store.Put(sctx, chain[len(chain)-1])
	}
	t.Cleanup(func() { gcAfterBatch = nil })

	report, err := store.Collect(ctx, gcOptions(t, 48*time.Hour))
	require.NoError(t, err)
	require.NotZero(t, report.ReclaimedObjects)

	requireStored(t, store, chain, true)
}
