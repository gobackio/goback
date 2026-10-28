package pack

import (
	"context"
	"testing"
	"time"

	"github.com/gobackio/goback/progress"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// recordProgress returns a context that records the updates of one
// operation, and a check that they came in the phases given, each moving
// forward and ending at the totals it named, which returns them.
func recordProgress(t *testing.T, op string) (context.Context, func(phases ...string) []progress.Update) {
	var updates []progress.Update
	ctx := progress.NewContext(context.Background(), func(u progress.Update) { updates = append(updates, u) })

	return ctx, func(phases ...string) []progress.Update {
		t.Helper()

		var seen []string
		for i, u := range updates {
			require.Equal(t, op, u.Op)

			if i == 0 || updates[i-1].Phase != u.Phase {
				seen = append(seen, u.Phase)
			} else {
				require.GreaterOrEqual(t, u.Done, updates[i-1].Done, "%s moves forward", u.Phase)
				require.GreaterOrEqual(t, u.Bytes, updates[i-1].Bytes, "%s moves forward", u.Phase)
			}

			if u.Total > 0 {
				require.LessOrEqual(t, u.Done, u.Total, u.Phase)
			}

			if u.BytesTotal > 0 {
				require.LessOrEqual(t, u.Bytes, u.BytesTotal, u.Phase)
			}

			if last := i == len(updates)-1 || updates[i+1].Phase != u.Phase; last {
				if u.Total > 0 {
					require.Equal(t, u.Total, u.Done, "%s ends at its total", u.Phase)
				}

				if u.BytesTotal > 0 {
					require.Equal(t, u.BytesTotal, u.Bytes, "%s ends at its byte total", u.Phase)
				}
			}
		}

		require.Equal(t, phases, seen)

		checked := updates
		updates = nil

		return checked
	}
}

func TestCollectReportsItsPhases(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })

	reachable, unreachable := makeGCTestData(t)
	putAll(t, store, append(append([]*proto.Object{}, reachable...), unreachable...))

	ctx, check := recordProgress(t, progress.OpCollect)

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)
	check(progress.PhaseSnapshot, progress.PhaseRoots, progress.PhaseMark, progress.PhaseMerge, progress.PhaseCondemn, progress.PhasePurge)

	second, err := store.Collect(ctx, gcOptions(t, 48*time.Hour))
	require.NoError(t, err)
	require.Positive(t, second.Swept)
	check(progress.PhaseSnapshot, progress.PhaseRoots, progress.PhaseMark, progress.PhaseMerge, progress.PhaseConfirm,
		progress.PhaseSweep, progress.PhasePurge)
}

func TestCompactAndScrubReportArchivesAndBytes(t *testing.T) {
	store := newTestStore(t, t.TempDir(), WithCompaction(CompactionConfig{MinimumCandidates: 1}))
	t.Cleanup(func() { _ = store.Close() })

	for range 3 {
		putAll(t, store, makeChain(makeTestData(t, 10)))
	}

	ctx, check := recordProgress(t, progress.OpCompact)
	compact(t, ctx, store)
	check(progress.PhaseRewrite)

	ctx, check = recordProgress(t, progress.OpScrub)
	_, err := store.Scrub(ctx)
	require.NoError(t, err)
	check(progress.PhaseScrub)
}

func TestResealReportsItsCommitsPinsAndBytes(t *testing.T) {
	a, b := resealKeys(t)
	f := newResealFixture(t, a, b)
	t.Cleanup(func() { _ = f.store.Close() })

	ctx, check := recordProgress(t, progress.OpReseal)

	_, err := f.store.Reseal(ctx, f.opener, nil)
	require.NoError(t, err)

	last := map[string]progress.Update{}
	for _, u := range check(progress.PhaseCommits, progress.PhasePins) {
		last[u.Phase] = u
	}

	commits := last[progress.PhaseCommits]
	require.EqualValues(t, len(f.commits)+1, commits.Total, "every live commit, the one in the clear too")
	require.Equal(t, commits.Total, commits.Done)
	require.Positive(t, commits.Bytes)
	require.EqualValues(t, 1, last[progress.PhasePins].Done, "the pin of a tree")
}
