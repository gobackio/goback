package sql

import (
	"context"
	"testing"
	"time"

	"github.com/gobackio/goback/progress"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// lastUpdates records an operation's updates and returns the last of
// each phase, in the order the phases came.
func lastUpdates(ctx context.Context) (context.Context, func() []progress.Update) {
	var updates []progress.Update
	ctx = progress.NewContext(ctx, func(u progress.Update) {
		if n := len(updates); n > 0 && updates[n-1].Phase == u.Phase {
			updates[n-1] = u
		} else {
			updates = append(updates, u)
		}
	})

	return ctx, func() []progress.Update { return updates }
}

func TestACommitReportsTheDirectoriesItIndexed(t *testing.T) {
	f := newFixture(t)
	root := f.tree(f.dir("a", f.dir("b", f.file("c.txt", "one"))), f.file("d.txt", "two"))

	ctx, updates := lastUpdates(f.ctx)
	commit := proto.NewObject(&proto.Commit{Timestamp: f.clock.Unix(), Tree: root.Ref(), BackupSet: "world", AgentId: "node-1"})
	require.NoError(t, f.x.Put(ctx, commit))

	require.Equal(t, []progress.Update{{Op: progress.OpCommit, Phase: progress.PhaseIndex, Done: 3}}, updates())
}

func TestRetireReportsItsPhases(t *testing.T) {
	f := newFixture(t)

	f.commit("world", f.tree(f.file("a.txt", "one")), false)
	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "two")), false)
	require.NoError(t, f.x.DeleteSet(f.ctx, "world", false))

	ctx, updates := lastUpdates(f.ctx)
	_, err := f.x.RetireCommits(ctx, f.clock.Add(15*24*time.Hour))
	require.NoError(t, err)

	op := progress.OpRetire
	require.Equal(t, []progress.Update{
		{Op: op, Phase: progress.PhaseDue},
		{Op: op, Phase: progress.PhaseTombstones, Done: 2, Total: 2},
		{Op: op, Phase: progress.PhaseRows, Done: 2, Total: 2},
		{Op: op, Phase: progress.PhasePrune, Done: 1, Total: 1},
	}, updates())
}

func TestReIndexReportsItsPhases(t *testing.T) {
	f := newFixture(t)

	f.commit("world", f.tree(f.file("a.txt", "one")), false)
	f.commit("other", f.tree(f.file("a.txt", "two")), false)

	ctx, updates := lastUpdates(f.ctx)
	require.NoError(t, reindexErr(f.index().ReIndex(ctx)))

	var phases []string
	for _, u := range updates() {
		require.Equal(t, progress.OpReIndex, u.Op)
		phases = append(phases, u.Phase)
	}

	require.Equal(t, []string{progress.PhaseTombstones, progress.PhasePins, progress.PhaseCommits, progress.PhaseSets,
		progress.PhasePolicies, progress.PhaseDamage}, phases)
	require.EqualValues(t, 2, updates()[2].Done, "both commits walked")
	require.Equal(t, progress.Update{Op: progress.OpReIndex, Phase: progress.PhaseSets, Done: 2, Total: 2}, updates()[3])
}
