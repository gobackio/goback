package sql

import (
	"testing"
	"time"

	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/index/sql/ent/set"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/maintenance"
	"github.com/gobackio/goback/storage/pack"

	"github.com/stretchr/testify/require"
)

var _ maintenance.Forecaster = (*Index)(nil)

func TestRootDeathExpectsEachCommitToDieWhenItsPolicyLetsItGo(t *testing.T) {
	f := newFixture(t)
	hourly := &retention.Policy{KeepLast: 1, Brackets: []retention.Bracket{{Period: retention.Hourly, Count: 48}}}

	var thinned, paused, trashed []*proto.Ref
	for range 3 {
		thinned = append(thinned, f.commit("thinned", f.tree(f.file("a.txt", "thinned")), false))
		paused = append(paused, f.commit("paused", f.tree(f.file("a.txt", "paused")), false))
		trashed = append(trashed, f.commit("trashed", f.tree(f.file("a.txt", "trashed")), false))
		f.advance(time.Hour)
	}

	single := f.commit("single", f.tree(f.file("a.txt", "single")), false)

	for _, name := range []string{"thinned", "paused", "single"} {
		require.NoError(t, f.x.SetPolicy(f.ctx, name, hourly))
	}
	require.NoError(t, f.x.client.Set.Update().Where(set.Name("paused")).SetRetentionPaused(true).Exec(f.ctx))

	pin, err := f.pin(thinned[0])
	require.NoError(t, err)

	require.NoError(t, f.x.DeleteCommit(f.ctx, trashed[0]))
	expires := f.commitRow(trashed[0]).ExpiresAt
	require.NotNil(t, expires)

	death, err := f.x.RootDeath(f.ctx, f.clock)
	require.NoError(t, err)

	week := f.clock.Add(pack.DeathHorizons[0])
	for _, ref := range thinned[1:] {
		require.True(t, week.Equal(death(ref.Hash)), "the next commits supersede %x within the week: %s", ref.Hash, death(ref.Hash))
	}

	require.Zero(t, death(thinned[0].Hash), "a pinned commit is kept")
	require.Zero(t, death(pin.Hash), "a pin never dies")
	require.Zero(t, death(single.Hash), "with no cadence to go by, nothing arrives to supersede the newest")

	for _, ref := range paused {
		require.Zero(t, death(ref.Hash), "a set whose retention is paused keeps everything")
	}

	require.True(t, expires.Equal(death(trashed[0].Hash)), "a deleted commit dies when its trash window ends")
	require.Zero(t, death([]byte("something else")))
}
