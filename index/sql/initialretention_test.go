package sql

import (
	"context"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// commitIn stores a commit to set through the index under ctx.
func (f *fixture) commitIn(ctx context.Context, set string) error {
	f.advance(time.Second)

	return f.x.Put(ctx, proto.NewObject(&proto.Commit{
		Timestamp: f.clock.Unix(), Tree: f.tree(f.file("a.txt", set)).Ref(), BackupSet: set, AgentId: "node-1",
	}))
}

func (f *fixture) ownPolicy(x *Index, set string) *retention.Policy {
	f.t.Helper()

	ret, err := x.GetPolicy(f.ctx, set)
	require.NoError(f.t, err)
	require.False(f.t, ret.Paused)

	return ret.Policy
}

func (f *fixture) session(set string, initial *retention.Policy) context.Context {
	return backup.WithSession(f.ctx, &backup.Session{AgentID: "node-1", Set: set, InitialRetention: initial})
}

func TestASetCreatedUnderASessionStartsWithItsInitialRetention(t *testing.T) {
	f := newFixture(t)

	base := retention.DatabaseBase
	require.NoError(t, f.commitIn(f.session("db/base", &base), "db/base"))
	require.Equal(t, &base, f.ownPolicy(f.x, "db/base"), "the commit created the set")

	log := retention.DatabaseLog
	_, err := f.x.BeginCommit(f.session("db/wal", &log), "db/wal")
	require.NoError(t, err)
	require.Equal(t, &log, f.ownPolicy(f.x, "db/wal"), "the gate created the set")

	require.NoError(t, f.commitIn(f.session("db/base", &base), "other"))
	require.Nil(t, f.ownPolicy(f.x, "other"), "a session's policy is for its own set")

	y := f.index()
	require.NoError(t, reindexErr(y.ReIndex(f.ctx)))
	require.Equal(t, &base, f.ownPolicy(y, "db/base"), "a rebuild restores it")
	require.Equal(t, &log, f.ownPolicy(y, "db/wal"))
}

func TestAnExistingSetKeepsItsPolicyWhateverASessionCarries(t *testing.T) {
	f := newFixture(t)

	require.NoError(t, f.commitIn(f.ctx, "inherits"))
	require.NoError(t, f.commitIn(f.ctx, "inherited-again"))
	require.NoError(t, f.x.SetPolicy(f.ctx, "inherited-again", &retention.Policy{KeepLast: 3}))
	require.NoError(t, f.x.SetPolicy(f.ctx, "inherited-again", nil))
	require.NoError(t, f.commitIn(f.ctx, "own"))
	require.NoError(t, f.x.SetPolicy(f.ctx, "own", &retention.Policy{KeepLast: 7}))

	before, err := loadSettings(f.ctx, f.x.client)
	require.NoError(t, err)

	base := retention.DatabaseBase
	for _, set := range []string{"inherits", "inherited-again", "own"} {
		ctx := f.session(set, &base)

		_, err := f.x.BeginCommit(ctx, set)
		require.NoError(t, err)
		require.NoError(t, f.commitIn(ctx, set))
	}

	require.Nil(t, f.ownPolicy(f.x, "inherits"))
	require.Nil(t, f.ownPolicy(f.x, "inherited-again"), "an explicit inherit stays")
	require.Equal(t, &retention.Policy{KeepLast: 7}, f.ownPolicy(f.x, "own"))

	after, err := loadSettings(f.ctx, f.x.client)
	require.NoError(t, err)
	require.Equal(t, before.PolicySequence, after.PolicySequence, "nothing changed, so nothing is recorded")
}

func TestASessionKeepsItsInitialRetention(t *testing.T) {
	f := newFixture(t)

	log := retention.DatabaseLog
	s := &backup.Session{ID: "s1", AgentID: "node-1", Set: "db/wal", InitialRetention: &log, Started: f.clock, LastSeen: f.clock}
	require.NoError(t, f.x.BeginSession(s))
	require.NoError(t, f.x.BeginSession(&backup.Session{ID: "s2", AgentID: "node-1", Set: "db/base", Started: f.clock, LastSeen: f.clock}))

	read, err := f.x.GetSession("s1")
	require.NoError(t, err)
	require.Equal(t, &log, read.InitialRetention)

	read, err = f.x.GetSession("s2")
	require.NoError(t, err)
	require.Nil(t, read.InitialRetention)
}
