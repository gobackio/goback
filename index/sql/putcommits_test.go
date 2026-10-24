package sql

import (
	"testing"
	"time"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

func TestCommitsPutTogetherAreReceivedInTheOrderGivenWithinEachSet(t *testing.T) {
	f := newFixture(t)

	newest := f.clock
	f.commit("world", f.tree(f.file("a.txt", "zero")), false)
	f.advance(time.Hour)

	carrying := func(set, content string, received time.Time) *proto.Object {
		return proto.NewObject(&proto.Commit{Timestamp: f.clock.Unix(), Tree: f.tree(f.file("a.txt", content)).Ref(),
			BackupSet: set, AgentId: "node-1", ReceivedAtNs: received.UnixNano()})
	}

	commits := []*proto.Object{
		// before the set's newest, so stamped now
		carrying("world", "one", newest.Add(-time.Hour)),
		// after the set's newest but before the commit ahead of it
		carrying("world", "two", newest.Add(30*time.Minute)),
		carrying("other", "three", newest.Add(10*time.Minute)),
		carrying("world", "four", f.clock),
	}

	require.NoError(t, f.x.PutCommits(f.ctx, commits))

	received := func(i int) time.Time { return time.Unix(0, int64(commits[i].GetCommit().GetReceivedAtNs())).UTC() }

	require.True(t, received(0).After(newest))
	require.True(t, received(1).After(received(0)), "a commit is received after those of its set before it")
	require.True(t, received(3).After(received(1)))
	require.Equal(t, newest.Add(10*time.Minute), received(2), "another set keeps the time its commit carries")

	world, err := f.x.CommitInfo(f.ctx, "world", f.clock.Add(time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, world, 4)

	latest, err := f.x.LatestCommit(f.ctx, "world")
	require.NoError(t, err)
	require.Equal(t, commits[3].Ref().Hash, latest.Hash)

	versions, err := f.x.FileInfo(f.ctx, "world", "a.txt", f.clock.Add(time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, versions, 4, "each commit keeps its own version")
}
