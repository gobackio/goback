package sql

import (
	"testing"
	"time"

	"github.com/gobackio/goback/proto"
	"github.com/stretchr/testify/require"
)

func (f *fixture) commitAt(set string, root *proto.Object, received time.Time) int64 {
	f.t.Helper()

	obj := proto.NewObject(&proto.Commit{Timestamp: received.Unix(), Tree: root.Ref(), BackupSet: set, AgentId: "node-1", ReceivedAtNs: received.UnixNano()})
	require.NoError(f.t, f.x.Put(f.ctx, obj))

	return obj.GetCommit().GetReceivedAtNs()
}

func TestACommitKeepsTheReceiptTimeItCarriesWithinItsBounds(t *testing.T) {
	f := newFixture(t)
	root := f.tree(f.file("a.txt", "one"))

	offline := f.clock.Add(-6 * time.Hour)
	require.Equal(t, offline.UnixNano(), f.commitAt("world", root, offline), "a commit stored while offline keeps its time")

	later := offline.Add(time.Hour)
	require.Equal(t, later.UnixNano(), f.commitAt("world", root, later), "and so does the next one")

	f.stampedNow(f.commitAt("world", root, offline.Add(30*time.Minute)), "one before the set's newest")

	f.advance(time.Minute)
	f.stampedNow(f.commitAt("world", root, f.clock.Add(time.Hour)), "one from the future")

	f.advance(time.Minute)
	f.stampedNow(f.commitAt("other", root, f.clock.Add(-carriedReceipt-time.Hour)), "one older than the bound")
}

// stampedNow checks a receipt time is the index's own: its clock, give or
// take the microseconds that keep its stamps apart.
func (f *fixture) stampedNow(received int64, what string) {
	f.t.Helper()

	require.InDelta(f.t, f.clock.UnixNano(), received, float64(time.Millisecond), "%s is stamped now", what)
}
