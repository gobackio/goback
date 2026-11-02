package sql

import (
	"fmt"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/index"
	"github.com/gobackio/goback/index/sql/ent/commitrow"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// unnamed stores, without indexing it, a commit that names no set but
// carries setID, an hour after the last.
func (f *fixture) unnamed(setID uint64, parent *proto.Ref, content string) *proto.Ref {
	f.t.Helper()
	f.advance(time.Hour)

	obj := proto.NewObject(&proto.Commit{Timestamp: f.clock.Unix(), Tree: f.tree(f.file("a.txt", content)).Ref(),
		AgentId: "node-1", ReceivedAtNs: f.clock.UnixNano(), SetId: setID, Parent: parent})
	require.NoError(f.t, f.store.Put(f.ctx, obj))

	return obj.Ref()
}

func (f *fixture) setNames(x *Index) map[int64]string {
	f.t.Helper()

	sets, err := x.client.Set.Query().All(f.ctx)
	require.NoError(f.t, err)

	names := map[int64]string{}
	for _, s := range sets {
		names[s.ID] = s.Name
	}

	return names
}

func TestReIndexPutsTheCommitsThatNameNoSetUnderAPlaceholderSet(t *testing.T) {
	f := newFixture(t)

	first := f.unnamed(1, nil, "one")
	f.unnamed(1, first, "two")
	f.advance(time.Hour)
	require.NoError(t, f.store.Put(f.ctx, proto.NewObject(&proto.Commit{Timestamp: f.clock.Unix(), Tree: f.tree(f.file("b.txt", "b")).Ref(),
		BackupSet: index.PlaceholderSet, AgentId: "node-1", ReceivedAtNs: f.clock.UnixNano(), SetId: 2})))

	y := f.index()
	report, err := y.ReIndex(f.ctx)
	require.NoError(t, err)
	require.Equal(t, backup.ReIndexReport{Unnamed: 2}, report)
	require.Equal(t, map[int64]string{1: index.PlaceholderSet + "-2", 2: index.PlaceholderSet}, f.setNames(y))

	commits, err := y.CommitInfo(f.ctx, index.PlaceholderSet+"-2", f.clock.Add(time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, commits, 2)
}

func TestReIndexIndexesTheGroupsOfOneSetInOrder(t *testing.T) {
	f := newFixture(t)

	for i := range 20 {
		f.advance(time.Hour)

		var id uint64
		if i%2 == 1 {
			id = 5
		}

		require.NoError(t, f.store.Put(f.ctx, proto.NewObject(&proto.Commit{Timestamp: f.clock.Unix(), Tree: f.tree(f.file("a.txt", fmt.Sprint(i))).Ref(),
			BackupSet: "web", AgentId: "node-1", ReceivedAtNs: f.clock.UnixNano(), SetId: id})))
	}

	y := f.index()
	report, err := y.ReIndex(f.ctx)
	require.NoError(t, err)
	require.Equal(t, backup.ReIndexReport{Behind: 10}, report, "the commits naming the set by id go first, the others then fall behind them")
	require.Equal(t, map[int64]string{5: "web"}, f.setNames(y))
}

func TestIndexCommitsPutsCommitsThatNameNoSetUnderAPlaceholderSet(t *testing.T) {
	f := newFixture(t)

	f.commit(index.PlaceholderSet, f.tree(f.file("b.txt", "b")), false)
	first := f.unnamed(7, nil, "one")
	second := f.unnamed(7, first, "two")
	oldest, newest := f.clock.Add(-time.Hour), f.clock

	want := index.IndexedSet{SetID: 7, Set: index.PlaceholderSet + "-2", Created: true, Placeholder: true, Commits: 2, Chained: true,
		Oldest: oldest, Newest: newest}

	planned, err := f.x.IndexCommits(f.ctx, []*proto.Ref{second, first}, true)
	require.NoError(t, err)
	require.Equal(t, []index.IndexedSet{want}, planned)
	require.NotContains(t, f.setNames(f.x), int64(7), "a dry run created the set")

	rows, err := f.x.client.CommitRow.Query().Where(commitrow.SetID(7)).Count(f.ctx)
	require.NoError(t, err)
	require.Zero(t, rows, "a dry run indexed commits")

	done, err := f.x.IndexCommits(f.ctx, []*proto.Ref{second, first}, false)
	require.NoError(t, err)

	want.Indexed = 2
	require.Equal(t, []index.IndexedSet{want}, done)
	require.Equal(t, want.Set, f.setNames(f.x)[7])

	commits, err := f.x.CommitInfo(f.ctx, want.Set, f.clock.Add(time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, commits, 2)
}

func TestIndexCommitsRefusesACommitThatAlreadyHasARow(t *testing.T) {
	f := newFixture(t)

	indexed := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	orphan := f.unnamed(7, nil, "two")

	_, err := f.x.IndexCommits(f.ctx, []*proto.Ref{orphan, indexed}, false)
	require.ErrorIs(t, err, index.ErrIndexed)

	exists, err := f.x.client.CommitRow.Query().Where(commitrow.Ref(orphan.Hash)).Exist(f.ctx)
	require.NoError(t, err)
	require.False(t, exists, "a refused run indexed the other commits")
}
