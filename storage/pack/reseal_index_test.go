package pack_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/index"
	"github.com/gobackio/goback/index/sql"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage"
	"github.com/gobackio/goback/storage/maintenance"
	"github.com/gobackio/goback/storage/pack"
	"github.com/gobackio/goback/testing/testpg"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/fileblob"
	pb "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

var memoryIndexes atomic.Int64

// resealIndexes names a database of each kind a store index runs on.
var resealIndexes = map[string]func(t *testing.T) string{
	"SQLite":   func(*testing.T) string { return fmt.Sprintf("memory://reseal-%d", memoryIndexes.Add(1)) },
	"Postgres": func(t *testing.T) string { return testpg.Start(t) },
}

func forEachIndex(t *testing.T, test func(t *testing.T, location string)) {
	for name, location := range resealIndexes {
		t.Run(name, func(t *testing.T) { test(t, location(t)) })
	}
}

// indexed is a pack store whose archive index is the index over it, as
// goback opens one.
type indexed struct {
	packs *pack.PackStorage
	x     *sql.Index
	keys  map[string]*storekey.Key
	// refused names keys the opener does not give
	refused map[string]bool
}

func openIndexed(t *testing.T, location string) *indexed {
	t.Helper()

	bucket, err := fileblob.OpenBucket(t.TempDir(), nil)
	require.NoError(t, err)

	x := sql.New(location, nil)
	x.DefaultPolicy = &retention.KeepAll

	packs, err := pack.NewPackStorage(pack.WithArchiveStorage(storage.NewBucketStore(bucket)), pack.WithArchiveIndex(x),
		pack.WithCompaction(pack.CompactionConfig{Workers: 1}))
	require.NoError(t, err)

	x.ObjectStore = packs
	require.NoError(t, x.Open())
	require.NoError(t, packs.Open())
	t.Cleanup(func() {
		_ = packs.Close()
		_ = x.Close()
	})

	return &indexed{packs: packs, x: x, keys: map[string]*storekey.Key{}, refused: map[string]bool{}}
}

func (s *indexed) newKey(t *testing.T, name string) *storekey.Key {
	t.Helper()

	key, err := storekey.Generate(name)
	require.NoError(t, err)
	s.keys[string(key.ID())] = key

	return key
}

func (s *indexed) opener(_ context.Context, keyID []byte) (*storekey.Key, error) {
	if s.refused[string(keyID)] {
		return nil, fmt.Errorf("key %x is not to hand", keyID)
	}

	return s.keys[string(keyID)], nil
}

func (s *indexed) tree(t *testing.T, nodes ...*proto.TreeNode) *proto.Ref {
	t.Helper()

	ref, err := backup.PutTree(context.Background(), s.packs, nodes, nil, nil)
	require.NoError(t, err)

	return ref
}

// commit indexes a commit of the set over root, received at, after parent.
func (s *indexed) commit(t *testing.T, key *storekey.Key, at time.Time, root, parent *proto.Ref) *proto.Ref {
	t.Helper()

	commit := proto.NewObject(&proto.Commit{Timestamp: at.Unix(), Tree: root, BackupSet: "world", AgentId: "agent", Parent: parent,
		KeyId: key.ID(), ReceivedAtNs: at.UnixNano()})
	require.NoError(t, s.x.Put(context.Background(), commit))

	return commit.Ref()
}

func (s *indexed) reseal(t *testing.T, follower pack.ResealFollower) (*pack.ResealReport, error) {
	t.Helper()

	if follower == nil {
		return maintenance.Reseal(context.Background(), s.packs, s.x, s.opener)
	}

	return s.packs.Reseal(context.Background(), s.opener, follower)
}

func indexFileNode(name string, size int, ref *proto.Ref) *proto.TreeNode {
	return &proto.TreeNode{Stat: &proto.FileInfo{Name: []byte(name), Mode: 0o644, Size: int64(size), MtimeNs: int64(size), Type: proto.NodeType_NODE_FILE}, Ref: ref}
}

func indexDirNode(name string, ref *proto.Ref) *proto.TreeNode {
	return &proto.TreeNode{Stat: &proto.FileInfo{Name: []byte(name), Mode: 0o755, Type: proto.NodeType_NODE_DIRECTORY}, Ref: ref}
}

func content(random *rand.Rand, size int) []byte {
	out := make([]byte, size)
	random.Read(out)

	for i := range out {
		out[i] = 'a' + out[i]&7
	}

	return out
}

// followFixture is a set of four commits over files sealed the way a store
// held them before Reseal: the first pinned, the second retired by the
// set's policy, the third in the trash and the fourth the newest, holding
// what files each holds, by path, with damage, a presence filter and
// store settings recorded.
type followFixture struct {
	*indexed
	key      *storekey.Key
	commits  []*proto.Ref
	holds    []map[string][]byte
	setID    int64
	presence int
	// keyOf names the key of a path sealed with another than key
	keyOf map[string]*storekey.Key
}

func newFollowFixture(t *testing.T, location string) *followFixture {
	t.Helper()
	ctx := context.Background()

	f := &followFixture{indexed: openIndexed(t, location)}
	f.key = f.newKey(t, "world")

	random := rand.New(rand.NewSource(1))
	a, meta, b1, b2, c := content(random, 40<<10), content(random, 5000), content(random, 30<<10), content(random, 30<<10), content(random, 10<<10)

	withMeta := indexFileNode("a.txt", len(a), pack.LegacyFile(t, f.packs, f.key, a, 16<<10))
	withMeta.Stat.Streams = []*proto.Stream{{Name: []byte("meta"), Ref: pack.LegacyFile(t, f.packs, f.key, meta, 4096), Size: int64(len(meta))}}
	cNode := indexFileNode("c.txt", len(c), pack.LegacyFile(t, f.packs, f.key, c, 4096))
	sub1 := f.tree(t, indexFileNode("b.txt", len(b1), pack.LegacyFile(t, f.packs, f.key, b1, 8192)))
	sub2 := f.tree(t, indexFileNode("b.txt", len(b2)+1, pack.LegacyFile(t, f.packs, f.key, append(b2, 'x'), 8192)))

	roots := []*proto.Ref{
		f.tree(t, withMeta, indexDirNode("sub", sub1)),
		f.tree(t, withMeta, indexDirNode("sub", sub2)),
		f.tree(t, withMeta, cNode, indexDirNode("sub", sub2)),
		f.tree(t, withMeta, cNode, indexDirNode("sub", sub1)),
	}
	f.holds = []map[string][]byte{
		{"a.txt": a, "sub/b.txt": b1},
		{"a.txt": a, "sub/b.txt": append(b2, 'x')},
		{"a.txt": a, "c.txt": c, "sub/b.txt": append(b2, 'x')},
		{"a.txt": a, "c.txt": c, "sub/b.txt": b1},
	}
	require.NoError(t, f.packs.Flush())

	base := time.Now().Add(-time.Hour)

	var parent *proto.Ref
	for i, root := range roots {
		parent = f.commit(t, f.key, base.Add(time.Duration(i)*time.Minute), root, parent)
		f.commits = append(f.commits, parent)
	}

	require.NoError(t, f.x.Put(ctx, proto.NewObject(&proto.Pin{Target: f.commits[0], Metadata: map[string]string{"why": "release"}})))
	require.NoError(t, f.x.SetWindows(ctx, index.Windows{TrashDays: 9}))
	require.NoError(t, f.x.SetDefaultPolicy(ctx, &retention.Policy{KeepLast: 10}))
	require.NoError(t, f.x.DeleteCommit(ctx, f.commits[2]))
	require.NoError(t, f.x.SetPolicy(ctx, "world", &retention.Policy{KeepLast: 1}))

	info, err := f.x.GetSet(ctx, "world")
	require.NoError(t, err)
	f.setID = info.ID
	require.NoError(t, f.x.MarkDamaged(ctx, f.setID, []string{"sub/b.txt"}))

	f.presence, err = f.x.BuildPendingPresence(ctx)
	require.NoError(t, err)

	return f
}

// followed is what the index answers about the set: its commits, by ref,
// as live, retired or trashed commits, its pins, policy and damage, the
// store's settings, and the files and directories each commit holds.
type followed struct {
	Details  map[string]index.CommitDetail
	Retired  map[string]index.Retired
	Trashed  map[string]*proto.TrashedCommit
	Pins     map[string]*proto.PinInfo
	Policy   index.SetRetention
	Default  retention.Policy
	Windows  index.Windows
	Damaged  []string
	Latest   string
	Listings map[string]map[string]listed
}

// listed is a path as the index lists it.
type listed struct {
	Ref  string
	Size int64
	Dir  bool
}

func (f *followFixture) followed(t *testing.T, commits []*proto.Ref) followed {
	t.Helper()
	ctx := context.Background()

	state := followed{Details: map[string]index.CommitDetail{}, Retired: map[string]index.Retired{}, Trashed: map[string]*proto.TrashedCommit{},
		Pins: map[string]*proto.PinInfo{}, Listings: map[string]map[string]listed{}}

	details, err := f.x.GetCommitDetails(ctx, "world", commits...)
	require.NoError(t, err)

	for _, detail := range details {
		state.Details[string(detail.Ref.Hash)] = detail
	}

	retired, err := f.x.RetiredCommits(ctx, commits)
	require.NoError(t, err)

	for _, r := range retired {
		state.Retired[string(r.Ref.Hash)] = r
		state.Listings[string(r.Ref.Hash)] = f.list(t, "", r.ReceivedAt)
	}

	trashed, err := f.x.GetTrashedCommits(ctx, "world", commits...)
	require.NoError(t, err)

	for _, commit := range trashed {
		state.Trashed[string(commit.Ref.Hash)] = commit
	}

	pins, err := f.x.PinsOf(ctx, commits...)
	require.NoError(t, err)

	for _, p := range pins {
		state.Pins[string(p.Ref.Hash)] = p
	}

	state.Policy, err = f.x.GetPolicy(ctx, "world")
	require.NoError(t, err)

	state.Default, _, err = f.x.GetDefaultPolicy(ctx)
	require.NoError(t, err)

	state.Windows, err = f.x.Windows(ctx)
	require.NoError(t, err)

	grant, err := f.x.BeginCommit(ctx, "world")
	require.NoError(t, err)
	state.Damaged = grant.Damaged

	latest, err := f.x.LatestCommit(ctx, "world")
	require.NoError(t, err)
	state.Latest = string(latest.Hash)

	return state
}

// list is what the index lists below dir at the moment at.
func (f *followFixture) list(t *testing.T, dir string, at time.Time) map[string]listed {
	t.Helper()

	entries, err := f.x.ReadDir(context.Background(), "world", dir, at)
	require.NoError(t, err)

	out := map[string]listed{}

	for _, entry := range entries {
		path := proto.JoinPath(dir, entry.Stat.Name)
		isDir := entry.Stat.Type == proto.NodeType_NODE_DIRECTORY
		out[path] = listed{Ref: string(entry.Ref.GetHash()), Size: entry.Stat.Size, Dir: isDir}

		if isDir {
			for below, entry := range f.list(t, path, at) {
				out[below] = entry
			}
		}
	}

	return out
}

// stored is what the tree holds, as list puts it.
func (f *followFixture) stored(t *testing.T, dir string, root *proto.Ref) map[string]listed {
	t.Helper()

	tree, err := backup.LoadTree(context.Background(), f.packs, root)
	require.NoError(t, err)

	out := map[string]listed{}

	for _, node := range tree.Nodes {
		path := proto.JoinPath(dir, node.Stat.Name)

		if node.Stat.Type == proto.NodeType_NODE_DIRECTORY {
			out[path] = listed{Ref: string(node.Ref.Hash), Dir: true}

			for below, entry := range f.stored(t, path, node.Ref) {
				out[below] = entry
			}

			continue
		}

		out[path] = listed{Ref: string(node.Ref.Hash), Size: node.Stat.Size}
	}

	return out
}

func (f *followFixture) commitObject(t *testing.T, ref *proto.Ref) *proto.Commit {
	t.Helper()

	obj, err := f.packs.Get(context.Background(), ref)
	require.NoError(t, err)

	return obj.GetCommit()
}

// translated is what the index should answer once the reseal the reports
// tell of replaced every commit of before.
func (f *followFixture) translated(t *testing.T, before followed, reports ...*pack.ResealReport) followed {
	t.Helper()

	commits, pins := map[string]*proto.Ref{}, map[string]*proto.Ref{}

	for _, report := range reports {
		for _, pair := range report.Resealed {
			commits[string(pair.Old.Hash)] = pair.New

			for _, p := range pair.Pins {
				pins[string(p.Old.Hash)] = p.New
			}
		}
	}

	next := func(ref string) *proto.Ref {
		replacement := commits[ref]
		require.NotNil(t, replacement, "commit %x is replaced", ref)

		return replacement
	}

	commit := func(old *proto.Commit, ref *proto.Ref) *proto.Commit {
		c := pb.Clone(old).(*proto.Commit)
		c.Tree = f.commitObject(t, ref).Tree

		if c.Parent != nil {
			c.Parent = next(string(c.Parent.Hash))
		}

		return c
	}

	want := before
	want.Details, want.Retired, want.Trashed, want.Pins = map[string]index.CommitDetail{}, map[string]index.Retired{}, map[string]*proto.TrashedCommit{}, map[string]*proto.PinInfo{}
	want.Listings = map[string]map[string]listed{}

	for old, detail := range before.Details {
		ref := next(old)
		detail.Ref, detail.Commit = ref, commit(detail.Commit, ref)
		want.Details[string(ref.Hash)] = detail
	}

	for old, r := range before.Retired {
		ref := next(old)
		r.Ref = ref
		want.Retired[string(ref.Hash)] = r

		listing := map[string]listed{}
		holds := f.stored(t, "", f.commitObject(t, ref).Tree)

		for path, entry := range before.Listings[old] {
			replaced := holds[path]
			require.Equal(t, entry.Dir, replaced.Dir, path)
			entry.Ref = replaced.Ref
			listing[path] = entry
		}

		want.Listings[string(ref.Hash)] = listing
	}

	for old, trashed := range before.Trashed {
		ref := next(old)
		moved := pb.Clone(trashed).(*proto.TrashedCommit)
		moved.Ref, moved.Commit = ref, commit(trashed.Commit, ref)
		want.Trashed[string(ref.Hash)] = moved
	}

	for old, p := range before.Pins {
		moved := pb.Clone(p).(*proto.PinInfo)
		moved.Ref, moved.Target = pins[old], next(string(p.Target.Hash))
		require.NotNil(t, moved.Ref, "pin %x is moved", old)
		want.Pins[string(moved.Ref.Hash)] = moved
	}

	want.Latest = string(next(before.Latest).Hash)

	return want
}

func requireFollowed(t *testing.T, want, got followed) {
	t.Helper()

	require.Empty(t, cmp.Diff(want, got, protocmp.Transform()), "the index answers for the replacements as it did for what they replaced")
}

// replacements maps each commit the reports replaced to its replacement.
func replacements(reports ...*pack.ResealReport) []*proto.Ref {
	var out []*proto.Ref
	for _, report := range reports {
		for _, pair := range report.Resealed {
			out = append(out, pair.New)
		}
	}

	return out
}

// requireReadable checks that each file the index lists at each commit
// restores through the index as holds has it, and that the set reads
// every ref listed.
func (f *followFixture) requireReadable(t *testing.T, commits []*proto.Ref, holds []map[string][]byte) {
	t.Helper()
	ctx := context.Background()

	retired, err := f.x.RetiredCommits(ctx, commits)
	require.NoError(t, err)
	require.Len(t, retired, len(commits))

	for i, r := range retired {
		listing := f.list(t, "", r.ReceivedAt)
		require.NotEmpty(t, listing)

		for path, entry := range listing {
			readable, err := f.x.References(ctx, &proto.Ref{Hash: []byte(entry.Ref)})
			require.NoError(t, err)
			require.True(t, readable, "the set reads %s", path)
		}

		for path, entry := range listing {
			if entry.Dir {
				continue
			}

			want, ok := holds[i][path]
			require.True(t, ok, "commit %d lists only what it holds, not %s", i+1, path)

			key := f.key
			if other := f.keyOf[path]; other != nil {
				key = other
			}

			restorer := &backup.Restorer{Store: f.x, Key: key}
			data, err := restorer.ReadFile(ctx, &proto.FileInfo{Name: []byte(path), Size: entry.Size, Type: proto.NodeType_NODE_FILE},
				&proto.Ref{Hash: []byte(entry.Ref)})
			require.NoError(t, err, path)
			require.Equal(t, want, data, "%s restores at commit %d", path, i+1)
		}
	}
}

// at is when the index received the commit.
func (f *followFixture) at(t *testing.T, ref *proto.Ref) time.Time {
	t.Helper()

	retired, err := f.x.RetiredCommits(context.Background(), []*proto.Ref{ref})
	require.NoError(t, err)
	require.Len(t, retired, 1)

	return retired[0].ReceivedAt
}

func collect(t *testing.T, packs *pack.PackStorage) {
	t.Helper()

	for _, ahead := range []time.Duration{0, 48 * time.Hour} {
		_, err := packs.Collect(context.Background(), pack.CollectOptions{Now: time.Now().Add(ahead), DeadRatio: 1e-9, TempDir: t.TempDir(), Readers: 4})
		require.NoError(t, err)
	}
}

func TestTheIndexFollowsAResealInPlace(t *testing.T) {
	forEachIndex(t, func(t *testing.T, location string) {
		ctx := context.Background()
		f := newFollowFixture(t, location)
		require.Equal(t, 1, f.presence)

		before := f.followed(t, f.commits)
		require.Len(t, before.Details, 2, "the pinned and the newest commit are live")
		require.Len(t, before.Trashed, 1)
		require.Len(t, before.Pins, 1)
		require.False(t, before.Retired[string(f.commits[1].Hash)].RetiredAt.IsZero(), "the second commit is retired")
		require.Equal(t, []string{"sub/b.txt"}, before.Damaged)

		report, err := f.reseal(t, nil)
		require.NoError(t, err)
		require.Empty(t, report.Failed)
		require.Len(t, report.Resealed, 4)

		after := replacements(report)
		requireFollowed(t, f.translated(t, before, report), f.followed(t, after))

		gone, err := f.x.GetCommitDetails(ctx, "world", f.commits...)
		require.NoError(t, err)
		require.Empty(t, gone, "no old commit is left live")

		for _, ref := range []*proto.Ref{after[0], after[3]} {
			require.Equal(t, f.stored(t, "", f.commitObject(t, ref).Tree), f.list(t, "", f.at(t, ref)), "a live commit lists what its replacement holds")
		}

		for _, listing := range before.Listings {
			for path, entry := range listing {
				readable, err := f.x.References(ctx, &proto.Ref{Hash: []byte(entry.Ref)})
				require.NoError(t, err)
				require.False(t, readable, "the set no longer reads what %s was", path)
			}
		}

		f.requireReadable(t, after, f.holds)

		filters, err := f.x.PresenceOf(ctx, "world")
		require.NoError(t, err)
		require.Empty(t, filters, "a filter naming the old blobs is dropped")

		built, err := f.x.BuildPendingPresence(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, built)

		filters, err = f.x.PresenceOf(ctx, "world")
		require.NoError(t, err)
		require.Len(t, filters, 1)
		require.True(t, filters[0].Commit.Equal(after[3]), "maintenance builds the newest replacement's filter")

		again, err := f.reseal(t, nil)
		require.NoError(t, err)
		require.Empty(t, again.Resealed)
		requireFollowed(t, f.translated(t, before, report), f.followed(t, after))

		collect(t, f.packs)

		for _, listing := range before.Listings {
			for path, entry := range listing {
				_, err := f.packs.Get(ctx, &proto.Ref{Hash: []byte(entry.Ref)})
				require.ErrorIs(t, err, backup.ErrNotFound, "what %s was is collected", path)
			}
		}

		f.requireReadable(t, after, f.holds)
	})
}

// stoppingFollower stops the reseal at the second commit it is told of,
// having passed it on when through says so.
type stoppingFollower struct {
	pack.ResealFollower
	stop    context.CancelFunc
	through bool
	seen    int
}

func (s *stoppingFollower) ReplaceCommit(ctx context.Context, commit pack.ResealedCommit) error {
	if s.seen++; s.seen < 2 {
		return s.ResealFollower.ReplaceCommit(ctx, commit)
	}

	if s.through {
		if err := s.ResealFollower.ReplaceCommit(ctx, commit); err != nil {
			return err
		}
	}

	s.stop()

	return ctx.Err()
}

func TestAResealStoppedBeforeOrAfterItsIndexFollowedCarriesOnWhenRunAgain(t *testing.T) {
	for name, through := range map[string]bool{"Before": false, "After": true} {
		t.Run(name, func(t *testing.T) {
			forEachIndex(t, func(t *testing.T, location string) {
				f := newFollowFixture(t, location)
				before := f.followed(t, f.commits)

				ctx, stop := context.WithCancel(context.Background())
				first, err := f.packs.Reseal(ctx, f.opener, &stoppingFollower{ResealFollower: f.x, stop: stop, through: through})
				require.ErrorIs(t, err, context.Canceled)
				require.Len(t, first.Resealed, 1)

				rest, err := f.reseal(t, nil)
				require.NoError(t, err)
				require.Empty(t, rest.Failed)

				after := replacements(first, rest)
				require.Len(t, after, 4)
				requireFollowed(t, f.translated(t, before, first, rest), f.followed(t, after))
				f.requireReadable(t, after, f.holds)
			})
		})
	}
}

func TestTheIndexKeepsACommitTheResealLeftAsItWas(t *testing.T) {
	forEachIndex(t, func(t *testing.T, location string) {
		ctx := context.Background()
		f := &followFixture{indexed: openIndexed(t, location)}
		f.key = f.newKey(t, "world")
		other := f.newKey(t, "other")
		f.keyOf = map[string]*storekey.Key{"b.txt": other}

		random := rand.New(rand.NewSource(2))
		a, b, c := content(random, 40<<10), content(random, 20<<10), content(random, 20<<10)

		aNode := indexFileNode("a.txt", len(a), pack.LegacyFile(t, f.packs, f.key, a, 16<<10))
		foreign := indexFileNode("b.txt", len(b), pack.LegacyFile(t, f.packs, other, b, 8192))
		sub := f.tree(t, indexFileNode("c.txt", len(c), pack.LegacyFile(t, f.packs, f.key, c, 8192)))

		roots := []*proto.Ref{
			f.tree(t, aNode, indexDirNode("sub", sub)),
			f.tree(t, aNode, foreign, indexDirNode("sub", sub)),
			f.tree(t, aNode, indexDirNode("sub", sub)),
		}
		holds := []map[string][]byte{{"a.txt": a, "sub/c.txt": c}, {"a.txt": a, "b.txt": b, "sub/c.txt": c}, {"a.txt": a, "sub/c.txt": c}}
		require.NoError(t, f.packs.Flush())

		base := time.Now().Add(-time.Hour)

		var parent *proto.Ref
		for i, root := range roots {
			parent = f.commit(t, f.key, base.Add(time.Duration(i)*time.Minute), root, parent)
			f.commits = append(f.commits, parent)
		}

		kept := f.commits[1]
		before := f.list(t, "", f.at(t, kept))
		f.refused[string(other.ID())] = true

		report, err := f.reseal(t, nil)
		require.NoError(t, err)
		require.Len(t, report.Failed, 1)
		require.True(t, report.Failed[0].Ref.Equal(kept))
		require.Len(t, report.Resealed, 2)

		resealed := replacements(report)
		requireKept := func() {
			t.Helper()

			listing := f.list(t, "", f.at(t, kept))
			require.Equal(t, before, listing, "the commit left as it was lists what it holds")
			require.Equal(t, f.stored(t, "", roots[1]), listing)

			for path, entry := range listing {
				readable, err := f.x.References(ctx, &proto.Ref{Hash: []byte(entry.Ref)})
				require.NoError(t, err)
				require.True(t, readable, "the set reads %s as the commit left as it was holds it", path)

				_, err = f.packs.Get(ctx, &proto.Ref{Hash: []byte(entry.Ref)})
				require.NoError(t, err, "%s as the commit left as it was holds it is stored", path)
			}

			for _, ref := range resealed {
				require.Equal(t, f.stored(t, "", f.commitObject(t, ref).Tree), f.list(t, "", f.at(t, ref)), "a replacement lists what it holds")
			}

			f.requireReadable(t, resealed, []map[string][]byte{holds[0], holds[2]})
		}

		requireKept()
		collect(t, f.packs)
		requireKept()

		delete(f.refused, string(other.ID()))

		again, err := f.reseal(t, nil)
		require.NoError(t, err)
		require.Empty(t, again.Failed)
		require.Len(t, again.Resealed, 2, "the commit left as it was is replaced once its key is to hand, and its child over it")
		require.True(t, again.Resealed[0].Old.Equal(kept))
		require.True(t, again.Resealed[1].Old.Equal(resealed[1]))

		all := []*proto.Ref{resealed[0], again.Resealed[0].New, again.Resealed[1].New}
		for _, ref := range all {
			require.Equal(t, f.stored(t, "", f.commitObject(t, ref).Tree), f.list(t, "", f.at(t, ref)))
		}

		f.requireReadable(t, all, holds)
		collect(t, f.packs)
		f.requireReadable(t, all, holds)
	})
}
