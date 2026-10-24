package pack

import (
	"bytes"
	"context"
	"crypto/rand"
	mrand "math/rand"
	"testing"
	"time"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// deathFixture is a store holding what only a soon-dying commit reaches,
// what only a commit that never dies reaches, a file and its blobs both
// reach, and unreachable blobs, shuffled together into small archives.
type deathFixture struct {
	store      *PackStorage
	soon, keep *proto.Object
	// label and metaLabel name the blobs, and the other objects, as only
	// the soon commit's, only the kept one's or shared
	label, metaLabel map[string]string
	soonObjects      []*proto.Object
	keepObjects      []*proto.Object
}

func randomBlobs(n int) []*proto.Object {
	var out []*proto.Object
	for range n {
		data := make([]byte, 4096)
		_, _ = rand.Read(data)
		out = append(out, proto.NewObject(&proto.Blob{Data: data}))
	}

	return out
}

func fileOfBlobs(blobs []*proto.Object) *proto.Object {
	var parts []*proto.FilePart
	for i, b := range blobs {
		parts = append(parts, &proto.FilePart{Ref: b.Ref(), Offset: uint64(i) * 4096, Length: 4096})
	}

	return proto.NewObject(&proto.File{Parts: parts})
}

// commitOfFiles is a commit whose tree holds the files, and the objects
// it adds to them.
func commitOfFiles(name string, files ...*proto.Object) (*proto.Object, []*proto.Object) {
	tree := treeOf(files)
	commit := proto.NewObject(&proto.Commit{Tree: tree.Ref(), Timestamp: 1, BackupSet: name})

	return commit, []*proto.Object{tree, commit}
}

func newDeathFixture(t *testing.T, opts ...PackOption) *deathFixture {
	store := newTestStore(t, t.TempDir(), opts...)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	soonOnly, keepOnly, shared := randomBlobs(30), randomBlobs(30), randomBlobs(10)

	soonFile, keepFile, sharedFile := fileOfBlobs(soonOnly), fileOfBlobs(keepOnly), fileOfBlobs(shared)

	soon, soonMeta := commitOfFiles("soon", soonFile, sharedFile)
	keep, keepMeta := commitOfFiles("keep", keepFile, sharedFile)
	soonMeta = append(soonMeta, soonFile, sharedFile)
	keepMeta = append(keepMeta, keepFile)

	f := &deathFixture{store: store, soon: soon, keep: keep, label: make(map[string]string), metaLabel: make(map[string]string)}
	for label, objects := range map[string][]*proto.Object{"soon": soonOnly, "keep": keepOnly, "shared": shared} {
		for _, o := range objects {
			f.label[string(o.Ref().Hash)] = label
		}
	}

	for label, objects := range map[string][]*proto.Object{"soon": append([]*proto.Object{soonFile}, soonMeta[:2]...),
		"keep": keepMeta, "shared": {sharedFile}} {
		for _, o := range objects {
			f.metaLabel[string(o.Ref().Hash)] = label
		}
	}

	f.soonObjects = append(append(soonOnly, shared...), soonMeta...)
	f.keepObjects = append(append(append(keepOnly, shared...), keepMeta...), sharedFile)

	all := append(append(append(append(append([]*proto.Object{}, soonOnly...), keepOnly...), shared...), soonMeta...), keepMeta...)
	all = append(all, randomBlobs(40)...)
	mrand.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })

	for _, batch := range batchObjects(all, 8) {
		putAll(t, store, batch)
	}

	return f
}

// options collects as of now, the soon commit dying three days on.
func (f *deathFixture) options(t *testing.T, now time.Time) CollectOptions {
	opts := gcOptions(t, 0)
	opts.Now = now
	opts.Death = func(root []byte) time.Time {
		if bytes.Equal(root, f.soon.Ref().Hash) {
			return now.Add(3 * 24 * time.Hour)
		}

		return time.Time{}
	}

	return opts
}

// requireApart fails unless every archive the store holds that before
// does not name keeps the soon-dying blobs, and with meta the other
// objects too, from those that die later, and some of them hold each.
func (f *deathFixture) requireApart(t *testing.T, before map[string]bool, meta bool) {
	t.Helper()

	f.store.mtx.RLock()
	defer f.store.mtx.RUnlock()

	seen := make(map[string]bool)

	for _, a := range f.store.archives {
		if before[a.name] {
			continue
		}

		idx, err := a.getIndex()
		require.NoError(t, err)

		held := make(map[string]bool)
		for _, rec := range idx {
			label := f.label[string(rec.Sum[:])]
			if meta && label == "" {
				label = f.metaLabel[string(rec.Sum[:])]
			}

			if label != "" {
				held[label], seen[label] = true, true
			}
		}

		require.False(t, held["soon"] && held["keep"], "archive %s holds blobs of both", a.name)
		require.False(t, held["soon"] && held["shared"], "archive %s holds what the later commit keeps with what dies soon", a.name)
	}

	require.True(t, seen["soon"] && seen["keep"] && seen["shared"], "the rewrite wrote each kind of blob: %v", seen)
}

func TestASweepWritesWhatDiesSoonApartFromWhatLives(t *testing.T) {
	f := newDeathFixture(t, WithMaxSize(256<<10), WithCompaction(CompactionConfig{Small: 1}))
	ctx := context.Background()

	now := time.Now()
	_, err := f.store.Collect(ctx, f.options(t, now))
	require.NoError(t, err)

	before := archivesOf(f.store)

	report, err := f.store.Collect(ctx, f.options(t, now.Add(48*time.Hour)))
	require.NoError(t, err)
	require.Greater(t, report.Swept, 0)

	f.requireApart(t, before, true)
	requireStored(t, f.store, f.soonObjects, true)
	requireStored(t, f.store, f.keepObjects, true)
}

func TestCompactionWritesWhatTheLastMarkExpectsToDieSoonApart(t *testing.T) {
	f := newDeathFixture(t, WithMaxSize(4<<20),
		WithCompaction(CompactionConfig{Small: 64 << 10, MinimumCandidates: 1, Workers: 4}))
	ctx := context.Background()

	_, err := f.store.Collect(ctx, f.options(t, time.Now()))
	require.NoError(t, err)

	before := archivesOf(f.store)

	report := compact(t, ctx, f.store)
	require.Greater(t, report.Rewritten, 0)

	f.requireApart(t, before, false)
	requireStored(t, f.store, f.soonObjects, true)
	requireStored(t, f.store, f.keepObjects, true)
}
