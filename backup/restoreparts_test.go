package backup

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// refCountingStore counts the gets of every ref.
type refCountingStore struct {
	*memStore

	mtx  sync.Mutex
	gets map[string]int
}

func (s *refCountingStore) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	s.mtx.Lock()
	s.gets[string(ref.Hash)]++
	s.mtx.Unlock()

	return s.memStore.Get(ctx, ref)
}

// partGets is how often each part of the files at names in the commit's
// tree was fetched.
func partGets(t *testing.T, f *walkerFixture, store *refCountingStore, commit *proto.Commit, names ...string) map[string]int {
	t.Helper()

	gets := map[string]int{}

	for _, name := range names {
		tree := commit.GetTree()
		elems := strings.Split(name, "/")

		for _, dir := range elems[:len(elems)-1] {
			tree = f.tree(tree)[dir].GetRef()
		}

		file, err := f.store.Get(context.Background(), f.tree(tree)[elems[len(elems)-1]].GetRef())
		require.NoError(t, err)

		for _, part := range file.GetFile().GetParts() {
			if part.Ref != nil {
				gets[string(part.Ref.Hash)] = store.gets[string(part.Ref.Hash)]
			}
		}
	}

	return gets
}

func TestAPartManyFilesHoldIsFetchedOnce(t *testing.T) {
	f := newWalkerFixture(t)
	big := f.random(6*maxBlobSize + 17)
	shares := append(append([]byte{}, big[:3*maxBlobSize]...), f.random(2*maxBlobSize)...)
	f.write("a/region.mca", big)
	f.write("b/region.mca", big)
	f.write("c/region.mca", shares)

	result, err := f.walker.Run(context.Background())
	require.NoError(t, err)

	store := &refCountingStore{memStore: f.store, gets: map[string]int{}}
	dst := t.TempDir()
	r := &Restorer{Store: store, Workers: 8}
	require.NoError(t, r.RestoreTree(context.Background(), result.Commit, dst, TreeOptions{}))

	for name, want := range map[string][]byte{"a/region.mca": big, "b/region.mca": big, "c/region.mca": shares} {
		got, err := os.ReadFile(filepath.Join(dst, filepath.FromSlash(name)))
		require.NoError(t, err)
		require.Equal(t, want, got, name)
	}

	for hash, n := range partGets(t, f, store, result.Commit, "a/region.mca", "b/region.mca", "c/region.mca") {
		require.Equal(t, 1, n, "part %x", hash)
	}

	stats := r.Stats()
	require.Positive(t, stats.BytesFromRestored)
	require.EqualValues(t, 2*len(big)+len(shares), stats.BytesFromStore+stats.BytesFromRestored)
}

func TestADuplicateIsCopiedFromACopyAlreadyInPlace(t *testing.T) {
	f := newWalkerFixture(t)
	big := f.random(4*maxBlobSize + 5)
	f.write("a.dat", big)
	f.write("b.dat", big)

	result, err := f.walker.Run(context.Background())
	require.NoError(t, err)

	dst := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dst, "a.dat"), big, 0o644))

	store := &refCountingStore{memStore: f.store, gets: map[string]int{}}
	r := &Restorer{Store: store, Workers: 1}

	var outcomes sync.Map
	require.NoError(t, r.RestoreTree(context.Background(), result.Commit, dst, TreeOptions{Done: func(file RestoredFile) error {
		outcomes.Store(filepath.Base(file.Path), file.Outcome)
		return file.Err
	}}))

	a, _ := outcomes.Load("a.dat")
	require.Equal(t, OutcomeUnchanged, a, "a copy that matches at its destination is left alone")

	got, err := os.ReadFile(filepath.Join(dst, "b.dat"))
	require.NoError(t, err)
	require.Equal(t, big, got)

	for hash, n := range partGets(t, f, store, result.Commit, "a.dat", "b.dat") {
		require.Zero(t, n, "part %x", hash)
	}

	require.EqualValues(t, len(big), r.Stats().BytesFromRestored)
}

func TestAGuessFindsAMovedAndARenamedSeed(t *testing.T) {
	f := newWalkerFixture(t)
	moved, renamed := f.random(4*maxBlobSize+5), f.random(2*maxBlobSize+3)
	f.write("world/r.0.0.mca", moved)
	f.write("world/r.1.0.mca", renamed)

	result, err := f.walker.Run(context.Background())
	require.NoError(t, err)

	seed := t.TempDir()
	for rel, content := range map[string][]byte{"old/r.0.0.mca": moved, "world/copy.mca": renamed, "decoy.dat": f.random(maxBlobSize)} {
		full := filepath.Join(seed, filepath.FromSlash(rel))
		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o755))
		require.NoError(t, os.WriteFile(full, content, 0o644))
	}

	seeds := NewSeedMap(nil)
	require.NoError(t, seeds.AddSeed(seed, SeedGuess))
	require.Zero(t, seeds.Len(), "a guess reads nothing before the restore asks")

	store := &refCountingStore{memStore: f.store, gets: map[string]int{}}
	dst := t.TempDir()
	r := &Restorer{Store: store, Seeds: seeds}
	require.NoError(t, r.RestoreTree(context.Background(), result.Commit, dst, TreeOptions{}))

	for name, want := range map[string][]byte{"r.0.0.mca": moved, "r.1.0.mca": renamed} {
		got, err := os.ReadFile(filepath.Join(dst, "world", name))
		require.NoError(t, err)
		require.Equal(t, want, got, name)
	}

	for hash, n := range partGets(t, f, store, result.Commit, "world/r.0.0.mca", "world/r.1.0.mca") {
		require.Zero(t, n, "part %x", hash)
	}

	require.EqualValues(t, len(moved)+len(renamed), r.Stats().BytesFromSeeds)
	require.NotContains(t, seeds.indexed, filepath.Join(seed, "decoy.dat"))
}

func TestSeedCandidatesGoByPathThenNameThenSize(t *testing.T) {
	var l seedListing
	l.add(
		newSeedStat("far", "x/far.mca", 1300),
		newSeedStat("near", "x/near.mca", 1050),
		newSeedStat("exact", "x/exact.mca", 1000),
		newSeedStat("below", "x/below.MCA", 920),
		newSeedStat("other ext", "x/other.dat", 1000),
		newSeedStat("same name", "y/r.0.0.mca", 5),
		newSeedStat("same path", "world/r.0.0.mca", 7),
	)

	paths := func(stats []seedStat) []string {
		var out []string
		for _, s := range stats {
			out = append(out, s.path)
		}

		return out
	}

	require.Equal(t, []string{"same path", "same name", "exact", "near", "below"}, paths(l.candidates("world/r.0.0.mca", "r.0.0.mca", 1000, 10)))
	require.Equal(t, []string{"same path", "same name", "exact"}, paths(l.candidates("world/r.0.0.mca", "r.0.0.mca", 1000, 3)))
	require.Equal(t, []string{"exact", "near", "below"}, paths(l.candidates("new/a.mca", "a.mca", 1000, 10)))
	require.Empty(t, l.candidates("new/a.txt", "a.txt", 1000, 10))
}
