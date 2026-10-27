package pack

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// simLease is how long a simulated session lives without writing.
const simLease = time.Hour

// simulation runs writers, collections, retirements, compactions, lease
// expiries and process crashes in an order a seed decides, against one
// bucket and one index, and checks after every collection that each
// backup that committed and was not retired still restores.
type simulation struct {
	t      *testing.T
	rng    *rand.Rand
	bucket *memBucket
	index  *InMemoryIndex

	servers    []*PackStorage
	maintainer *PackStorage
	// handoff has collections publish their sweeps for a rewriter process
	handoff bool
	// markers, when set, keeps the markers of every other writer's
	// sessions outside the bucket
	markers *memMarkers

	writers []*simWriter
	pool    []*proto.Object
	commits int

	live    [][]*proto.Object
	retired int

	reclaimed uint64
	lost      int
}

// simWriter is one agent backing up through a server.
type simWriter struct {
	server  int
	ctx     context.Context
	pending []*proto.Object
	commit  *proto.Object
	closure []*proto.Object
}

func newSimulation(t *testing.T, seed int64, handoff, routed bool) *simulation {
	sim := &simulation{
		t:       t,
		handoff: handoff,
		rng:     rand.New(rand.NewSource(seed)),
		bucket:  newMemBucket(),
		index:   NewInMemoryIndex(),
	}

	if routed {
		sim.markers = newMemMarkers()
	}

	sim.servers = []*PackStorage{sim.open(), sim.open()}
	sim.maintainer = sim.open()
	sim.writers = make([]*simWriter, 3)

	return sim
}

func (sim *simulation) open() *PackStorage {
	options := []PackOption{
		WithArchiveStorage(sim.bucket.view()),
		WithMaxParallel(16),
		WithArchiveIndex(sim.index),
		WithMaxSize(64 * 1024),
		WithSessionLease(simLease),
	}

	if sim.markers != nil {
		options = append(options, WithSessionMarkers(sim.markers, markedInBucket))
	}

	store, err := NewPackStorage(options...)
	require.NoError(sim.t, err)
	require.NoError(sim.t, store.Open())

	return store
}

// blob draws content for a backup: mostly what earlier backups stored, so
// writers deduplicate against objects a collection may be condemning.
func (sim *simulation) blob() *proto.Object {
	if len(sim.pool) > 0 && sim.rng.Intn(3) > 0 {
		return sim.pool[sim.rng.Intn(len(sim.pool))]
	}

	data := make([]byte, 512+sim.rng.Intn(4096))
	sim.rng.Read(data)

	obj := proto.NewObject(&proto.Blob{Data: data})
	sim.pool = append(sim.pool, obj)

	return obj
}

// backup plans a backup: its objects bottom up, then its commit.
func (sim *simulation) backup() ([]*proto.Object, *proto.Object) {
	blobs := make([]*proto.Object, 1+sim.rng.Intn(12))
	for i := range blobs {
		blobs[i] = sim.blob()
	}

	files := makeGCFiles(blobs)
	trees := makeGCTrees(files)
	root := treeOf(trees)

	sim.commits++
	commit := proto.NewObject(&proto.Commit{Tree: root.Ref(), Timestamp: int64(sim.commits), BackupSet: "world"})

	var objects []*proto.Object
	objects = append(objects, blobs...)
	objects = append(objects, files...)
	objects = append(objects, trees...)
	objects = append(objects, root)

	return objects, commit
}

// step advances one writer: it begins a backup, uploads the next few
// objects the store does not have, commits or aborts.
func (sim *simulation) step(i int) {
	w := sim.writers[i]
	if w == nil {
		server := sim.rng.Intn(len(sim.servers))

		ctx, err := sim.servers[server].BeginSession(context.WithValue(context.Background(), inBucketKey{}, i%2 == 0), &backup.Session{AgentID: fmt.Sprintf("agent-%d", i), Set: "world"})
		require.NoError(sim.t, err)

		objects, commit := sim.backup()
		sim.writers[i] = &simWriter{server: server, ctx: ctx, pending: objects, commit: commit,
			closure: append(append([]*proto.Object(nil), objects...), commit)}

		return
	}

	store := sim.servers[w.server]

	if sim.rng.Intn(20) == 0 {
		sim.writers[i] = nil
		sim.endable(store.EndSession(w.ctx))

		return
	}

	if len(w.pending) == 0 {
		sim.writers[i] = nil

		if sim.endable(store.Put(w.ctx, w.commit)) {
			sim.live = append(sim.live, w.closure)
		}

		return
	}

	for n := 1 + sim.rng.Intn(4); n > 0 && len(w.pending) > 0; n-- {
		obj := w.pending[0]
		w.pending = w.pending[1:]

		has, err := store.Has(w.ctx, obj.Ref())
		if !sim.endable(err) {
			sim.writers[i] = nil
			return
		}

		if has {
			continue
		}

		if !sim.endable(store.Put(w.ctx, obj)) {
			sim.writers[i] = nil
			return
		}
	}

	if sim.rng.Intn(4) == 0 {
		sim.flushed(store.Flush())
	}
}

// endable reports whether err is nil, and accepts the end of a session
// a lease expiry or a collection took, and a commit missing what a
// collection took, as an outcome a writer may meet.
func (sim *simulation) endable(err error) bool {
	if errors.Is(err, backup.ErrNoSession) || errors.Is(err, backup.ErrSessionLost) || errors.Is(err, backup.ErrDanglingRef) {
		sim.lost++
		return false
	}

	require.NoError(sim.t, err)

	return true
}

// flushed accepts a lapsed claim, and the end of a session another process
// ended, from flushing or closing a process.
func (sim *simulation) flushed(err error) {
	if !errors.Is(err, ErrClaimLapsed) && !errors.Is(err, backup.ErrNoSession) {
		require.NoError(sim.t, err)
	}
}

// writersStep advances a few random writers.
func (sim *simulation) writersStep() {
	for n := sim.rng.Intn(4); n > 0; n-- {
		sim.step(sim.rng.Intn(len(sim.writers)))
	}
}

// crash drops a server without ending its sessions, as a process dies;
// its writers are gone with it and their leases run out.
func (sim *simulation) crash(server int) {
	sim.flushed(sim.servers[server].Close())
	sim.servers[server] = sim.open()

	for i, w := range sim.writers {
		if w != nil && w.server == server {
			sim.writers[i] = nil
		}
	}
}

func (sim *simulation) collect() {
	gcAfterBatch = func(int) error {
		sim.writersStep()
		return nil
	}
	defer func() { gcAfterBatch = nil }()

	report, err := sim.maintainer.Collect(context.Background(), CollectOptions{
		Now:       time.Now(),
		MinAge:    time.Nanosecond,
		DeadRatio: 1e-9,
		TempDir:   sim.t.TempDir(),
		Handoff:   sim.handoff,
	})
	require.NoError(sim.t, err)

	sim.reclaimed += report.ReclaimedObjects
	sim.check()
}

// rewrite runs the published plans in a process of its own, as a
// maintenance job elsewhere does.
func (sim *simulation) rewrite() {
	rewriter := sim.open()
	defer func() { require.NoError(sim.t, rewriter.Close()) }()

	report, err := rewriter.RewritePlan(context.Background())
	require.NoError(sim.t, err)

	sim.reclaimed += report.ReclaimedObjects
	sim.check()
}

// retire tombstones a committed backup, which nothing then needs to keep.
func (sim *simulation) retire() {
	if len(sim.live) == 0 {
		return
	}

	i := sim.rng.Intn(len(sim.live))
	commit := sim.live[i][len(sim.live[i])-1]
	require.NoError(sim.t, sim.maintainer.Delete(context.Background(), commit.Ref()))
	require.NoError(sim.t, sim.maintainer.Flush())

	sim.live = slices.Delete(sim.live, i, i+1)
	sim.retired++
}

// check restores every live backup through a process opened afresh.
func (sim *simulation) check() {
	reader := sim.open()
	defer func() { require.NoError(sim.t, reader.Close()) }()

	ctx := context.Background()
	for _, closure := range sim.live {
		for _, obj := range closure {
			_, err := reader.Get(ctx, obj.Ref())
			require.NoErrorf(sim.t, err, "%s %x of commit %x", obj.Type(), obj.Ref().Hash, closure[len(closure)-1].Ref().Hash)
		}
	}
}

func (sim *simulation) run(steps int) {
	for range steps {
		switch roll := sim.rng.Intn(100); {
		case roll < 60 && sim.handoff && roll >= 54:
			sim.rewrite()
		case roll < 60:
			sim.writersStep()
		case roll < 75:
			sim.collect()
		case roll < 85:
			sim.retire()
		case roll < 90:
			compact(sim.t, context.Background(), sim.maintainer)
		case roll < 93:
			sim.maintainer.Sweep(time.Now().Add(simLease))
		case roll < 97:
			sim.crash(sim.rng.Intn(len(sim.servers)))
		default:
			require.NoError(sim.t, sim.maintainer.Close())
			sim.maintainer = sim.open()
		}
	}

	for range 4 {
		sim.collect()

		if sim.handoff {
			sim.rewrite()
		}
	}

	for _, server := range sim.servers {
		sim.flushed(server.Close())
	}
	require.NoError(sim.t, sim.maintainer.Close())
}

func TestSimulatedStoresKeepEveryCommittedBackup(t *testing.T) {
	seeds, steps := 8, 150
	if testing.Short() {
		seeds, steps = 2, 60
	}

	if n, err := strconv.Atoi(os.Getenv("GOBACK_SIM_SEEDS")); err == nil {
		seeds = n
	}

	var reclaimed uint64
	var live, retired, lost int

	for _, mode := range []struct{ handoff, routed bool }{{false, false}, {true, false}, {false, true}} {
		for seed := range int64(seeds) {
			t.Run(fmt.Sprintf("handoff-%v/routed-%v/seed-%d", mode.handoff, mode.routed, seed), func(t *testing.T) {
				sim := newSimulation(t, seed, mode.handoff, mode.routed)
				sim.run(steps)

				reclaimed += sim.reclaimed
				live += len(sim.live)
				retired += sim.retired
				lost += sim.lost
			})
		}
	}

	t.Logf("%d backups live, %d retired, %d sessions lost, %d objects reclaimed", live, retired, lost, reclaimed)
	require.NotZero(t, reclaimed, "the simulation must drop something to test anything")
}
