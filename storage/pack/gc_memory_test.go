package pack

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sync"
	"testing"
	"time"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// gcMemoryStore fills a store as a desktop's backups do: files of one small
// part each under trees of 50 nodes, every commit sharing the trees of the
// one before but one.
func gcMemoryStore(b *testing.B, files int) (*PackStorage, int) {
	b.Helper()

	store := newGCStore(&testing.T{}, b.TempDir())
	ctx := context.Background()
	objects := 0

	put := func(obj *proto.Object) *proto.Object {
		require.NoError(b, store.Put(ctx, obj))
		objects++

		return obj
	}

	file := func(i, version int) *proto.Object {
		blob := put(proto.NewObject(&proto.Blob{Data: []byte(fmt.Sprintf("file %d version %d", i, version))}))

		return put(proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Ref: blob.Ref(), Length: 32}}}))
	}

	b.Cleanup(func() { _ = store.Close() })

	tree := func(children []*proto.Object) *proto.Object {
		nodes := make([]*proto.TreeNode, len(children))
		for i, child := range children {
			nodes[i] = &proto.TreeNode{Stat: &proto.FileInfo{Name: []byte(fmt.Sprintf("%07d", i)), Mode: 0o644, Size: 1}, Ref: child.Ref()}
		}

		return proto.NewObject(&proto.Tree{Nodes: nodes})
	}

	dirs := make([]*proto.Object, 0, files/50)
	for d := 0; d < files/50; d++ {
		children := make([]*proto.Object, 50)
		for i := range children {
			children[i] = file(d*50+i, 0)
		}

		dirs = append(dirs, put(tree(children)))
	}

	for c := 0; c < 20; c++ {
		changed := make([]*proto.Object, 50)
		for i := range changed {
			changed[i] = file(c*50+i, c+1)
		}

		dirs[c] = put(tree(changed))
		root := put(tree(dirs))
		put(proto.NewObject(&proto.Commit{Tree: root.Ref(), Timestamp: int64(c + 1), BackupSet: "world"}))
	}

	require.NoError(b, store.Flush())

	return store, objects
}

// peaks samples the live heap and the bytes under dir until stopped.
func peaks(dir string) (stop func() (heap, disk uint64)) {
	sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(sample)
	base := sample[0].Value.Uint64()

	var heap, disk uint64
	done := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		for tick := time.NewTicker(5 * time.Millisecond); ; {
			select {
			case <-done:
				return
			case <-tick.C:
			}

			metrics.Read(sample)
			if v := sample[0].Value.Uint64(); v > base && v-base > heap {
				heap = v - base
			}

			var size uint64
			_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
				if err == nil && !d.IsDir() {
					if info, err := d.Info(); err == nil {
						size += uint64(info.Size())
					}
				}

				return nil
			})
			disk = max(disk, size)
		}
	}()

	return func() (uint64, uint64) {
		close(done)
		wg.Wait()

		return heap, disk
	}
}

// BenchmarkCollectMemory reports what a collection holds in memory and in
// its TempDir per object of the store at its peak.
func BenchmarkCollectMemory(b *testing.B) {
	for _, files := range []int{20_000, 80_000} {
		b.Run(fmt.Sprint(files), func(b *testing.B) {
			store, objects := gcMemoryStore(b, files)
			defer debug.SetGCPercent(debug.SetGCPercent(5))

			for b.Loop() {
				dir := b.TempDir()
				runtime.GC()

				stop := peaks(dir)
				_, err := store.Collect(context.Background(), CollectOptions{TempDir: dir, Now: time.Now().Add(48 * time.Hour), NoSweep: true})
				heap, disk := stop()
				require.NoError(b, err)

				b.ReportMetric(float64(heap)/float64(objects), "heap-B/object")
				b.ReportMetric(float64(disk)/float64(objects), "temp-B/object")
				b.ReportMetric(float64(objects), "objects")
			}
		})
	}
}

func TestACollectionTheMemoryLimitLeavesNoRoomForRefusesAndLetsGo(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })

	reachable, unreachable := makeGCTestData(t)
	putAll(t, store, append(reachable, unreachable...))

	limit := debug.SetMemoryLimit(1)
	_, err := store.Collect(context.Background(), gcOptions(t, 0))
	debug.SetMemoryLimit(limit)
	require.ErrorIs(t, err, ErrMemoryLimit)

	report, err := store.Collect(context.Background(), gcOptions(t, 0))
	require.NoError(t, err)
	require.False(t, report.Busy, "the refused collection let the maintenance go")
	require.EqualValues(t, len(reachable), report.Marked)
}
