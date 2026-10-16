package backup

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// benchTree writes count small files into store and returns the tree
// holding them, shaped like a backup of a directory of little files.
func benchTree(b *testing.B, store ObjectStore, count int) *proto.Tree {
	b.Helper()
	ctx := context.Background()

	tree := &proto.Tree{}
	for i := range count {
		data := randomData(2048, int64(i))

		blob := proto.NewObject(&proto.Blob{Data: data})
		require.NoError(b, store.Put(ctx, blob))

		file := proto.NewObject(&proto.File{Parts: []*proto.FilePart{
			{Length: uint64(len(data)), Ref: blob.Ref()},
		}})
		require.NoError(b, store.Put(ctx, file))

		stat := statFor(data)
		stat.Name = []byte("file" + strconv.Itoa(i) + ".dat")

		tree.Nodes = append(tree.Nodes, &proto.TreeNode{Stat: stat, Ref: file.Ref()})
	}

	return tree
}

// BenchmarkRestoreSmallFiles restores a directory of small files the way
// the CLI does, one after another, which is what a restore of many of
// them costs.
func BenchmarkRestoreSmallFiles(b *testing.B) {
	const count = 2000

	store := newMemStore()
	tree := benchTree(b, store, count)
	restorer := &Restorer{Store: store, Workers: 32}
	ctx := context.Background()

	b.ResetTimer()

	for range b.N {
		b.StopTimer()
		into := b.TempDir()
		b.StartTimer()

		for _, node := range tree.Nodes {
			path := filepath.Join(into, string(node.Stat.Name))

			_, err := restorer.RestoreFile(ctx, path, node.Stat, node.Ref)
			if err != nil {
				b.Fatal(err)
			}
		}

		b.StopTimer()
		_ = os.RemoveAll(into)
		b.StartTimer()
	}

	b.ReportMetric(float64(count), "files/op")
}

func TestWorkersFallBackToTheDefault(t *testing.T) {
	require.Equal(t, max(32, 2*runtime.NumCPU()), DefaultRestoreWorkers(), "twice the cpu count, at least 32")

	require.Equal(t, DefaultRestoreWorkers(), (&Restorer{}).workers(), "nothing set means the default")
	require.Equal(t, DefaultRestoreWorkers(), (&Restorer{Workers: -1}).workers(), "so does a nonsense count")
	require.Equal(t, 3, (&Restorer{Workers: 3}).workers())
}
