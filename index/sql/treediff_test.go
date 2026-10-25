package sql

import (
	"bytes"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/index/sql/ent"
	"github.com/gobackio/goback/index/sql/ent/file"
	"github.com/gobackio/goback/index/sql/ent/tree"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// rowsOf describes every files, trees and set_refs row, with times as
// hours since the fixture's clock started and refs by name.
func (f *fixture) rowsOf(start time.Time, names map[string]string) []string {
	f.t.Helper()

	hours := func(t *time.Time) string {
		if t == nil {
			return "-"
		}

		return fmt.Sprint(int(t.Sub(start).Hours()))
	}

	name := func(ref []byte) string {
		if n, ok := names[string(ref)]; ok {
			return n
		}

		return fmt.Sprintf("%x", ref[:min(4, len(ref))])
	}

	var out []string

	files, err := f.x.client.File.Query().Order(ent.Asc(file.FieldPath), ent.Asc(file.FieldValidFrom)).All(f.ctx)
	require.NoError(f.t, err)

	for _, r := range files {
		out = append(out, fmt.Sprintf("file %s in %q %s..%s %s size %d type %d link %q", r.Path, r.Dir, hours(&r.ValidFrom), hours(r.ValidUntil),
			name(r.Ref), r.Size, r.Type, r.LinkTarget))
	}

	trees, err := f.x.client.Tree.Query().Order(ent.Asc(tree.FieldPath), ent.Asc(tree.FieldValidFrom)).All(f.ctx)
	require.NoError(f.t, err)

	for _, r := range trees {
		out = append(out, fmt.Sprintf("tree %s in %q %s..%s %s", r.Path, r.Dir, hours(&r.ValidFrom), hours(r.ValidUntil), name(r.Ref)))
	}

	refs, err := f.x.client.SetRef.Query().All(f.ctx)
	require.NoError(f.t, err)

	var named []string
	for _, r := range refs {
		named = append(named, "ref "+name(r.Ref))
	}
	sort.Strings(named)

	return append(out, named...)
}

func TestIndexingRecordsTheVersionsOfEveryKindOfChange(t *testing.T) {
	f := newFixture(t)
	start := f.clock
	names := map[string]string{}

	named := func(name string, obj *proto.Object) *proto.Object {
		names[string(obj.Ref().Hash)] = name
		return obj
	}

	file := func(name, content string) *proto.TreeNode {
		node := f.file(name, content)
		names[string(node.Ref.Hash)] = "f:" + content

		return node
	}

	dir := func(name string, nodes ...*proto.TreeNode) *proto.TreeNode {
		node := f.dir(name, nodes...)
		names[string(node.Ref.Hash)] = "d:" + name

		return node
	}

	big := func(name string, parts ...string) *proto.TreeNode {
		var splits []*proto.Ref
		var lengths []uint64
		for _, part := range parts {
			lengths = append(lengths, uint64(len(part)))
			sub := named("sub:"+part, proto.NewObject(&proto.File{Inline: []byte(part)}))
			if part != "lost" {
				require.NoError(t, f.store.Put(f.ctx, sub))
			}

			splits = append(splits, sub.Ref())
		}

		obj := named("big:"+name, proto.NewObject(&proto.File{Splits: splits, SplitLengths: lengths, SplitDepth: 2}))
		require.NoError(t, f.store.Put(f.ctx, obj))

		return &proto.TreeNode{
			Stat: &proto.FileInfo{Name: []byte(name), Type: proto.NodeType_NODE_FILE, MtimeNs: f.epoch.UnixNano(), Size: backup.SplitFileSize, Mode: 0644},
			Ref:  obj.Ref(),
		}
	}

	splitDir := func(name string, halves ...[]*proto.TreeNode) *proto.TreeNode {
		var splits []*proto.Ref
		for i, half := range halves {
			splits = append(splits, named(fmt.Sprintf("half:%s%d", name, i), f.tree(half...)).Ref())
		}

		obj := named("split:"+name, proto.NewObject(&proto.Tree{Splits: splits}))
		require.NoError(t, f.store.Put(f.ctx, obj))

		return &proto.TreeNode{Stat: &proto.FileInfo{Name: []byte(name), Type: proto.NodeType_NODE_DIRECTORY, Mode: 0755}, Ref: obj.Ref()}
	}

	missingDir := func(name string) *proto.TreeNode {
		obj := named("missing:"+name, proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{f.file("never", "never")}}))
		return &proto.TreeNode{Stat: &proto.FileInfo{Name: []byte(name), Type: proto.NodeType_NODE_DIRECTORY, Mode: 0755}, Ref: obj.Ref()}
	}

	var states []string
	record := func() {
		states = append(states, "--")
		states = append(states, f.rowsOf(start, names)...)
	}

	f.commit("world", f.tree(
		file("a.txt", "a1"),
		big("big.bin", "p1", "p2"),
		dir("d1", file("b.txt", "b1"), dir("d2", file("c.txt", "c1"))),
		dir("d3", file("e.txt", "e1"), dir("d5", file("g.txt", "g1"))),
		splitDir("s", []*proto.TreeNode{file("h.txt", "h1")}, []*proto.TreeNode{file("i.txt", "i1")}),
		f.symlink("l", "a.txt"),
	), false)
	record()

	f.advance(time.Hour)
	f.commit("world", f.tree(
		file("a.txt", "a2"),
		big("big.bin", "p1", "p2"),
		dir("d1", file("b.txt", "b1"), dir("d2", file("c.txt", "c2"))),
		dir("d4", file("f.txt", "f1")),
		splitDir("s", []*proto.TreeNode{file("h.txt", "h1")}, []*proto.TreeNode{file("i.txt", "i2")}),
		f.symlink("l", "big.bin"),
		file("x", "x1"),
	), false)
	record()

	f.advance(time.Hour)
	f.commit("world", f.tree(
		dir("a.txt", file("inner", "n1")),
		file("d1", "now a file"),
		dir("d4", file("f.txt", "f1")),
		file("x", "x1"),
	), false)
	record()

	f.advance(time.Hour)
	root := f.tree(
		dir("a.txt", file("inner", "n1")),
		missingDir("d4"),
		big("big.bin", "p3", "lost"),
		file("x", "x2"),
	)
	commit := proto.NewObject(&proto.Commit{Timestamp: f.clock.Unix(), Tree: root.Ref(), BackupSet: "world", AgentId: "node-1", ReceivedAtNs: f.clock.UnixNano()})
	require.NoError(t, f.store.Put(f.ctx, commit))

	placed, err := f.x.indexCommit(f.ctx, commit.GetCommit(), commit.Ref(), false, true, nil)
	require.NoError(t, err)
	require.Equal(t, inOrder, placed)
	require.True(t, f.commitRow(commit.Ref()).Incomplete)
	record()

	require.Equal(t, treeDiffGolden, states)
}

// treeDiffGolden is what each commit of the test leaves in the tables.
var treeDiffGolden = []string{
	"--",
	"file a.txt in \"\" 0..- f:a1 size 2 type 0 link \"\"",
	"file big.bin in \"\" 0..- big:big.bin size 67108864 type 0 link \"\"",
	"file d1/b.txt in \"d1\" 0..- f:b1 size 2 type 0 link \"\"",
	"file d1/d2/c.txt in \"d1/d2\" 0..- f:c1 size 2 type 0 link \"\"",
	"file d3/d5/g.txt in \"d3/d5\" 0..- f:g1 size 2 type 0 link \"\"",
	"file d3/e.txt in \"d3\" 0..- f:e1 size 2 type 0 link \"\"",
	"file l in \"\" 0..-  size 0 type 2 link \"a.txt\"",
	"file s/h.txt in \"s\" 0..- f:h1 size 2 type 0 link \"\"",
	"file s/i.txt in \"s\" 0..- f:i1 size 2 type 0 link \"\"",
	"tree d1 in \"\" 0..- d:d1",
	"tree d1/d2 in \"d1\" 0..- d:d2",
	"tree d3 in \"\" 0..- d:d3",
	"tree d3/d5 in \"d3\" 0..- d:d5",
	"tree s in \"\" 0..- split:s",
	"ref 0ce5e26b",
	"ref big:big.bin",
	"ref d:d1",
	"ref d:d2",
	"ref d:d3",
	"ref d:d5",
	"ref e7cb67d2",
	"ref f:a1",
	"ref f:b1",
	"ref f:c1",
	"ref f:e1",
	"ref f:g1",
	"ref f:h1",
	"ref f:i1",
	"ref half:s0",
	"ref half:s1",
	"ref split:s",
	"ref sub:p1",
	"ref sub:p2",
	"--",
	"file a.txt in \"\" 0..1 f:a1 size 2 type 0 link \"\"",
	"file a.txt in \"\" 1..- f:a2 size 2 type 0 link \"\"",
	"file big.bin in \"\" 0..- big:big.bin size 67108864 type 0 link \"\"",
	"file d1/b.txt in \"d1\" 0..- f:b1 size 2 type 0 link \"\"",
	"file d1/d2/c.txt in \"d1/d2\" 0..1 f:c1 size 2 type 0 link \"\"",
	"file d1/d2/c.txt in \"d1/d2\" 1..- f:c2 size 2 type 0 link \"\"",
	"file d3/d5/g.txt in \"d3/d5\" 0..1 f:g1 size 2 type 0 link \"\"",
	"file d3/e.txt in \"d3\" 0..1 f:e1 size 2 type 0 link \"\"",
	"file d4/f.txt in \"d4\" 1..- f:f1 size 2 type 0 link \"\"",
	"file l in \"\" 0..1  size 0 type 2 link \"a.txt\"",
	"file l in \"\" 1..-  size 0 type 2 link \"big.bin\"",
	"file s/h.txt in \"s\" 0..- f:h1 size 2 type 0 link \"\"",
	"file s/i.txt in \"s\" 0..1 f:i1 size 2 type 0 link \"\"",
	"file s/i.txt in \"s\" 1..- f:i2 size 2 type 0 link \"\"",
	"file x in \"\" 1..- f:x1 size 2 type 0 link \"\"",
	"tree d1 in \"\" 0..1 d:d1",
	"tree d1 in \"\" 1..- d:d1",
	"tree d1/d2 in \"d1\" 0..1 d:d2",
	"tree d1/d2 in \"d1\" 1..- d:d2",
	"tree d3 in \"\" 0..1 d:d3",
	"tree d3/d5 in \"d3\" 0..1 d:d5",
	"tree d4 in \"\" 1..- d:d4",
	"tree s in \"\" 0..1 split:s",
	"tree s in \"\" 1..- split:s",
	"ref 0ce5e26b",
	"ref 2aa759c4",
	"ref bfc9f285",
	"ref big:big.bin",
	"ref d:d1",
	"ref d:d1",
	"ref d:d2",
	"ref d:d2",
	"ref d:d3",
	"ref d:d4",
	"ref d:d5",
	"ref e7cb67d2",
	"ref f:a1",
	"ref f:a2",
	"ref f:b1",
	"ref f:c1",
	"ref f:c2",
	"ref f:e1",
	"ref f:f1",
	"ref f:g1",
	"ref f:h1",
	"ref f:i1",
	"ref f:i2",
	"ref f:x1",
	"ref half:s0",
	"ref half:s1",
	"ref half:s1",
	"ref split:s",
	"ref split:s",
	"ref sub:p1",
	"ref sub:p2",
	"--",
	"file a.txt in \"\" 0..1 f:a1 size 2 type 0 link \"\"",
	"file a.txt in \"\" 1..- f:a2 size 2 type 0 link \"\"",
	"file a.txt/inner in \"a.txt\" 2..- f:n1 size 2 type 0 link \"\"",
	"file big.bin in \"\" 0..2 big:big.bin size 67108864 type 0 link \"\"",
	"file d1 in \"\" 2..- f:now a file size 10 type 0 link \"\"",
	"file d1/b.txt in \"d1\" 0..- f:b1 size 2 type 0 link \"\"",
	"file d1/d2/c.txt in \"d1/d2\" 0..1 f:c1 size 2 type 0 link \"\"",
	"file d1/d2/c.txt in \"d1/d2\" 1..- f:c2 size 2 type 0 link \"\"",
	"file d3/d5/g.txt in \"d3/d5\" 0..1 f:g1 size 2 type 0 link \"\"",
	"file d3/e.txt in \"d3\" 0..1 f:e1 size 2 type 0 link \"\"",
	"file d4/f.txt in \"d4\" 1..- f:f1 size 2 type 0 link \"\"",
	"file l in \"\" 0..1  size 0 type 2 link \"a.txt\"",
	"file l in \"\" 1..2  size 0 type 2 link \"big.bin\"",
	"file s/h.txt in \"s\" 0..2 f:h1 size 2 type 0 link \"\"",
	"file s/i.txt in \"s\" 0..1 f:i1 size 2 type 0 link \"\"",
	"file s/i.txt in \"s\" 1..2 f:i2 size 2 type 0 link \"\"",
	"file x in \"\" 1..- f:x1 size 2 type 0 link \"\"",
	"tree a.txt in \"\" 2..- d:a.txt",
	"tree d1 in \"\" 0..1 d:d1",
	"tree d1 in \"\" 1..- d:d1",
	"tree d1/d2 in \"d1\" 0..1 d:d2",
	"tree d1/d2 in \"d1\" 1..- d:d2",
	"tree d3 in \"\" 0..1 d:d3",
	"tree d3/d5 in \"d3\" 0..1 d:d5",
	"tree d4 in \"\" 1..- d:d4",
	"tree s in \"\" 0..1 split:s",
	"tree s in \"\" 1..2 split:s",
	"ref 0ce5e26b",
	"ref 2aa759c4",
	"ref 4eafc97e",
	"ref b551c8c4",
	"ref bfc9f285",
	"ref big:big.bin",
	"ref d:a.txt",
	"ref d:d1",
	"ref d:d1",
	"ref d:d2",
	"ref d:d2",
	"ref d:d3",
	"ref d:d4",
	"ref d:d5",
	"ref e7cb67d2",
	"ref f:a1",
	"ref f:a2",
	"ref f:b1",
	"ref f:c1",
	"ref f:c2",
	"ref f:e1",
	"ref f:f1",
	"ref f:g1",
	"ref f:h1",
	"ref f:i1",
	"ref f:i2",
	"ref f:n1",
	"ref f:now a file",
	"ref f:x1",
	"ref half:s0",
	"ref half:s1",
	"ref half:s1",
	"ref split:s",
	"ref split:s",
	"ref sub:p1",
	"ref sub:p2",
	"--",
	"file a.txt in \"\" 0..1 f:a1 size 2 type 0 link \"\"",
	"file a.txt in \"\" 1..- f:a2 size 2 type 0 link \"\"",
	"file a.txt/inner in \"a.txt\" 2..- f:n1 size 2 type 0 link \"\"",
	"file big.bin in \"\" 0..2 big:big.bin size 67108864 type 0 link \"\"",
	"file big.bin in \"\" 3..- big:big.bin size 67108864 type 0 link \"\"",
	"file d1 in \"\" 2..3 f:now a file size 10 type 0 link \"\"",
	"file d1/b.txt in \"d1\" 0..3 f:b1 size 2 type 0 link \"\"",
	"file d1/d2/c.txt in \"d1/d2\" 0..1 f:c1 size 2 type 0 link \"\"",
	"file d1/d2/c.txt in \"d1/d2\" 1..3 f:c2 size 2 type 0 link \"\"",
	"file d3/d5/g.txt in \"d3/d5\" 0..1 f:g1 size 2 type 0 link \"\"",
	"file d3/e.txt in \"d3\" 0..1 f:e1 size 2 type 0 link \"\"",
	"file d4/f.txt in \"d4\" 1..3 f:f1 size 2 type 0 link \"\"",
	"file l in \"\" 0..1  size 0 type 2 link \"a.txt\"",
	"file l in \"\" 1..2  size 0 type 2 link \"big.bin\"",
	"file s/h.txt in \"s\" 0..2 f:h1 size 2 type 0 link \"\"",
	"file s/i.txt in \"s\" 0..1 f:i1 size 2 type 0 link \"\"",
	"file s/i.txt in \"s\" 1..2 f:i2 size 2 type 0 link \"\"",
	"file x in \"\" 1..3 f:x1 size 2 type 0 link \"\"",
	"file x in \"\" 3..- f:x2 size 2 type 0 link \"\"",
	"tree a.txt in \"\" 2..- d:a.txt",
	"tree d1 in \"\" 0..1 d:d1",
	"tree d1 in \"\" 1..3 d:d1",
	"tree d1/d2 in \"d1\" 0..1 d:d2",
	"tree d1/d2 in \"d1\" 1..3 d:d2",
	"tree d3 in \"\" 0..1 d:d3",
	"tree d3/d5 in \"d3\" 0..1 d:d5",
	"tree d4 in \"\" 1..3 d:d4",
	"tree d4 in \"\" 3..- missing:d4",
	"tree s in \"\" 0..1 split:s",
	"tree s in \"\" 1..2 split:s",
	"ref 0ce5e26b",
	"ref 2aa759c4",
	"ref 4eafc97e",
	"ref 6a408ba6",
	"ref b551c8c4",
	"ref bfc9f285",
	"ref big:big.bin",
	"ref big:big.bin",
	"ref ceca74e5",
	"ref d:a.txt",
	"ref d:d1",
	"ref d:d1",
	"ref d:d2",
	"ref d:d2",
	"ref d:d3",
	"ref d:d4",
	"ref d:d5",
	"ref e7cb67d2",
	"ref f:a1",
	"ref f:a2",
	"ref f:b1",
	"ref f:c1",
	"ref f:c2",
	"ref f:e1",
	"ref f:f1",
	"ref f:g1",
	"ref f:h1",
	"ref f:i1",
	"ref f:i2",
	"ref f:n1",
	"ref f:now a file",
	"ref f:x1",
	"ref f:x2",
	"ref half:s0",
	"ref half:s1",
	"ref half:s1",
	"ref split:s",
	"ref split:s",
	"ref sub:lost",
	"ref sub:p1",
	"ref sub:p2",
	"ref sub:p3",
}

// forge stores obj under ref, as a store that does not check what it is
// handed would.
func (f *fixture) forge(ref *proto.Ref, obj *proto.Object) {
	f.store.mu.Lock()
	f.store.objects[string(ref.Hash)] = obj
	f.store.mu.Unlock()
}

// indexWithin indexes a commit of root and fails the test unless that
// ends within a few seconds.
func (f *fixture) indexWithin(root *proto.Ref) error {
	f.t.Helper()

	obj := proto.NewObject(&proto.Commit{Timestamp: f.clock.Unix(), Tree: root, BackupSet: "world", AgentId: "node-1"})
	done := make(chan error, 1)

	go func() { done <- f.x.Put(f.ctx, obj) }()

	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		f.t.Fatal("indexing the commit does not end")
		return nil
	}
}

func TestIndexingRefusesATreeThatHoldsItself(t *testing.T) {
	f := newFixture(t)
	forged := &proto.Ref{Hash: bytes.Repeat([]byte{1}, proto.HashSize)}

	loop := f.tree(f.file("a.txt", "one"), &proto.TreeNode{
		Stat: &proto.FileInfo{Name: []byte("loop"), Type: proto.NodeType_NODE_DIRECTORY, Mode: 0755},
		Ref:  forged,
	})
	f.forge(forged, loop)

	require.ErrorIs(t, f.indexWithin(loop.Ref()), backup.ErrDanglingRef)

	split := &proto.Ref{Hash: bytes.Repeat([]byte{2}, proto.HashSize)}
	f.forge(split, proto.NewObject(&proto.Tree{Splits: []*proto.Ref{split}}))

	root := f.tree(&proto.TreeNode{
		Stat: &proto.FileInfo{Name: []byte("d"), Type: proto.NodeType_NODE_DIRECTORY, Mode: 0755},
		Ref:  split,
	})

	require.ErrorIs(t, f.indexWithin(root.Ref()), backup.ErrDanglingRef)
}

func TestIndexingRefusesACommitOverThePathCap(t *testing.T) {
	f := newFixture(t)

	// each level names the one below twice, doubling the paths
	level := f.dir("d", f.file("a.txt", "one"))
	for range 6 {
		level = f.dir("d", &proto.TreeNode{Stat: &proto.FileInfo{Name: []byte("l"), Type: proto.NodeType_NODE_DIRECTORY, Mode: 0755}, Ref: level.Ref},
			&proto.TreeNode{Stat: &proto.FileInfo{Name: []byte("r"), Type: proto.NodeType_NODE_DIRECTORY, Mode: 0755}, Ref: level.Ref})
	}

	root := f.tree(level)

	f.x.MaxPaths = 100
	require.ErrorIs(t, f.indexWithin(root.Ref()), proto.ErrInvalidObject)

	commits, err := f.x.CommitInfo(f.ctx, "world", f.clock, 10)
	require.NoError(t, err)
	require.Empty(t, commits)

	f.x.MaxPaths = 1000
	require.NoError(t, f.indexWithin(root.Ref()))
}

func TestIndexingRecordsEveryPathOfASubtreeSharedByManyDirectories(t *testing.T) {
	f := newFixture(t)
	shared := f.dir("x", f.file("a.txt", "one"))

	var nodes []*proto.TreeNode
	for _, name := range []string{"p", "q"} {
		nodes = append(nodes, &proto.TreeNode{Stat: &proto.FileInfo{Name: []byte(name), Type: proto.NodeType_NODE_DIRECTORY, Mode: 0755}, Ref: shared.Ref})
	}

	require.NoError(t, f.indexWithin(f.tree(nodes...).Ref()))

	for _, dir := range []string{"p", "q"} {
		entries, err := f.x.ReadDir(f.ctx, "world", dir, f.clock)
		require.NoError(t, err)
		require.Len(t, entries, 1, dir)
	}
}
