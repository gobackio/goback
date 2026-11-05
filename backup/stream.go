package backup

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync/atomic"

	"github.com/gobackio/goback/proto"
)

// Stream is a backup read from a stream, such as a command's output,
// instead of from the disk: either a tar, each entry of which becomes a node
// of the commit's tree, or a single file.
type Stream struct {
	Tar io.Reader
	// Name and File, set instead of Tar, store File's content as the one
	// file Name of the commit's tree.
	Name string
	File io.Reader
	// Inspect, when set, sees every entry's header before it is stored; a
	// writer it returns gets a copy of the entry's content.
	Inspect func(hdr *tar.Header) io.Writer
	// Finish, when set, runs once Tar is read to its end and before the
	// commit is stored; an error fails the run without a commit.
	Finish func() error
}

type streamDir struct {
	stat  *proto.FileInfo
	nodes []*proto.TreeNode
}

// putStream stores the stream's entries, skipping what the base, whose root
// holds baseNodes, has at the same paths, and returns the root's nodes.
func (w *Walker) putStream(ctx context.Context, baseNodes []*proto.TreeNode) ([]*proto.TreeNode, error) {
	var base *streamBase
	if !w.rescan {
		base = &streamBase{w: w, root: baseNodes, dirs: map[string]*baseDir{}}
	}

	if w.Stream.File != nil {
		return w.putStreamFile(ctx, base)
	}

	dirs := map[string]*streamDir{"": {}}

	var dir func(p string) *streamDir
	dir = func(p string) *streamDir {
		if d, ok := dirs[p]; ok {
			return d
		}

		d := &streamDir{}
		dirs[p] = d
		dir(parentDir(p))

		return d
	}

	entries := tar.NewReader(w.Stream.Tar)
	for {
		hdr, err := entries.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("reading the stream: %w", err)
		}

		name := path.Clean(strings.TrimPrefix(hdr.Name, "./"))
		if name == "." {
			continue
		}

		if !isLocalSlash(name) {
			return nil, fmt.Errorf("the stream holds %q, outside its root", hdr.Name)
		}

		var content io.Reader = entries
		if w.Stream.Inspect != nil {
			if copy := w.Stream.Inspect(hdr); copy != nil {
				content = io.TeeReader(entries, copy)
			}
		}

		stat := &proto.FileInfo{
			Name:    []byte(path.Base(name)),
			Mode:    uint32(hdr.FileInfo().Mode()),
			MtimeNs: hdr.ModTime.UnixNano(),
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			stat.Type = proto.NodeType_NODE_DIRECTORY
			dir(name).stat = stat
		case tar.TypeReg:
			stat.Type = proto.NodeType_NODE_FILE
			stat.Size = hdr.Size

			ref, err := w.putStreamContent(ctx, name, hdr.Size, content, base)
			if err != nil {
				return nil, fmt.Errorf("storing %s: %w", name, err)
			}

			atomic.AddInt64(&w.result.Files, 1)
			atomic.AddInt64(&w.result.Read, 1)
			atomic.AddInt64(&w.result.Bytes, hdr.Size)

			parent := dir(parentDir(name))
			parent.nodes = append(parent.nodes, &proto.TreeNode{Stat: stat, Ref: ref})
		case tar.TypeSymlink:
			stat.Type = proto.NodeType_NODE_SYMLINK
			stat.LinkTarget = []byte(hdr.Linkname)

			parent := dir(parentDir(name))
			parent.nodes = append(parent.nodes, &proto.TreeNode{Stat: stat})
		default:
			return nil, fmt.Errorf("the stream holds %s, of a type a backup does not store (%c)", name, hdr.Typeflag)
		}
	}

	// whatever follows the archive's end is read too, so its writer can exit
	if _, err := io.Copy(io.Discard, w.Stream.Tar); err != nil {
		return nil, fmt.Errorf("reading the stream: %w", err)
	}

	if w.Stream.Finish != nil {
		if err := w.Stream.Finish(); err != nil {
			return nil, err
		}
	}

	return w.putStreamDir(ctx, dirs, "", nil)
}

// putStreamFile stores the stream's single file and returns its node.
func (w *Walker) putStreamFile(ctx context.Context, base *streamBase) ([]*proto.TreeNode, error) {
	name := w.Stream.Name
	if name == "" || strings.Contains(name, "/") || name == "." || name == ".." {
		return nil, fmt.Errorf("a stream's file needs a plain name, not %q", name)
	}

	content := &countingReader{r: w.Stream.File}

	ref, err := w.putStreamContent(ctx, name, 0, content, base)
	if err != nil {
		return nil, fmt.Errorf("storing %s: %w", name, err)
	}

	if w.Stream.Finish != nil {
		if err := w.Stream.Finish(); err != nil {
			return nil, err
		}
	}

	atomic.AddInt64(&w.result.Files, 1)
	atomic.AddInt64(&w.result.Read, 1)
	atomic.AddInt64(&w.result.Bytes, content.n)

	stat := &proto.FileInfo{
		Name:    []byte(name),
		Type:    proto.NodeType_NODE_FILE,
		Mode:    0o600,
		Size:    content.n,
		MtimeNs: w.now().UnixNano(),
	}

	return []*proto.TreeNode{{Stat: stat, Ref: ref}}, nil
}

// putStreamContent stores the stream's file at name like putContent, but
// skips the chunks of the base's file at name and, once the store said it
// holds them, the chunks the filters hit.
func (w *Walker) putStreamContent(ctx context.Context, name string, size int64, content io.Reader, base *streamBase) (*proto.Ref, error) {
	if w.lost(name) {
		return w.putContent(ctx, size, content)
	}

	writer := w.contentWriter(ctx, size)
	writer.known = base.known(ctx, name)
	writer.confirmer = w.confirmer
	writer.window = w.window

	if w.checker != nil {
		writer.filters = w.filters
		writer.checker = w.checker
	}

	ref, err := writeContent(writer, content)
	if err != nil {
		return nil, err
	}

	atomic.AddInt64(&w.result.Assumed, int64(len(writer.assumed)))
	atomic.AddInt64(&w.result.Repaired, int64(writer.repaired))

	return ref, nil
}

// streamBase finds the parts of the base's files at a stream's paths. It
// reads a directory's tree and file objects once, when the stream first
// reaches the directory.
type streamBase struct {
	w    *Walker
	root []*proto.TreeNode
	dirs map[string]*baseDir
}

type baseDir struct {
	token []byte
	nodes map[string]*proto.TreeNode
	known map[string]map[string]struct{}
}

// known returns the blob refs of the base's file at name, nil when there
// is none or it cannot be read. A stream holds a path once, so they are
// handed out once.
func (b *streamBase) known(ctx context.Context, name string) map[string]struct{} {
	if b == nil {
		return nil
	}

	d := b.dir(ctx, parentDir(name))
	if d == nil {
		return nil
	}

	file := path.Base(name)
	known := d.known[file]
	delete(d.known, file)

	return known
}

func (b *streamBase) dir(ctx context.Context, p string) *baseDir {
	if d, ok := b.dirs[p]; ok {
		return d
	}

	d := b.load(ctx, p)
	b.dirs[p] = d

	return d
}

func (b *streamBase) load(ctx context.Context, p string) *baseDir {
	nodes, token := b.root, []byte(nil)

	if p != "" {
		parent := b.dir(ctx, parentDir(p))
		if parent == nil {
			return nil
		}

		name := path.Base(p)
		node := parent.nodes[name]
		if node == nil || !node.Stat.IsDir() || node.Ref == nil {
			return nil
		}

		token = NameToken(b.w.Key, parent.token, []byte(name))

		tree, err := b.w.trees.load(ctx, node.Ref, token)
		if err != nil {
			b.w.logger().Warn("base tree unavailable, storing the directory's files in full", "dir", p, "err", err)
			return nil
		}

		nodes = tree.Nodes
	}

	d := &baseDir{token: token, nodes: make(map[string]*proto.TreeNode, len(nodes)), known: map[string]map[string]struct{}{}}

	var (
		names []string
		refs  []*proto.Ref
	)

	for _, node := range nodes {
		name := string(node.Stat.GetName())
		d.nodes[name] = node

		if node.Stat.GetType() == proto.NodeType_NODE_FILE && node.Ref != nil {
			names = append(names, name)
			refs = append(refs, node.Ref)
		}
	}

	for i, obj := range b.w.readFiles(ctx, refs) {
		if obj.GetFile() == nil {
			continue
		}

		parts, err := FileParts(ctx, b.w.Objects, obj.GetFile())
		if err != nil {
			continue
		}

		known := make(map[string]struct{}, len(parts))
		for _, part := range parts {
			if part.Ref != nil {
				known[string(part.Ref.Hash)] = struct{}{}
			}
		}

		d.known[names[i]] = known
	}

	return d
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)

	return n, err
}

// putStreamDir stores the trees below p, whose name token is token, and
// returns p's nodes.
func (w *Walker) putStreamDir(ctx context.Context, dirs map[string]*streamDir, p string, token []byte) ([]*proto.TreeNode, error) {
	nodes := dirs[p].nodes

	for child, d := range dirs {
		if child == "" || parentDir(child) != p {
			continue
		}

		name := path.Base(child)
		childToken := NameToken(w.Key, token, []byte(name))

		children, err := w.putStreamDir(ctx, dirs, child, childToken)
		if err != nil {
			return nil, err
		}

		ref, err := PutTree(ctx, w.Objects, SortNodes(children), w.Key, childToken)
		if err != nil {
			return nil, fmt.Errorf("storing tree for %s: %w", child, err)
		}

		stat := d.stat
		if stat == nil {
			stat = &proto.FileInfo{Name: []byte(name), Type: proto.NodeType_NODE_DIRECTORY, Mode: uint32(os.ModeDir | 0o700)}
		}

		nodes = append(nodes, &proto.TreeNode{Stat: stat, Ref: ref})
	}

	return SortNodes(nodes), nil
}

func parentDir(p string) string {
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		return p[:i]
	}

	return ""
}

func isLocalSlash(p string) bool {
	return !path.IsAbs(p) && p != ".." && !strings.HasPrefix(p, "../")
}
