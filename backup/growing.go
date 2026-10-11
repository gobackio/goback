package backup

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/gobackio/goback/proto"
)

// Growing is a file a walk includes up to a length, such as a log a
// receiver is still appending to. Its writer guarantees that the first Size
// bytes only ever grow: a walk keeps the parts of the previous version it
// continues, except the last, and reads only from the start of that part.
type Growing struct {
	// Name is the file's name in the commit's root; it replaces any entry
	// of that name.
	Name string
	// Path is where the file is on disk.
	Path string
	// Size is how many bytes of it the commit holds.
	Size int64
	// From names the previous commit's root entry this file continues;
	// empty means Name.
	From string
	// ModTime, when set, is recorded as the file's modification time in
	// place of the one on disk, which its writer keeps changing.
	ModTime time.Time
}

// putGrowing stores each growing file and returns nodes with them in
// place of any entry of their names.
func (w *Walker) putGrowing(ctx context.Context, nodes, base []*proto.TreeNode) ([]*proto.TreeNode, bool, error) {
	baseByName := make(map[string]*proto.TreeNode, len(base))
	for _, node := range base {
		baseByName[string(node.Stat.Name)] = node
	}

	replaced := make(map[string]bool, len(w.Growing))
	for _, g := range w.Growing {
		replaced[g.Name] = true
	}

	out := make([]*proto.TreeNode, 0, len(nodes)+len(w.Growing))
	for _, node := range nodes {
		if !replaced[string(node.Stat.Name)] {
			out = append(out, node)
		}
	}

	changed := false

	for _, g := range w.Growing {
		from := g.From
		if from == "" {
			from = g.Name
		}

		node, err := w.backupGrowing(ctx, g, baseByName[from])
		if err != nil {
			return nil, false, err
		}

		if !nodeEqual(node, baseByName[g.Name]) {
			changed = true
		}

		out = append(out, node)
	}

	return SortNodes(out), changed, nil
}

// backupGrowing stores g, continuing prev when that is the same file at a
// length no longer than g's.
func (w *Walker) backupGrowing(ctx context.Context, g Growing, prev *proto.TreeNode) (*proto.TreeNode, error) {
	file, err := os.Open(g.Path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	if info.Size() < g.Size {
		return nil, fmt.Errorf("%s holds %d bytes, fewer than the %d to commit", g.Path, info.Size(), g.Size)
	}

	stat := w.stat(info, "")
	stat.Name = []byte(g.Name)
	stat.Size = g.Size

	if !g.ModTime.IsZero() {
		stat.MtimeNs = g.ModTime.UnixNano()
	}

	atomic.AddInt64(&w.result.Files, 1)
	atomic.AddInt64(&w.result.Bytes, g.Size)

	usable := prev != nil && prev.Stat.GetType() == proto.NodeType_NODE_FILE && prev.Ref != nil && prev.Stat.GetSize() <= g.Size && !w.lost(g.Name)

	if usable && prev.Stat.GetSize() == g.Size {
		atomic.AddInt64(&w.result.Reused, 1)
		return &proto.TreeNode{Stat: stat, Ref: prev.Ref}, nil
	}

	var kept []*proto.FilePart
	known := map[string]struct{}{}

	if usable {
		obj, err := w.Objects.Get(ctx, prev.Ref)
		if err != nil {
			return nil, fmt.Errorf("loading the previous version of %s: %w", g.Name, err)
		}

		parts, err := FileParts(ctx, w.Objects, obj.GetFile())
		if err != nil {
			return nil, fmt.Errorf("loading the previous version of %s: %w", g.Name, err)
		}

		for _, part := range parts {
			if part.Ref != nil {
				known[string(part.Ref.Hash)] = struct{}{}
			}
		}

		if len(parts) > 1 && parts[0].Ref != nil {
			kept = parts[:len(parts)-1]
		}
	}

	var offset int64
	if len(kept) > 0 {
		last := kept[len(kept)-1]
		offset = int64(last.Offset + last.Length)
	}

	writer := newFileWriter(ctx, w.Objects, w.Key, g.Size)
	writer.parts = append(writer.parts, kept...)
	writer.offset = offset
	writer.sent = w.sent
	writer.source = file
	writer.cache = w.BlobCache
	writer.uploaded = &w.result.Uploaded
	writer.known = known
	writer.forced = map[string]struct{}{}
	writer.filters = w.filters
	writer.confirmer = w.confirmer
	writer.window = w.window

	if _, err := io.Copy(writer, io.NewSectionReader(file, offset, g.Size-offset)); err != nil {
		return nil, fmt.Errorf("reading %s: %w", g.Path, err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("storing %s: %w", g.Name, err)
	}

	atomic.AddInt64(&w.result.Read, 1)
	atomic.AddInt64(&w.result.Assumed, int64(len(writer.assumed)))
	atomic.AddInt64(&w.result.Repaired, int64(writer.repaired))

	return &proto.TreeNode{Stat: stat, Ref: writer.Ref()}, nil
}
