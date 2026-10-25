package backup

import (
	"context"
	"os"
	"path"
	"sync"
	"sync/atomic"

	"github.com/gobackio/goback/proto"
)

// fileID is a file's device and inode.
type fileID struct {
	dev, ino uint64
}

// hardLinks is where a walk first found each file that has more than one
// link, so the paths it finds the file at later take that one's content
// rather than read it again.
type hardLinks struct {
	mtx   sync.Mutex
	first map[fileID]*firstLink
}

// firstLink is the first path a walk found a linked file at. Its stat and
// ref are set before done closes, and stay nil when that path was not read
// or kept unchanged.
type firstLink struct {
	path string
	done chan struct{}
	stat *proto.FileInfo
	ref  *proto.Ref
}

// claim returns where the walk first found the file info describes, and
// whether that is rel, which then settles it once its node is known. It
// returns nil for a file with one link.
func (w *Walker) claim(info os.FileInfo, rel string) (*firstLink, bool) {
	id, ok := linkID(info)
	if !ok {
		return nil, false
	}

	l := w.links
	l.mtx.Lock()
	defer l.mtx.Unlock()

	if first := l.first[id]; first != nil {
		return first, false
	}

	if l.first == nil {
		l.first = map[fileID]*firstLink{}
	}

	first := &firstLink{path: path.Join(w.Subdir, rel), done: make(chan struct{})}
	l.first[id] = first

	return first, true
}

// settle hands the paths waiting on f the node the walk recorded for it;
// a node that is not its content as read or kept unchanged, or none, makes
// them read the file themselves.
func (f *firstLink) settle(node *proto.TreeNode, read bool) {
	if read && node != nil && node.Ref != nil {
		f.stat, f.ref = node.Stat, node.Ref
	}

	close(f.done)
}

// linked records the file at path, a later link to the one first found,
// with that one's content, read once, and its extended attributes and
// flags, which belong to the file rather than to a path. It reads the file
// itself when the first was not read, when the store lost what this path
// held, or when the file changed in between.
func (w *Walker) linked(ctx context.Context, first *firstLink, path, rel string, parent, info os.FileInfo, stat *proto.FileInfo, baseNode *proto.TreeNode) (*proto.TreeNode, error) {
	select {
	case <-first.done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	if first.ref == nil || w.lost(rel) || first.stat.GetSize() != stat.GetSize() || first.stat.GetMtimeNs() != stat.GetMtimeNs() {
		w.gate.Start()
		defer w.gate.Done()

		return w.backupFile(ctx, path, rel, parent, info, baseNode)
	}

	atomic.AddInt64(&w.result.Reused, 1)

	stat.Xattrs, stat.LinuxFlags, stat.DarwinFlags = first.stat.GetXattrs(), first.stat.GetLinuxFlags(), first.stat.GetDarwinFlags()
	stat.HardLink = []byte(first.path)

	return &proto.TreeNode{Stat: stat, Ref: first.ref}, nil
}
