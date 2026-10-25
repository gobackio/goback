package backup

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/gobackio/goback/proto"
)

// extended records the extended attributes and file flags of the file or
// directory at path into stat, keeping the base node's when it cannot
// read them.
func (w *Walker) extended(ctx context.Context, path, rel string, stat *proto.FileInfo, baseNode *proto.TreeNode) error {
	read, err := w.readExtended(ctx, path, rel)
	if errors.Is(err, errors.ErrUnsupported) {
		return nil
	}

	base := baseNode.GetStat()
	if base.GetType() != stat.GetType() {
		base = nil
	}

	if !w.lost(rel) {
		for _, xattr := range base.GetXattrs() {
			w.contents.hold(xattr.GetRef())
		}
	}

	if err != nil {
		if !vanished(err) && !errors.Is(err, errVanished) {
			w.logger().Warn("reading extended attributes and flags failed, keeping the previous ones", "path", path, "err", err)
			atomic.AddInt64(&w.result.Unreadable, 1)
		}

		stat.Xattrs, stat.LinuxFlags, stat.DarwinFlags = base.GetXattrs(), base.GetLinuxFlags(), base.GetDarwinFlags()

		return nil
	}

	stat.LinuxFlags, stat.DarwinFlags = read.LinuxFlags, read.DarwinFlags
	stat.Xattrs = nil

	for name, value := range read.Xattrs {
		ref, err := w.putShared(ctx, value)
		if err != nil {
			return err
		}

		stat.Xattrs = append(stat.Xattrs, &proto.Stream{Name: []byte(name), Ref: ref, Size: int64(len(value))})
	}

	proto.SortStreams(stat.Xattrs)

	return nil
}

// readExtended reads what extended records, in the walk's local root when
// it has one.
func (w *Walker) readExtended(ctx context.Context, path, rel string) (Extended, error) {
	if w.local != nil {
		return extendedIn(w.local, rel)
	}

	fsys, ok := w.fs().(ExtendedFS)
	if !ok {
		return Extended{}, errors.ErrUnsupported
	}

	return fsys.Extended(ctx, path)
}
