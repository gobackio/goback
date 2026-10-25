package backup

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gobackio/goback/proto"
)

// sharedContentMax bounds the size of a content a restore keeps for the
// entries that share it.
const sharedContentMax = 64 << 10

// restoredContents holds the security descriptors and extended attribute
// values a restore read, so the many entries sharing one read it once.
type restoredContents struct {
	mtx      sync.Mutex
	contents map[string][]byte
	// refused is set once the FS would not set a security descriptor for
	// lack of privilege
	refused atomic.Bool
}

// placeMeta gives path, which holds its content already, everything else
// stat records.
func (r *Restorer) placeMeta(ctx context.Context, fsys WriteFS, path string, stat *proto.FileInfo) error {
	if err := r.putStreams(ctx, fsys, path, stat); err != nil {
		return err
	}

	mode, err := r.ownFile(ctx, fsys, path, stat)
	if err != nil {
		return err
	}

	if err := r.putXattrs(ctx, fsys, path, stat); err != nil {
		return err
	}

	if err := setMeta(ctx, fsys, path, mode, mtimeOf(stat)); err != nil {
		return err
	}

	return r.putSecurity(ctx, fsys, path, stat)
}

// putStreams writes the streams stat records into path. It comes before
// the mtime is set, which writing a stream moves.
func (r *Restorer) putStreams(ctx context.Context, fsys WriteFS, path string, stat *proto.FileInfo) error {
	streams := stat.GetStreams()
	if len(streams) == 0 {
		return nil
	}

	writer, ok := fsys.(MetadataWriteFS)
	if !ok {
		atomic.AddInt64(&r.stats.StreamsLeftOut, int64(len(streams)))
		return nil
	}

	reader := NewBackupReader(r.Store).WithKey(r.Key)

	for _, stream := range streams {
		content, err := reader.ReadFile(ctx, stream.GetRef())
		if err != nil {
			return err
		}

		if err := writer.WriteStream(ctx, path, string(stream.GetName()), content); err != nil {
			return err
		}
	}

	return nil
}

// putXattrs sets the extended attributes stat records on path, counting
// those the tree does not take. It comes after the owner is given, which
// clears a file's capabilities, and before the mode is set, which may take
// away the write access setting one needs.
func (r *Restorer) putXattrs(ctx context.Context, fsys WriteFS, path string, stat *proto.FileInfo) error {
	xattrs := stat.GetXattrs()
	if len(xattrs) == 0 {
		return nil
	}

	writer, ok := fsys.(ExtendedWriteFS)
	if !ok {
		atomic.AddInt64(&r.stats.XattrsLeftOut, int64(len(xattrs)))
		return nil
	}

	for _, xattr := range xattrs {
		value, err := r.sharedContent(ctx, xattr.GetRef())
		if err != nil {
			return err
		}

		if err := writer.SetXattr(ctx, path, string(xattr.GetName()), value); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			atomic.AddInt64(&r.stats.XattrsLeftOut, 1)
		}
	}

	return nil
}

// putSecurity gives path the security descriptor, creation time,
// attributes and file flags stat records, once its content, streams and
// mtime are in place.
func (r *Restorer) putSecurity(ctx context.Context, fsys WriteFS, path string, stat *proto.FileInfo) error {
	writer, ok := fsys.(MetadataWriteFS)

	if ref := stat.GetSecurity(); ref != nil {
		err := ErrNotPrivileged
		if ok && !r.contents.refused.Load() {
			err = r.setSecurity(ctx, writer, path, ref)
		}

		switch {
		case errors.Is(err, ErrNotPrivileged):
			r.contents.refused.Store(true)
			atomic.AddInt64(&r.stats.SecurityLeftOut, 1)
		case err != nil:
			return err
		}
	}

	if ok && (stat.GetAttributes() != 0 || stat.GetBirthtimeNs() != 0) {
		var birth time.Time
		if ns := stat.GetBirthtimeNs(); ns != 0 {
			birth = time.Unix(0, ns)
		}

		if err := writer.SetAttributes(ctx, path, birth, stat.GetAttributes()); err != nil {
			return err
		}
	}

	return r.putFlags(ctx, fsys, path, stat)
}

// putFlags gives path the file flags stat records, last, since an
// immutable or append-only entry takes no other change; it counts those
// the tree does not take.
func (r *Restorer) putFlags(ctx context.Context, fsys WriteFS, path string, stat *proto.FileInfo) error {
	linux, darwin := stat.GetLinuxFlags(), stat.GetDarwinFlags()
	if linux == 0 && darwin == 0 {
		return nil
	}

	writer, ok := fsys.(ExtendedWriteFS)
	if ok && writer.SetFlags(ctx, path, linux, darwin) == nil {
		return nil
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	atomic.AddInt64(&r.stats.FlagsLeftOut, 1)

	return nil
}

func (r *Restorer) setSecurity(ctx context.Context, writer MetadataWriteFS, path string, ref *proto.Ref) error {
	sd, err := r.sharedContent(ctx, ref)
	if err != nil {
		return err
	}

	return writer.SetSecurity(ctx, path, sd)
}

// sharedContent reads the File object at ref, a security descriptor or an
// extended attribute value, which it keeps for the entries that share it
// when it is small.
func (r *Restorer) sharedContent(ctx context.Context, ref *proto.Ref) ([]byte, error) {
	key := string(ref.GetHash())

	r.contents.mtx.Lock()
	content, ok := r.contents.contents[key]
	r.contents.mtx.Unlock()

	if ok {
		return content, nil
	}

	file, err := NewBackupReader(r.Store).WithKey(r.Key).ReadFile(ctx, ref)
	if err != nil {
		return nil, err
	}

	if content, err = io.ReadAll(file); err != nil {
		return nil, err
	}

	r.contents.mtx.Lock()
	if r.contents.contents == nil {
		r.contents.contents = map[string][]byte{}
	}
	if len(content) <= sharedContentMax && len(r.contents.contents) < contentsKept {
		r.contents.contents[key] = content
	}
	r.contents.mtx.Unlock()

	return content, nil
}
