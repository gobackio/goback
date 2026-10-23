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

// restoredDescriptors holds the security descriptors a restore read, so the
// many entries sharing one read it once.
type restoredDescriptors struct {
	mtx sync.Mutex
	sds map[string][]byte
	// refused is set once the FS would not set one for lack of privilege
	refused atomic.Bool
}

// placeMeta gives path, which holds its content already, everything else
// stat records.
func (r *Restorer) placeMeta(ctx context.Context, fsys WriteFS, path string, stat *proto.FileInfo) error {
	if err := r.putStreams(ctx, fsys, path, stat); err != nil {
		return err
	}

	if err := r.putOwner(ctx, fsys, path, stat); err != nil {
		return err
	}

	if err := setMeta(ctx, fsys, path, fileMode(stat), mtimeOf(stat)); err != nil {
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

// putSecurity gives path the security descriptor, creation time and
// attributes stat records, once its content, streams and mtime are in
// place.
func (r *Restorer) putSecurity(ctx context.Context, fsys WriteFS, path string, stat *proto.FileInfo) error {
	writer, ok := fsys.(MetadataWriteFS)

	if ref := stat.GetSecurity(); ref != nil {
		err := ErrNotPrivileged
		if ok && !r.descriptors.refused.Load() {
			err = r.setSecurity(ctx, writer, path, ref)
		}

		switch {
		case errors.Is(err, ErrNotPrivileged):
			r.descriptors.refused.Store(true)
			atomic.AddInt64(&r.stats.SecurityLeftOut, 1)
		case err != nil:
			return err
		}
	}

	if !ok || (stat.GetAttributes() == 0 && stat.GetBirthtimeNs() == 0) {
		return nil
	}

	var birth time.Time
	if ns := stat.GetBirthtimeNs(); ns != 0 {
		birth = time.Unix(0, ns)
	}

	return writer.SetAttributes(ctx, path, birth, stat.GetAttributes())
}

func (r *Restorer) setSecurity(ctx context.Context, writer MetadataWriteFS, path string, ref *proto.Ref) error {
	key := string(ref.GetHash())

	r.descriptors.mtx.Lock()
	sd, ok := r.descriptors.sds[key]
	r.descriptors.mtx.Unlock()

	if !ok {
		content, err := NewBackupReader(r.Store).WithKey(r.Key).ReadFile(ctx, ref)
		if err != nil {
			return err
		}

		if sd, err = io.ReadAll(content); err != nil {
			return err
		}

		r.descriptors.mtx.Lock()
		if r.descriptors.sds == nil {
			r.descriptors.sds = map[string][]byte{}
		}
		if len(r.descriptors.sds) < descriptorsKept {
			r.descriptors.sds[key] = sd
		}
		r.descriptors.mtx.Unlock()
	}

	return writer.SetSecurity(ctx, path, sd)
}
