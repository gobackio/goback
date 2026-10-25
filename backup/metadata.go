package backup

import (
	"bytes"
	"context"
	"io"
	"sync"
	"sync/atomic"

	"github.com/gobackio/goback/proto"
)

// recallAttributes are FILE_ATTRIBUTE_RECALL_ON_OPEN and
// FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS, which a cloud sync client sets on a
// file whose content it keeps only in the cloud and downloads when read.
const recallAttributes = 0x0004_0000 | 0x0040_0000

// cloudOnly reports whether stat is of a file whose content was only in
// the cloud, which a walk records without it unless DownloadPlaceholders.
func cloudOnly(stat *proto.FileInfo) bool {
	return stat.GetType() == proto.NodeType_NODE_FILE && stat.GetAttributes()&recallAttributes != 0
}

// contentsKept bounds the security descriptors and extended attribute
// values a walk remembers the stored ref of; beyond it, each further one is
// stored again.
const contentsKept = 10_000

// sharedContents remembers where a walk stored each security descriptor
// and extended attribute value, and which ones the base commit holds, so
// the many entries that share one store it once.
type sharedContents struct {
	mtx  sync.Mutex
	refs map[string]*proto.Ref
	held map[string]bool
}

// hold notes a content the base commit refers to.
func (d *sharedContents) hold(ref *proto.Ref) {
	if ref == nil {
		return
	}

	d.mtx.Lock()
	defer d.mtx.Unlock()

	if d.held == nil {
		d.held = map[string]bool{}
	}

	if len(d.held) < contentsKept {
		d.held[string(ref.Hash)] = true
	}
}

// stored returns the ref sd is held or was stored under, storing it with
// put the first time. Concurrent callers with the same content wait for one
// put.
func (d *sharedContents) stored(sd []byte, ref *proto.Ref, put func() (*proto.Ref, error)) (*proto.Ref, error) {
	d.mtx.Lock()
	defer d.mtx.Unlock()

	if d.held[string(ref.Hash)] {
		return ref, nil
	}

	if stored := d.refs[string(sd)]; stored != nil {
		return stored, nil
	}

	stored, err := put()
	if err != nil {
		return nil, err
	}

	if d.refs == nil {
		d.refs = map[string]*proto.Ref{}
	}

	if len(d.refs) < contentsKept {
		d.refs[string(sd)] = stored
	}

	return stored, nil
}

// metadata records the extended attributes and flags, security descriptor
// and streams of the entry at path into stat. It takes the descriptor and
// streams from the base node when the size, mtime and change time say
// nothing changed and the base node holds a security descriptor, and keeps
// the base node's for what it cannot read.
func (w *Walker) metadata(ctx context.Context, path, rel string, stat *proto.FileInfo, baseNode *proto.TreeNode) error {
	if err := w.extended(ctx, path, rel, stat, baseNode); err != nil {
		return err
	}

	fsys, ok := w.fs().(MetadataFS)
	if !ok {
		return nil
	}

	base := baseNode.GetStat()
	if base.GetType() != stat.GetType() {
		base = nil
	}

	if !w.lost(rel) {
		w.contents.hold(base.GetSecurity())
	}

	if !w.Verify && !w.lost(rel) && base.GetCtimeNs() != 0 && base.GetSecurity() != nil &&
		base.GetCtimeNs() == stat.GetCtimeNs() && base.GetMtimeNs() == stat.GetMtimeNs() && base.GetSize() == stat.GetSize() {
		stat.Security, stat.Streams = base.GetSecurity(), base.GetStreams()
		return nil
	}

	sd, err := fsys.Security(ctx, path)
	switch {
	case err != nil:
		w.logger().Warn("reading the security descriptor failed, keeping the previous one", "path", path, "err", err)
		atomic.AddInt64(&w.result.Unreadable, 1)
		stat.Security = base.GetSecurity()
	case len(sd) > 0:
		if stat.Security, err = w.putShared(ctx, sd); err != nil {
			return err
		}
	}

	if cloudOnly(stat) {
		stat.Streams = nil
		return nil
	}

	listed, err := fsys.Streams(ctx, path)
	if err != nil {
		w.logger().Warn("listing streams failed, keeping the previous ones", "path", path, "err", err)
		atomic.AddInt64(&w.result.Unreadable, 1)
		stat.Streams = base.GetStreams()

		return nil
	}

	stat.Streams = nil

	for _, info := range listed {
		stream, err := w.putAltStream(ctx, fsys, path, info)
		if err != nil && ctx.Err() == nil && !isStorageError(err) {
			w.logger().Warn("reading a stream failed, keeping the previous version", "path", path, "stream", info.Name, "err", err)
			atomic.AddInt64(&w.result.Unreadable, 1)
			stream, err = baseStream(base, info.Name), nil
		}

		if err != nil {
			return err
		}

		if stream != nil {
			stat.Streams = append(stat.Streams, stream)
		}
	}

	proto.SortStreams(stat.Streams)

	return nil
}

// placeholder records a cloud-only file without reading it, which would
// download it: its stat and security descriptor, with the base node's
// content when the base node has the same size and mtime, and the empty
// file when there is no such content.
func (w *Walker) placeholder(ctx context.Context, path, rel string, stat *proto.FileInfo, baseNode *proto.TreeNode) (*proto.TreeNode, error) {
	atomic.AddInt64(&w.result.Placeholders, 1)

	if err := w.metadata(ctx, path, rel, stat, baseNode); err != nil {
		return nil, err
	}

	base := baseNode.GetStat()
	if baseNode.GetRef() != nil && base.GetType() == proto.NodeType_NODE_FILE && !base.GetLost() && !w.lost(rel) && metaEqual(base, stat) {
		return &proto.TreeNode{Stat: stat, Ref: baseNode.Ref}, nil
	}

	ref, err := w.emptyFile()
	if err != nil {
		return nil, err
	}

	return &proto.TreeNode{Stat: stat, Ref: ref}, nil
}

// putShared stores sd, a security descriptor or an extended attribute
// value, unless this run stored it already or the base commit holds it.
func (w *Walker) putShared(ctx context.Context, sd []byte) (*proto.Ref, error) {
	ref, err := PutFile(ctx, discardStore{}, w.Key, int64(len(sd)), bytes.NewReader(sd))
	if err != nil {
		return nil, err
	}

	return w.contents.stored(sd, ref, func() (*proto.Ref, error) {
		return w.putContent(ctx, int64(len(sd)), bytes.NewReader(sd))
	})
}

func (w *Walker) putAltStream(ctx context.Context, fsys MetadataFS, path string, info StreamInfo) (*proto.Stream, error) {
	file, err := fsys.OpenStream(ctx, path, info.Name)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	counted := &countingReader{r: file}

	ref, err := w.putContent(ctx, info.Size, counted)
	if err != nil {
		return nil, err
	}

	return &proto.Stream{Name: []byte(info.Name), Ref: ref, Size: counted.n}, nil
}

// putContent stores content as a file object the way a file's content is
// stored. A failure to store it comes back as a storageError.
func (w *Walker) putContent(ctx context.Context, size int64, content io.Reader) (*proto.Ref, error) {
	writer := newFileWriter(ctx, w.Objects, w.Key, size)
	writer.sent = w.sent
	writer.cache = w.BlobCache
	writer.uploaded = &w.result.Uploaded
	writer.blobs = w.blobs

	read := &readErr{r: content}
	if _, err := io.Copy(writer, read); err != nil {
		if read.err != nil {
			return nil, err
		}

		return nil, storageError{err}
	}

	if err := writer.Close(); err != nil {
		return nil, storageError{err}
	}

	return writer.Ref(), nil
}

// storageError is a failure to store, which fails the walk, as opposed to
// one to read, which keeps what the base node had.
type storageError struct{ error }

func (e storageError) Unwrap() error { return e.error }

func isStorageError(err error) bool {
	_, ok := err.(storageError)
	return ok
}

func baseStream(base *proto.FileInfo, name string) *proto.Stream {
	for _, stream := range base.GetStreams() {
		if string(stream.GetName()) == name {
			return stream
		}
	}

	return nil
}

// readErr remembers the error its reader returned, which tells a failed
// read from a failed upload once io.Copy hands either back.
type readErr struct {
	r   io.Reader
	err error
}

func (r *readErr) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if err != nil && err != io.EOF {
		r.err = err
	}

	return n, err
}
