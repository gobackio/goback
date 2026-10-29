package storage

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/gobackio/goback/storage/badger"
	"github.com/gobackio/goback/storage/pack"

	gcs "cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
)

const (
	blobObjectPrefix = "pack/"
	blobObjectKey    = blobObjectPrefix + "%s/%s" // pack/<extension>/<filename>

	// a GCS upload holds a buffer this large, 16 MiB by default, while most
	// objects written are a few bytes or KiB; S3 refuses parts under 5 MiB,
	// so other buckets keep their driver's size
	gcsStreamedBufferSize = 1 << 20
)

var _ io.ReadSeeker = (*bucketFile)(nil)
var _ io.WriterTo = (*bucketFile)(nil)
var _ io.ReaderAt = (*bucketFile)(nil)
var _ pack.StoredFile = (*bucketFile)(nil)
var _ os.FileInfo = (*bucketFileInfo)(nil)
var _ pack.ArchiveStorage = (*BucketStore)(nil)
var _ pack.InfoLister = (*BucketStore)(nil)
var _ pack.ListedOpener = (*BucketStore)(nil)

// Read reads on from the file position through one request that stays
// open while the reads follow each other; a Seek elsewhere starts another.
func (s *bucketFile) Read(buf []byte) (int, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if !s.readOnly {
		return 0, errors.New("Read only supported for readonly files")
	}

	if s.offset >= s.attrs.Size {
		return 0, io.EOF
	}

	if s.stream == nil {
		stalls := s.store.stalls()

		stream, err := s.store.bucket.NewRangeReader(stalls.ctx, s.key, s.offset, s.attrs.Size-s.offset, nil)
		s.store.count(context.Background(), OpGet, 0)

		if err != nil {
			stalls.end()

			return 0, stalls.explain(err)
		}

		stalls.disarm()
		s.stream, s.streamStalls = stream, stalls
	}

	s.streamStalls.arm()
	n, err := s.stream.Read(buf)
	s.streamStalls.disarm()
	err = s.streamStalls.explain(err)
	s.offset += int64(n)
	s.store.carried(OpGet, int64(n))

	if err != nil || s.offset >= s.attrs.Size {
		s.closeStream()

		if errors.Is(err, io.EOF) && s.offset < s.attrs.Size {
			err = io.ErrUnexpectedEOF
		}
	}

	return n, err
}

func (s *bucketFile) closeStream() {
	if s.stream != nil {
		_ = s.stream.Close()
		s.streamStalls.end()
		s.stream, s.streamStalls = nil, nil
	}
}

func (s *bucketFile) ReadAt(buf []byte, offset int64) (int, error) {
	return s.ReadAtContext(context.Background(), buf, offset)
}

// ReadAtContext implements pack.ReaderAtContext: the range read is
// cancelled with ctx and counted toward the tallies ctx carries.
func (s *bucketFile) ReadAtContext(ctx context.Context, buf []byte, offset int64) (int, error) {
	if !s.readOnly {
		return 0, errors.New("Read only supported for readonly files")
	}

	if offset < 0 {
		return 0, errors.New("negative offset")
	}

	if offset >= s.attrs.Size {
		return 0, io.EOF
	}

	length := min(int64(len(buf)), s.attrs.Size-offset)

	stalls := s.store.stallsUnder(ctx)
	defer stalls.end()

	reader, err := s.store.bucket.NewRangeReader(stalls.ctx, s.key, offset, length, nil)
	if err != nil {
		s.store.count(ctx, OpGet, 0)

		return 0, stalls.explain(err)
	}

	defer reader.Close()

	stalls.disarm()
	n, err := io.ReadFull(stalls.reader(reader), buf[:length])
	err = stalls.explain(err)
	s.store.count(ctx, OpGet, int64(n))

	if err == nil && n < len(buf) {
		err = io.EOF
	}

	return n, err
}

// WriteTo writes the file from its position on to w, and moves the
// position to the end of what it wrote.
func (s *bucketFile) WriteTo(w io.Writer) (int64, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if !s.readOnly {
		return -1, errors.New("WriteTo only supported for readonly files")
	}

	if s.offset >= s.attrs.Size {
		return 0, nil
	}

	s.closeStream()

	stalls := s.store.stalls()
	defer stalls.end()

	reader, err := s.store.bucket.NewRangeReader(stalls.ctx, s.key, s.offset, s.attrs.Size-s.offset, nil)
	if err != nil {
		s.store.count(context.Background(), OpGet, 0)

		return 0, stalls.explain(err)
	}

	defer reader.Close()

	stalls.disarm()
	n, err := io.Copy(w, stalls.reader(reader))
	err = stalls.explain(err)
	s.offset += n
	s.store.count(context.Background(), OpGet, n)

	return n, err
}

func (s *bucketFile) Seek(offset int64, whence int) (int64, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if !s.readOnly {
		return -1, errors.New("Seek only supported for readonly files")
	}

	position := s.offset

	switch whence {
	case io.SeekStart:
		position = offset
	case io.SeekCurrent:
		position += offset
	case io.SeekEnd:
		position = s.attrs.Size + offset
	default:
		return 0, errors.New("invalid whence value")
	}

	if position < 0 {
		return 0, errors.New("negative position")
	}

	if position != s.offset {
		s.closeStream()
		s.offset = position
	}

	return s.offset, nil
}

type bucketFile struct {
	store   *BucketStore
	key     string
	onClose func()

	writer  *blob.Writer
	written int64
	// upload is the Cloud Storage writer under writer, whose answer says
	// what the bucket stored; stored is that answer once writer is closed
	upload *gcs.Writer
	stored *blob.Attributes

	// writeStalls bounds each step of writer, streamStalls each of stream
	writeStalls  *stalls
	streamStalls *stalls

	readOnly bool
	offset   int64
	stream   *blob.Reader
	attrs    *blob.Attributes
	mtx      sync.Mutex
}

func (s *bucketFile) Close() error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.closeStream()

	if s.readOnly || s.writer == nil {
		return nil
	}

	s.writeStalls.arm()
	err := s.writeStalls.explain(s.writer.Close())
	s.writeStalls.end()
	s.store.count(context.Background(), OpPut, s.written)
	s.writer = nil

	if err != nil {
		if s.onClose != nil {
			s.onClose()
		}

		return err
	}

	if s.upload != nil {
		if attrs := s.upload.Attrs(); attrs != nil {
			// as the bucket's Attributes answers for the object
			s.stored = &blob.Attributes{Size: attrs.Size, ModTime: attrs.Updated, CreateTime: attrs.Created, MD5: attrs.MD5}
		}
	}

	if s.onClose != nil {
		s.onClose()
	}

	return nil
}

// Stored implements pack.StoredFile with what Cloud Storage answered the
// upload with; other buckets do not say.
func (s *bucketFile) Stored() (os.FileInfo, []byte, bool) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.stored == nil {
		return nil, nil, false
	}

	return &bucketFileInfo{attrs: s.stored, key: s.key}, s.stored.MD5, true
}

func (s *bucketFile) Write(buf []byte) (int, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.readOnly || s.writer == nil {
		return 0, errors.New("Cannot write to read only file")
	}

	s.writeStalls.arm()
	n, err := s.writer.Write(buf)
	s.writeStalls.disarm()
	err = s.writeStalls.explain(err)
	s.written += int64(n)

	return n, err
}

func (s *bucketFile) Stat() (os.FileInfo, error) {
	return &bucketFileInfo{attrs: s.attrs, key: s.key}, nil
}

var _ pack.File = (*bucketFile)(nil)

// BucketStore is a pack.ArchiveStorage over a gocloud bucket, keyed
// pack/<extension>/<name>.
type BucketStore struct {
	bucket *blob.Bucket
	attrs  []attribute.KeyValue

	openFilesMtx sync.Mutex
	openFiles    map[string]*bucketFile

	sync      func(key string) error
	createNew func(key string, data []byte) error

	timeout time.Duration

	onGCS bool
}

// bufferSize is the upload buffer for a write of size bytes on GCS, and
// the driver's default elsewhere.
func (c *BucketStore) bufferSize(size int) int {
	if !c.onGCS {
		return 0
	}

	// zero would be the default
	return max(size, 1)
}

// A BucketOption adjusts a BucketStore.
type BucketOption func(*BucketStore)

// WithStoreName labels what this store sends to the object store, so the
// cost of one store can be told from another's in a process holding
// several of them.
func WithStoreName(name string) BucketOption {
	return func(c *BucketStore) {
		c.attrs = append(c.attrs, keyStore.String(name))
	}
}

// WithSync has Sync call sync with the file's bucket key, for a bucket
// whose writes are not durable when they return, as a directory's are not.
func WithSync(sync func(key string) error) BucketOption {
	return func(c *BucketStore) {
		c.sync = sync
	}
}

// WithCreateNew has CreateNew call create with the file's bucket key, for
// a bucket whose own precondition does not exclude a concurrent writer, as
// a directory's does not. create returns pack.ErrFileExists when the key
// is taken.
func WithCreateNew(create func(key string, data []byte) error) BucketOption {
	return func(c *BucketStore) {
		c.createNew = create
	}
}

// DefaultOperationTimeout is how long a BucketStore waits on one request
// to its bucket, or on one step of a streamed one, before it gives up.
const DefaultOperationTimeout = 5 * time.Minute

// WithOperationTimeout bounds what the store waits on its bucket: a
// request such as a listing page, a delete or a small write by d in all,
// and a streamed read or write by d for each step of it, so a long
// transfer that keeps moving is never cut. Zero or less waits forever.
func WithOperationTimeout(d time.Duration) BucketOption {
	return func(c *BucketStore) {
		c.timeout = d
	}
}

// request is the context of one bucket request.
func (c *BucketStore) request() (context.Context, context.CancelFunc) {
	if c.timeout <= 0 {
		return context.WithCancel(context.Background())
	}

	return context.WithTimeout(context.Background(), c.timeout)
}

// timedOut says that err came of a request running past the timeout, when
// it did.
func (c *BucketStore) timedOut(ctx context.Context, err error) error {
	if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("the bucket did not answer within %s: %w", c.timeout, err)
	}

	return err
}

// stalls is the context of a streamed bucket request, which ends once a
// step of it runs longer than the store's timeout: the request until it is
// disarmed, then each step between arm and disarm. end releases it.
type stalls struct {
	ctx     context.Context
	cancel  context.CancelFunc
	timer   *time.Timer
	timeout time.Duration
}

func (c *BucketStore) stalls() *stalls {
	return c.stallsUnder(context.Background())
}

// stallsUnder is stalls for a request cancelled with ctx as well.
func (c *BucketStore) stallsUnder(parent context.Context) *stalls {
	ctx, cancel := context.WithCancel(parent)
	s := &stalls{ctx: ctx, cancel: cancel, timeout: c.timeout}

	if c.timeout > 0 {
		s.timer = time.AfterFunc(c.timeout, cancel)
	}

	return s
}

func (s *stalls) arm() {
	if s.timer != nil {
		s.timer.Reset(s.timeout)
	}
}

func (s *stalls) disarm() {
	if s.timer != nil {
		s.timer.Stop()
	}
}

func (s *stalls) end() {
	s.disarm()
	s.cancel()
}

// explain says that err came of a step running past the timeout, when it
// did.
func (s *stalls) explain(err error) error {
	if err != nil && s.timer != nil && s.ctx.Err() != nil {
		return fmt.Errorf("the bucket did not answer within %s: %w", s.timeout, err)
	}

	return err
}

// reader reads r with each read a step.
func (s *stalls) reader(r io.Reader) io.Reader {
	return stallingReader{r: r, stalls: s}
}

type stallingReader struct {
	r      io.Reader
	stalls *stalls
}

func (r stallingReader) Read(buf []byte) (int, error) {
	r.stalls.arm()
	defer r.stalls.disarm()

	return r.r.Read(buf)
}

// Sync implements pack.Syncer.
func (c *BucketStore) Sync(name string) error {
	if c.sync == nil {
		return nil
	}

	return c.sync(c.key(name))
}

// uploading is a handle on a file still uploading, which is not in the
// bucket yet: it reads nothing, and closing it leaves the upload alone.
func (c *BucketStore) uploading(key string) (pack.File, bool) {
	c.openFilesMtx.Lock()
	_, ok := c.openFiles[key]
	c.openFilesMtx.Unlock()

	return &bucketFile{key: key, store: c}, ok
}

func (c *BucketStore) openFile(key string) (pack.File, error) {
	if file, ok := c.uploading(key); ok {
		return file, nil
	}

	ctx, cancel := c.request()
	defer cancel()

	attrs, err := c.bucket.Attributes(ctx, key)
	c.count(context.Background(), OpHead, 0)

	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil, pack.ErrFileNotFound
		}
		return nil, c.timedOut(ctx, err)
	}

	return &bucketFile{
		key:      key,
		attrs:    attrs,
		store:    c,
		readOnly: true,
	}, nil
}

func (c *BucketStore) newWriteFile(key string) (pack.File, error) {
	file := &bucketFile{
		store: c,
		key:   key,
		onClose: func() {
			c.openFilesMtx.Lock()
			delete(c.openFiles, key)
			c.openFilesMtx.Unlock()
		},
	}

	file.writeStalls = c.stalls()

	writer, err := c.bucket.NewWriter(file.writeStalls.ctx, key, &blob.WriterOptions{
		BufferSize: c.bufferSize(gcsStreamedBufferSize),
		// runs under the file's lock, from the Write or Close that starts the upload
		BeforeWrite: func(as func(any) bool) error {
			var upload *gcs.Writer
			if as(&upload) {
				file.upload = upload
			}

			return nil
		},
	})
	if err != nil {
		c.count(context.Background(), OpPut, 0)
		file.writeStalls.end()

		return nil, file.writeStalls.explain(err)
	}

	file.writeStalls.disarm()
	file.writer = writer

	c.openFilesMtx.Lock()
	c.openFiles[key] = file
	c.openFilesMtx.Unlock()

	return file, nil
}

func (c *BucketStore) key(name string) string {
	return ArchiveKey(name)
}

// ArchiveKey is where a BucketStore keeps the archive file name in its
// bucket.
func ArchiveKey(name string) string {
	return fmt.Sprintf(blobObjectKey, path.Ext(name), name)
}

// Open implements pack.ArchiveStorage.
func (c *BucketStore) Open(name string) (pack.File, error) {
	return c.openFile(c.key(name))
}

// OpenListed implements pack.ListedOpener.
func (c *BucketStore) OpenListed(file pack.ListedFile) (pack.File, error) {
	key := c.key(file.Name)

	if open, ok := c.uploading(key); ok {
		return open, nil
	}

	return &bucketFile{
		key:      key,
		attrs:    &blob.Attributes{Size: file.Size, ModTime: file.Modified},
		store:    c,
		readOnly: true,
	}, nil
}

// Create implements pack.ArchiveStorage.
func (c *BucketStore) Create(name string) (pack.File, error) {
	return c.newWriteFile(c.key(name))
}

// CreateNew implements pack.ArchiveStorage with the bucket's own
// precondition, so it holds across processes on GCS and S3, or with the
// WithCreateNew function.
func (c *BucketStore) CreateNew(name string, data []byte) error {
	if c.createNew != nil {
		c.count(context.Background(), OpPut, int64(len(data)))

		return c.createNew(c.key(name), data)
	}

	ctx, cancel := c.request()
	defer cancel()

	err := c.bucket.WriteAll(ctx, c.key(name), data, &blob.WriterOptions{IfNotExist: true, BufferSize: c.bufferSize(len(data))})
	c.count(context.Background(), OpPut, int64(len(data)))

	if gcerrors.Code(err) == gcerrors.FailedPrecondition {
		return pack.ErrFileExists
	}

	return c.timedOut(ctx, err)
}

// Checksum implements pack.Checksummer with the MD5 the bucket reports,
// empty when it has none.
func (c *BucketStore) Checksum(name string) ([]byte, error) {
	ctx, cancel := c.request()
	defer cancel()

	attrs, err := c.bucket.Attributes(ctx, c.key(name))
	c.count(context.Background(), OpHead, 0)

	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil, pack.ErrFileNotFound
		}

		return nil, c.timedOut(ctx, err)
	}

	return attrs.MD5, nil
}

// Delete implements pack.ArchiveStorage.
func (c *BucketStore) Delete(name string) error {
	c.count(context.Background(), OpDelete, 0)

	ctx, cancel := c.request()
	defer cancel()

	err := c.bucket.Delete(ctx, c.key(name))
	if gcerrors.Code(err) == gcerrors.NotFound {
		return pack.ErrFileNotFound
	}

	return c.timedOut(ctx, err)
}

// Move implements pack.ArchiveStorage by copying within the bucket and
// deleting the original.
func (c *BucketStore) Move(from, to string) error {
	c.count(context.Background(), OpCopy, 0)

	ctx, cancel := c.request()
	defer cancel()

	err := c.bucket.Copy(ctx, c.key(to), c.key(from), nil)
	if gcerrors.Code(err) == gcerrors.NotFound {
		return pack.ErrFileNotFound
	}

	if err != nil {
		return c.timedOut(ctx, err)
	}

	if err := c.Delete(from); err != nil && !errors.Is(err, pack.ErrFileNotFound) {
		return err
	}

	return nil
}

// listPageSize is how many keys one listing request asks for, which is
// the most either S3 or Cloud Storage answers with.
const listPageSize = 1000

// pages walks a prefix a page at a time, because a listing is billed by
// the page rather than by the walk.
func (c *BucketStore) pages(prefix string, each func(*blob.ListObject) error) error {
	options := &blob.ListOptions{Prefix: prefix}

	for token := blob.FirstPageToken; ; {
		page, next, err := c.listPage(token, options)
		c.count(context.Background(), OpList, 0)

		if err != nil {
			return err
		}

		for _, object := range page {
			if object.IsDir {
				continue
			}

			err = each(object)
			if err != nil {
				return err
			}
		}

		if len(next) == 0 {
			return nil
		}

		token = next
	}
}

func (c *BucketStore) listPage(token []byte, options *blob.ListOptions) ([]*blob.ListObject, []byte, error) {
	ctx, cancel := c.request()
	defer cancel()

	page, next, err := c.bucket.ListPage(ctx, token, listPageSize, options)

	return page, next, c.timedOut(ctx, err)
}

// DeleteAll implements pack.ArchiveStorage.
func (c *BucketStore) DeleteAll() error {
	return c.pages(blobObjectPrefix, func(object *blob.ListObject) error {
		c.count(context.Background(), OpDelete, 0)

		ctx, cancel := c.request()
		defer cancel()

		return c.timedOut(ctx, c.bucket.Delete(ctx, object.Key))
	})
}

// List implements pack.ArchiveStorage.
func (c *BucketStore) List(extension string) ([]string, error) {
	var names []string

	err := c.list(extension, func(name string, _ *blob.ListObject) {
		names = append(names, name)
	})

	return names, err
}

// ListInfo implements pack.InfoLister.
func (c *BucketStore) ListInfo(extension string) ([]pack.ListedFile, error) {
	var files []pack.ListedFile

	err := c.list(extension, func(name string, object *blob.ListObject) {
		files = append(files, pack.ListedFile{Name: name, Size: object.Size, Modified: object.ModTime})
	})

	return files, err
}

func (c *BucketStore) list(extension string, each func(name string, object *blob.ListObject)) error {
	prefix := blobObjectPrefix
	if extension != "" {
		if len(extension) < 2 || extension[0] != '.' {
			return pack.ErrInvalidExtension
		}

		prefix = fmt.Sprintf(blobObjectKey, extension, "")
	}

	return c.pages(prefix, func(object *blob.ListObject) error {
		// keys are pack/<extension>/<name>; the name may hold slashes
		name := strings.TrimPrefix(object.Key, blobObjectPrefix)
		if _, rest, ok := strings.Cut(name, "/"); ok && extension == "" {
			name = rest
		} else if extension != "" {
			name = strings.TrimPrefix(object.Key, prefix)
		}

		each(name, object)

		return nil
	})
}

type bucketFileInfo struct {
	key   string
	attrs *blob.Attributes
}

func (s *bucketFileInfo) Sys() interface{} {
	return s.attrs
}

func (s *bucketFileInfo) Size() int64 {
	return s.attrs.Size
}

func (s *bucketFileInfo) Name() string {
	return path.Base(s.key)
}

func (s *bucketFileInfo) Mode() os.FileMode {
	return 0
}

func (s *bucketFileInfo) ModTime() time.Time {
	return s.attrs.ModTime
}

func (s *bucketFileInfo) IsDir() bool {
	return false
}

// NewBucketStore returns an archive storage over bucket.
func NewBucketStore(bucket *blob.Bucket, options ...BucketOption) *BucketStore {
	storage := &BucketStore{
		bucket:    bucket,
		openFiles: make(map[string]*bucketFile),
		timeout:   DefaultOperationTimeout,
	}

	for _, option := range options {
		option(storage)
	}

	var client *gcs.Client
	storage.onGCS = bucket.As(&client)

	return storage
}

// NewBucketObjectStore returns a pack store over a remote bucket that keeps
// its archives in index and, if cacheDir is not empty, a local metadata
// cache of at most cacheSize bytes (0 is unbounded); extra pack options
// follow.
func NewBucketObjectStore(bucket *blob.Bucket, index pack.ArchiveIndex, cacheDir string, cacheSize int64, extra ...pack.PackOption) (*pack.PackStorage, error) {
	options := []pack.PackOption{
		pack.WithArchiveStorage(NewBucketStore(bucket)),
		pack.WithArchiveIndex(index),
		pack.WithMaxParallel(64),
		pack.WithCloseBeforeRead(true),
		pack.WithMaxSize(1024 * 1024 * 1024),
		pack.WithIdleFinalize(5 * time.Minute),
		pack.WithSessionLease(30 * time.Minute),
		pack.WithCompaction(pack.CompactionConfig{MinimumCandidates: 1000}),
	}

	if cacheDir != "" {
		err := os.MkdirAll(cacheDir, 0755)
		if err != nil {
			return nil, err
		}

		cache, err := badger.New(cacheDir, badger.WithCapacity(cacheSize))
		switch {
		case errors.Is(err, badger.ErrLocked):
			slog.Warn("running without the metadata cache, which another goback process has open", "dir", cacheDir)
		case err != nil:
			return nil, err
		default:
			options = append(options, pack.WithMetadataCache(cache))
		}
	}

	return pack.NewPackStorage(append(options, extra...)...)
}
