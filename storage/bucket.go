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

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
)

const (
	blobObjectPrefix = "pack/"
	blobObjectKey    = blobObjectPrefix + "%s/%s" // pack/<extension>/<filename>
)

var _ io.ReadSeeker = (*bucketFile)(nil)
var _ io.WriterTo = (*bucketFile)(nil)
var _ io.ReaderAt = (*bucketFile)(nil)
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
		stream, err := s.store.bucket.NewRangeReader(context.Background(), s.key, s.offset, s.attrs.Size-s.offset, nil)
		s.store.count(OpGet, 0)

		if err != nil {
			return 0, err
		}

		s.stream = stream
	}

	n, err := s.stream.Read(buf)
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
		s.stream = nil
	}
}

func (s *bucketFile) ReadAt(buf []byte, offset int64) (int, error) {
	if !s.readOnly {
		return 0, errors.New("Read only supported for readonly files")
	}

	if offset >= s.attrs.Size {
		return 0, io.EOF
	}

	length := int64(len(buf))

	if offset+length >= s.attrs.Size {
		length = s.attrs.Size - offset
	}

	reader, err := s.store.bucket.NewRangeReader(context.Background(), s.key, offset, length, nil)
	if err != nil {
		s.store.count(OpGet, 0)

		return 0, err
	}

	defer reader.Close()

	n, err := io.ReadFull(reader, buf[:length])
	s.store.count(OpGet, int64(n))

	return n, err
}

func (s *bucketFile) WriteTo(w io.Writer) (int64, error) {
	if !s.readOnly {
		return -1, errors.New("WriteTo only supported for readonly files")
	}

	reader, err := s.store.bucket.NewReader(context.Background(), s.key, nil)

	if err != nil {
		s.store.count(OpGet, 0)

		return 0, err
	}

	defer reader.Close()

	n, err := io.Copy(w, reader)
	s.store.count(OpGet, n)

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
		position = s.attrs.Size - offset
	default:
		return 0, errors.New("invalid whence value")
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

	err := s.writer.Close()
	s.store.count(OpPut, s.written)

	if err != nil {
		return err
	}
	s.writer = nil
	if s.onClose != nil {
		s.onClose()
	}

	return nil
}

func (s *bucketFile) Write(buf []byte) (int, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.readOnly {
		return 0, errors.New("Cannot write to read only file")
	}

	n, err := s.writer.Write(buf)
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

func (c *BucketStore) openFile(key string) (pack.File, error) {
	// a file still uploading is not in the bucket yet
	c.openFilesMtx.Lock()
	file, ok := c.openFiles[key]
	c.openFilesMtx.Unlock()

	if ok {
		return file, nil
	}

	attrs, err := c.bucket.Attributes(context.Background(), key)
	c.count(OpHead, 0)

	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil, pack.ErrFileNotFound
		}
		return nil, err
	}

	return &bucketFile{
		key:      key,
		attrs:    attrs,
		store:    c,
		readOnly: true,
	}, nil
}

func (c *BucketStore) newWriteFile(key string) (pack.File, error) {
	writer, err := c.bucket.NewWriter(context.Background(), key, nil)
	if err != nil {
		c.count(OpPut, 0)

		return nil, err
	}

	file := &bucketFile{
		writer: writer,
		store:  c,
		key:    key,
		onClose: func() {
			c.openFilesMtx.Lock()
			delete(c.openFiles, key)
			c.openFilesMtx.Unlock()
		},
	}

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

	c.openFilesMtx.Lock()
	open, ok := c.openFiles[key]
	c.openFilesMtx.Unlock()

	if ok {
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
// precondition, so it holds across processes on GCS and S3; a file://
// bucket only checks within one process.
func (c *BucketStore) CreateNew(name string, data []byte) error {
	err := c.bucket.WriteAll(context.Background(), c.key(name), data, &blob.WriterOptions{IfNotExist: true})
	c.count(OpPut, int64(len(data)))

	if gcerrors.Code(err) == gcerrors.FailedPrecondition {
		return pack.ErrFileExists
	}

	return err
}

// Checksum implements pack.Checksummer with the MD5 the bucket reports,
// empty when it has none.
func (c *BucketStore) Checksum(name string) ([]byte, error) {
	attrs, err := c.bucket.Attributes(context.Background(), c.key(name))
	c.count(OpHead, 0)

	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil, pack.ErrFileNotFound
		}

		return nil, err
	}

	return attrs.MD5, nil
}

// Delete implements pack.ArchiveStorage.
func (c *BucketStore) Delete(name string) error {
	c.count(OpDelete, 0)

	err := c.bucket.Delete(context.Background(), c.key(name))
	if gcerrors.Code(err) == gcerrors.NotFound {
		return pack.ErrFileNotFound
	}

	return err
}

// listPageSize is how many keys one listing request asks for, which is
// the most either S3 or Cloud Storage answers with.
const listPageSize = 1000

// pages walks a prefix a page at a time, because a listing is billed by
// the page rather than by the walk.
func (c *BucketStore) pages(prefix string, each func(*blob.ListObject) error) error {
	options := &blob.ListOptions{Prefix: prefix}

	for token := blob.FirstPageToken; ; {
		page, next, err := c.bucket.ListPage(context.Background(), token, listPageSize, options)
		c.count(OpList, 0)

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

// DeleteAll implements pack.ArchiveStorage.
func (c *BucketStore) DeleteAll() error {
	return c.pages(blobObjectPrefix, func(object *blob.ListObject) error {
		c.count(OpDelete, 0)

		return c.bucket.Delete(context.Background(), object.Key)
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
	}

	for _, option := range options {
		option(storage)
	}

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
