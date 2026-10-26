package pack

import (
	"bytes"
	"io/fs"
	"strings"
	"sync"
	"time"
)

var _ ArchiveStorage = (*memView)(nil)

// memBucket is a bucket in memory: a file appears whole when its writer
// closes it, stamped newer than every file before it, and a reader keeps
// the bytes it opened after the file is deleted.
type memBucket struct {
	mtx   sync.Mutex
	files map[string]memObject
	last  time.Time
}

type memObject struct {
	data    []byte
	created time.Time
}

func newMemBucket() *memBucket {
	return &memBucket{files: make(map[string]memObject)}
}

func (b *memBucket) stamp() time.Time {
	now := time.Now().Truncate(time.Microsecond)
	if !now.After(b.last) {
		now = b.last.Add(time.Microsecond)
	}
	b.last = now

	return now
}

// view is one process's handle on the bucket: like BucketStore, it
// answers Open of a file it is still writing with that writer.
func (b *memBucket) view() *memView {
	return &memView{memBucket: b, writing: make(map[string]*memWriter)}
}

type memView struct {
	*memBucket

	writingMtx sync.Mutex
	writing    map[string]*memWriter
}

func (v *memView) Create(name string) (File, error) {
	w := &memWriter{view: v, name: name}

	v.writingMtx.Lock()
	v.writing[name] = w
	v.writingMtx.Unlock()

	return w, nil
}

func (v *memView) Open(name string) (File, error) {
	v.writingMtx.Lock()
	w, ok := v.writing[name]
	v.writingMtx.Unlock()

	if ok {
		return w, nil
	}

	return v.memBucket.Open(name)
}

func (b *memBucket) CreateNew(name string, data []byte) error {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if _, ok := b.files[name]; ok {
		return ErrFileExists
	}

	b.files[name] = memObject{data: bytes.Clone(data), created: b.stamp()}

	return nil
}

func (b *memBucket) Open(name string) (File, error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	obj, ok := b.files[name]
	if !ok {
		return nil, ErrFileNotFound
	}

	return &memReader{Reader: bytes.NewReader(obj.data), info: memInfo{name: name, obj: obj}}, nil
}

func (b *memBucket) List(extension string) ([]string, error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	var names []string
	for name := range b.files {
		if strings.HasSuffix(name, extension) {
			names = append(names, name)
		}
	}

	return names, nil
}

func (b *memBucket) ListInfo(extension string) ([]ListedFile, error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	var files []ListedFile
	for name, obj := range b.files {
		if strings.HasSuffix(name, extension) {
			files = append(files, ListedFile{Name: name, Size: int64(len(obj.data)), Modified: obj.created})
		}
	}

	return files, nil
}

func (b *memBucket) Delete(name string) error {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if _, ok := b.files[name]; !ok {
		return ErrFileNotFound
	}

	delete(b.files, name)

	return nil
}

// Move stamps the moved file anew, as a copy within a bucket is.
func (b *memBucket) Move(from, to string) error {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	obj, ok := b.files[from]
	if !ok {
		return ErrFileNotFound
	}

	delete(b.files, from)
	b.files[to] = memObject{data: obj.data, created: b.stamp()}

	return nil
}

func (b *memBucket) DeleteAll() error {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	b.files = make(map[string]memObject)

	return nil
}

type memWriter struct {
	view *memView
	name string
	buf  bytes.Buffer
}

func (w *memWriter) Write(p []byte) (int, error) { return w.buf.Write(p) }

func (w *memWriter) Read([]byte) (int, error) { return 0, fs.ErrInvalid }

func (w *memWriter) Seek(int64, int) (int64, error) { return 0, fs.ErrInvalid }

func (w *memWriter) Stat() (fs.FileInfo, error) {
	return memInfo{name: w.name, obj: memObject{data: w.buf.Bytes()}}, nil
}

func (w *memWriter) Close() error {
	w.view.writingMtx.Lock()
	if w.view.writing[w.name] != w {
		w.view.writingMtx.Unlock()
		return nil
	}
	delete(w.view.writing, w.name)
	w.view.writingMtx.Unlock()

	b := w.view.memBucket
	b.mtx.Lock()
	defer b.mtx.Unlock()

	b.files[w.name] = memObject{data: bytes.Clone(w.buf.Bytes()), created: b.stamp()}

	return nil
}

type memReader struct {
	*bytes.Reader
	info memInfo
}

func (r *memReader) Write([]byte) (int, error) { return 0, fs.ErrInvalid }

func (r *memReader) Stat() (fs.FileInfo, error) { return r.info, nil }

func (r *memReader) Close() error { return nil }

type memInfo struct {
	name string
	obj  memObject
}

func (i memInfo) Name() string       { return i.name }
func (i memInfo) Size() int64        { return int64(len(i.obj.data)) }
func (i memInfo) Mode() fs.FileMode  { return 0o644 }
func (i memInfo) ModTime() time.Time { return i.obj.created }
func (i memInfo) IsDir() bool        { return false }
func (i memInfo) Sys() any           { return nil }
