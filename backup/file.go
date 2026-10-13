package backup

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/gobackio/goback/backup/blobcache"
	"github.com/gobackio/goback/backup/chunker"
	"github.com/gobackio/goback/backup/presence"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"

	"github.com/pkg/errors"
	"go4.org/syncutil"
	"golang.org/x/sync/errgroup"
)

const (
	maxBlobSize = chunker.MaxSize

	// SplitFileSize is the smallest content size whose file object may be
	// split into sub-file objects.
	SplitFileSize = 4096 * chunker.MinSize

	inFlightChunks = 80

	// readWindow bounds the parts a streaming read holds in memory
	readWindow = 32
)

// newFileWriter cuts a file of the given size into blobs. With a key the
// blobs and any inline content are sealed under it.
func newFileWriter(ctx context.Context, store ObjectStore, key *storekey.Key, size int64) *fileWriter {
	return &fileWriter{
		store:            store,
		key:              key,
		size:             size,
		parts:            make([]*proto.FilePart, 0),
		assumed:          make(map[string]*proto.FilePart),
		storageErr:       new(atomic.Value),
		storageSemaphore: syncutil.NewGate(inFlightChunks),
		ctx:              ctx,
	}
}

// PutFile stores size bytes of content as a file, cut and sealed under key
// as a backup would, and returns the ref a tree node should carry.
func PutFile(ctx context.Context, store ObjectStore, key *storekey.Key, size int64, content io.Reader) (*proto.Ref, error) {
	writer := newFileWriter(ctx, store, key, size)

	if _, err := io.Copy(writer, content); err != nil {
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	return writer.Ref(), nil
}

type fileWriter struct {
	store            ObjectStore
	key              *storekey.Key
	size             int64
	buf              [maxBlobSize]byte
	blobSize         int
	offset           int64
	chunker          chunker.FastCDC
	parts            []*proto.FilePart
	storageErr       *atomic.Value
	storageGroup     syncutil.Group
	storageSemaphore *syncutil.Gate
	ref              *proto.Ref
	ctx              context.Context

	// known holds blob refs the previous version of the file had, which
	// are not uploaded
	known map[string]struct{}

	// sent holds the refs uploaded so far in this run, shared across files
	sent *sentSet

	// filters say which refs the store probably holds; only consulted when
	// a confirmer can check the assumption
	filters   presence.Set
	confirmer Confirmer

	// forced refs are uploaded whatever known and the filters say
	forced map[string]struct{}

	// assumed are the parts skipped on the strength of known or a filter,
	// by ref, until the confirmer answers for them
	assumed  map[string]*proto.FilePart
	window   *chunkWindow
	source   io.ReaderAt
	repaired int

	// waits are uploads other files of the run own that this file's parts
	// depend on
	waits []*sentEntry

	// cache, when set, receives every blob this file uploads
	cache *blobcache.Cache

	// uploaded, when set, counts the chunk bytes this file sends
	uploaded *int64

	pending int32
}

// fileChangedError reports parts whose content on disk no longer hashes
// to the ref the store lacks, so the file has to be read again.
type fileChangedError struct {
	refs []*proto.Ref
}

func (e *fileChangedError) Error() string {
	return fmt.Sprintf("%d parts changed on disk before the store confirmed them", len(e.refs))
}

// sentSet remembers the refs one backup run uploads, so a chunk that
// appears in several files goes up once: the first writer claims it and
// the others wait for that upload.
type sentSet struct {
	mtx  sync.Mutex
	refs map[string]*sentEntry
}

type sentEntry struct {
	done chan struct{}
	err  error
}

func newSentSet() *sentSet {
	return &sentSet{refs: make(map[string]*sentEntry)}
}

// claim reports whether the caller has to upload the ref; when not, the
// entry says when the upload that was claimed earlier has finished.
func (s *sentSet) claim(ref *proto.Ref) (*sentEntry, bool) {
	if s == nil {
		return nil, true
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	if entry, ok := s.refs[string(ref.Hash)]; ok {
		return entry, false
	}

	s.refs[string(ref.Hash)] = &sentEntry{done: make(chan struct{})}

	return nil, true
}

// finish records the outcome of a claimed upload.
func (s *sentSet) finish(ref *proto.Ref, err error) {
	if s == nil {
		return
	}

	s.mtx.Lock()
	entry := s.refs[string(ref.Hash)]
	if err != nil {
		delete(s.refs, string(ref.Hash))
	}
	s.mtx.Unlock()

	if entry != nil {
		entry.err = err
		close(entry.done)
	}
}

func (e *sentEntry) wait() error {
	<-e.done

	return e.err
}

// split emits buf[:length] as a blob and uploads it in the background.
func (bfw *fileWriter) split(length int) {
	chunkBytes := make([]byte, length)
	copy(chunkBytes, bfw.buf[:length])

	blob, ref := bfw.seal(chunkBytes)

	part := &proto.FilePart{
		Ref:    ref,
		Offset: uint64(bfw.offset),
		Length: uint64(length),
	}

	bfw.parts = append(bfw.parts, part)
	bfw.offset += int64(length)

	key := string(ref.Hash)
	if _, forced := bfw.forced[key]; !forced {
		_, known := bfw.known[key]
		if known || (bfw.confirmer != nil && bfw.filters.Test(ref.Hash)) {
			if bfw.confirmer != nil {
				bfw.assumed[key] = part
				bfw.window.put(key, blob, length)
			}

			return
		}
	}

	bfw.upload(ref, blob, length)
}

// seal turns a chunk into the blob object that is stored for it, plain or
// sealed under the store key, and returns its ref.
func (bfw *fileWriter) seal(chunk []byte) (*proto.Object, *proto.Ref) {
	if bfw.key == nil {
		blob := proto.NewObject(&proto.Blob{Data: chunk})
		return blob, blob.Ref()
	}

	sealed := SealBlob(bfw.key, chunk)

	return proto.NewObject(sealed), sealed.Ref
}

// upload stores a blob in the background unless another file of the run
// is already uploading it, in which case that upload is awaited at Close.
func (bfw *fileWriter) upload(ref *proto.Ref, blob *proto.Object, length int) {
	entry, mine := bfw.sent.claim(ref)
	if !mine {
		bfw.waits = append(bfw.waits, entry)
		return
	}

	bfw.storageSemaphore.Start()
	atomic.AddInt32(&bfw.pending, 1)

	bfw.storageGroup.Go(func() error {
		defer bfw.storageSemaphore.Done()
		defer atomic.AddInt32(&bfw.pending, -1)

		err := bfw.store.Put(bfw.ctx, blob)
		bfw.sent.finish(ref, err)

		if err == nil && bfw.uploaded != nil {
			atomic.AddInt64(bfw.uploaded, int64(length))
		}

		if err == nil && bfw.cache != nil {
			_ = bfw.cache.Put(ref, blob)
		}

		if err != nil {
			// store the error here so future calls to write can exit early
			bfw.storageErr.Store(err)
		}
		return err
	})
}

// settle waits for this file's uploads and for the ones it shares with
// other files.
func (bfw *fileWriter) settle() error {
	err := bfw.storageGroup.Err()
	if err != nil {
		return err
	}

	for _, entry := range bfw.waits {
		if err := entry.wait(); err != nil {
			return err
		}
	}

	bfw.waits = nil

	return nil
}

// assumedRefs lists the parts among the given ones the store still has to
// confirm.
func (bfw *fileWriter) assumedRefs(parts []*proto.FilePart) []*proto.Ref {
	var refs []*proto.Ref
	for _, part := range parts {
		if _, ok := bfw.assumed[string(part.Ref.Hash)]; ok {
			refs = append(refs, part.Ref)
		}
	}

	return refs
}

// storeFile stores a file object over parts, first having the store confirm
// the parts that were not uploaded and repairing the ones it lacks.
func (bfw *fileWriter) storeFile(obj *proto.Object, parts []*proto.FilePart) error {
	assumed := bfw.assumedRefs(parts)
	if bfw.confirmer == nil || len(assumed) == 0 {
		return bfw.store.Put(bfw.ctx, obj)
	}

	keys := make([]string, len(assumed))
	for i, ref := range assumed {
		keys[i] = string(ref.Hash)
	}
	defer bfw.window.drop(keys)

	for attempt := 0; ; attempt++ {
		missing, err := bfw.confirmer.PutFile(bfw.ctx, obj, assumed)
		if err != nil {
			return err
		}

		if len(missing) == 0 {
			return nil
		}

		if attempt > 0 {
			return fmt.Errorf("store still lacks %d parts after they were uploaded", len(missing))
		}

		err = bfw.repair(missing)
		if err != nil {
			return err
		}
	}
}

// repair uploads assumed parts the store lacks, from the window or from
// the file, and fails with fileChangedError for parts the file no longer
// contains.
func (bfw *fileWriter) repair(missing []*proto.Ref) error {
	var changed []*proto.Ref

	for _, ref := range missing {
		key := string(ref.Hash)
		part := bfw.assumed[key]

		blob := bfw.window.take(key)
		if blob == nil {
			if part == nil || bfw.source == nil {
				changed = append(changed, ref)
				continue
			}

			chunk := make([]byte, part.Length)
			_, err := bfw.source.ReadAt(chunk, int64(part.Offset))
			if err != nil {
				changed = append(changed, ref)
				continue
			}

			var current *proto.Ref
			blob, current = bfw.seal(chunk)
			if !current.Equal(ref) {
				changed = append(changed, ref)
				continue
			}
		}

		bfw.upload(ref, blob, int(part.GetLength()))
		bfw.repaired++
	}

	err := bfw.settle()
	if err != nil {
		return err
	}

	if len(changed) > 0 {
		return &fileChangedError{refs: changed}
	}

	return nil
}

func (bfw *fileWriter) Ref() *proto.Ref {
	return bfw.ref
}

func (bfw *fileWriter) Write(p []byte) (int, error) {
	deferred := bfw.storageErr.Load()

	if deferred != nil {
		return 0, deferred.(error)
	}

	written := len(p)

	for len(p) > 0 {
		// a cut is forced at maxBlobSize, so there is always room
		n := copy(bfw.buf[bfw.blobSize:], p)
		p = p[n:]

		scanned := bfw.blobSize
		bfw.blobSize += n

		for {
			cut := bfw.chunker.Scan(bfw.buf[:bfw.blobSize], scanned)
			if cut == 0 {
				break
			}

			rest := bfw.blobSize - cut
			bfw.split(cut)
			copy(bfw.buf[:rest], bfw.buf[cut:bfw.blobSize])
			bfw.blobSize = rest
			scanned = 0
		}
	}

	return written, nil
}

func (bfw *fileWriter) object(file *proto.File) *proto.Object {
	obj := proto.NewObject(file)
	if bfw.key != nil {
		obj.KeyId = bfw.key.ID()
	}

	return obj
}

// inline is the file as one object holding its content, or nil when the
// content, sealed if there is a key, is too large for that.
func (bfw *fileWriter) inline() *proto.File {
	if len(bfw.parts) > 0 || bfw.blobSize > proto.InlineLimit {
		return nil
	}

	content := make([]byte, bfw.blobSize)
	copy(content, bfw.buf[:bfw.blobSize])

	file := &proto.File{Inline: content}
	if bfw.key != nil {
		file.Inline = bfw.key.SealInline(content)
		file.InlineEncryption = proto.Encryption_SEALED
	}

	if len(file.Inline) > proto.InlineLimit {
		return nil
	}

	return file
}

func (bfw *fileWriter) Close() (err error) {
	if file := bfw.inline(); file != nil {
		obj := bfw.object(file)
		bfw.ref = obj.Ref()

		err = bfw.store.Put(bfw.ctx, obj)
		if err == nil && bfw.uploaded != nil {
			atomic.AddInt64(bfw.uploaded, int64(bfw.blobSize))
		}

		return err
	}

	if bfw.blobSize > 0 {
		bfw.split(bfw.blobSize)
		bfw.blobSize = 0
		bfw.chunker.Reset()
	}

	// the store checks a file's parts, so the uploads must land first
	if err = bfw.settle(); err != nil {
		return err
	}

	return bfw.putParts()
}

// PutParts stores a file over parts that store already holds, as a backup
// that cut those parts would, and returns the ref a tree node should
// carry. parts must not be empty; an empty file is stored with PutFile.
func PutParts(ctx context.Context, store ObjectStore, key *storekey.Key, parts []*proto.FilePart) (*proto.Ref, error) {
	writer := newFileWriter(ctx, store, key, 0)
	writer.parts = parts

	if err := writer.putParts(); err != nil {
		return nil, err
	}

	return writer.Ref(), nil
}

// putParts stores the file object over the parts, as a tree of sub-file
// objects when there are too many.
func (bfw *fileWriter) putParts() error {
	if uint64(len(bfw.parts)) <= splitMax {
		file := bfw.object(&proto.File{Parts: bfw.parts})
		bfw.ref = file.Ref()

		return bfw.storeFile(file, bfw.parts)
	}

	var level []splitEntry

	for _, run := range cutRuns(len(bfw.parts), func(i int) *proto.Ref { return bfw.parts[i].Ref }) {
		parts := rebased(bfw.parts[run[0]:run[1]], -int64(bfw.parts[run[0]].Offset))

		leaf := bfw.object(&proto.File{Parts: parts})
		if err := bfw.storeFile(leaf, parts); err != nil {
			return err
		}

		last := parts[len(parts)-1]
		level = append(level, splitEntry{ref: leaf.Ref(), length: last.Offset + last.Length})
	}

	for depth := uint32(1); ; depth++ {
		if uint64(len(level)) <= splitMax {
			file := bfw.object(splitFile(level, depth))
			bfw.ref = file.Ref()

			return bfw.store.Put(bfw.ctx, file)
		}

		var up []splitEntry

		for _, run := range cutRuns(len(level), func(i int) *proto.Ref { return level[i].ref }) {
			entries := level[run[0]:run[1]]

			node := bfw.object(splitFile(entries, depth))
			if err := bfw.store.Put(bfw.ctx, node); err != nil {
				return err
			}

			var length uint64
			for _, entry := range entries {
				length += entry.length
			}

			up = append(up, splitEntry{ref: node.Ref(), length: length})
		}

		level = up
	}
}

// A file of more parts than splitMax is held by a tree of file objects.
// Each level is cut where an entry's ref hashes to a boundary, so an edit
// leaves the file objects around it as they were. Tests shrink the bounds.
var (
	splitMin uint64 = 256
	splitAvg uint64 = 1024
	splitMax uint64 = SplitFileSize / chunker.MinSize
)

// rebased copies parts with their offsets moved by delta. The leaves of a
// measured split file count from their own start, so the same run of
// parts is the same object wherever in the file it lands.
func rebased(parts []*proto.FilePart, delta int64) []*proto.FilePart {
	out := make([]*proto.FilePart, len(parts))
	for i, part := range parts {
		out[i] = &proto.FilePart{Offset: uint64(int64(part.Offset) + delta), Length: part.Length, Ref: part.Ref}
	}

	return out
}

// splitEntry is a sub-file object and the content bytes it holds.
type splitEntry struct {
	ref    *proto.Ref
	length uint64
}

func splitFile(entries []splitEntry, depth uint32) *proto.File {
	file := &proto.File{SplitDepth: depth}
	for _, entry := range entries {
		file.Splits = append(file.Splits, entry.ref)
		file.SplitLengths = append(file.SplitLengths, entry.length)
	}

	return file
}

// cutRuns cuts n entries into runs of splitMin to splitMax, ending a run
// at an entry whose ref is a boundary, and returns each run as [start, end).
func cutRuns(n int, ref func(i int) *proto.Ref) [][2]int {
	var (
		runs  [][2]int
		start int
	)

	for i := range n {
		size := uint64(i - start + 1)
		if size < splitMin {
			continue
		}

		if size >= splitMax || splitBoundary(ref(i)) {
			runs = append(runs, [2]int{start, i + 1})
			start = i + 1
		}
	}

	if start < n {
		runs = append(runs, [2]int{start, n})
	}

	return runs
}

func splitBoundary(ref *proto.Ref) bool {
	h := fnv.New64a()
	h.Write(ref.GetHash())

	return h.Sum64()%splitAvg == 0
}

var _ io.WriteCloser = new(fileWriter)

// FileParts returns a file's parts with its splits flattened, in order;
// an inline file has none.
func FileParts(ctx context.Context, store Getter, file *proto.File) ([]*proto.FilePart, error) {
	if len(file.GetSplits()) == 0 {
		return file.GetParts(), nil
	}

	subFiles := make([]*proto.File, len(file.GetSplits()))
	grp, grpCtx := errgroup.WithContext(ctx)
	grp.SetLimit(splitFetchers)

	for i, split := range file.GetSplits() {
		grp.Go(func() error {
			sub, err := loadSplit(grpCtx, store, i, split)
			subFiles[i] = sub

			return err
		})
	}

	if err := grp.Wait(); err != nil {
		return nil, err
	}

	var (
		parts []*proto.FilePart
		start uint64
	)

	for i, sub := range subFiles {
		held, err := FileParts(ctx, store, sub)
		if err != nil {
			return nil, err
		}

		if file.GetSplitDepth() > 0 {
			held = rebased(held, int64(start))
			start += file.GetSplitLengths()[i]
		}

		parts = append(parts, held...)
	}

	return parts, nil
}

// splitFetchers bounds the sub-file objects loaded at once.
const splitFetchers = 16

func loadSplit(ctx context.Context, store Getter, i int, split *proto.Ref) (*proto.File, error) {
	obj, err := store.Get(ctx, split)
	if err != nil {
		return nil, errors.Wrapf(err, "split %d (%x) of file", i, split.GetHash())
	}

	if obj.GetFile() == nil {
		return nil, errors.Errorf("split %d (%x) of file is not a file object", i, split.GetHash())
	}

	return obj.GetFile(), nil
}

// SubFiles calls visit with every file object of a split file's tree
// below file, loading only the ones that are split themselves.
func SubFiles(ctx context.Context, store Getter, file *proto.File, visit func(ref *proto.Ref) error) error {
	for i, split := range file.GetSplits() {
		if err := visit(split); err != nil {
			return err
		}

		if file.GetSplitDepth() <= 1 {
			continue
		}

		sub, err := loadSplit(ctx, store, i, split)
		if err != nil {
			return err
		}

		if err := SubFiles(ctx, store, sub, visit); err != nil {
			return err
		}
	}

	return nil
}

func newFileReader(ctx context.Context, store ObjectStore, file *proto.File, key *storekey.Key) *fileReader {
	return &fileReader{
		store: store,
		file:  file,
		key:   key,
		ctx:   ctx,
		total: -1,
	}
}

type fileReader struct {
	store ObjectStore
	file  *proto.File
	key   *storekey.Key
	// all is every part, once something needed them all
	all    []*proto.FilePart
	inline []byte
	// parts is the run of parts a read is in: one sub-file object's of a
	// measured split file, all of them otherwise
	parts     []*proto.FilePart
	partIndex int
	blob      *proto.Object
	offset    int64
	total     int64
	ctx       context.Context
}

func (bfr *fileReader) search(index int) bool {
	part := bfr.parts[index]

	return bfr.offset <= int64(part.Offset+part.Length-1)
}

// size is the file's length, known once measure or getFileParts ran.
func (bfr *fileReader) size() int64 {
	return bfr.total
}

// measure learns the file's length, without loading any sub-file object
// when the file records their lengths.
func (bfr *fileReader) measure(ctx context.Context) error {
	if bfr.total >= 0 {
		return nil
	}

	if bfr.file.GetSplitDepth() > 0 {
		var total uint64
		for _, length := range bfr.file.GetSplitLengths() {
			total += length
		}

		bfr.total = int64(total)

		return nil
	}

	_, err := bfr.getFileParts(ctx)

	return err
}

// locate makes parts the run holding offset, loading the one sub-file
// object on the way down to it.
func (bfr *fileReader) locate(ctx context.Context) error {
	if bfr.partIndex < len(bfr.parts) {
		part := bfr.parts[bfr.partIndex]
		if bfr.offset >= int64(part.Offset) && bfr.offset < int64(part.Offset+part.Length) {
			return nil
		}
	}

	bfr.blob = nil

	if !runHolds(bfr.parts, bfr.offset) {
		parts, err := bfr.partsAt(ctx, uint64(bfr.offset))
		if err != nil {
			return err
		}

		bfr.parts = parts
	}

	bfr.partIndex = sort.Search(len(bfr.parts), bfr.search)

	return nil
}

func runHolds(parts []*proto.FilePart, offset int64) bool {
	if len(parts) == 0 {
		return false
	}

	last := parts[len(parts)-1]

	return offset >= int64(parts[0].Offset) && offset < int64(last.Offset+last.Length)
}

func (bfr *fileReader) partsAt(ctx context.Context, offset uint64) ([]*proto.FilePart, error) {
	if bfr.file.GetSplitDepth() == 0 {
		return bfr.getFileParts(ctx)
	}

	file := bfr.file
	var start uint64

	for file.GetSplitDepth() > 0 {
		lengths := file.GetSplitLengths()

		i := 0
		for ; i < len(lengths)-1 && offset >= start+lengths[i]; i++ {
			start += lengths[i]
		}

		sub, err := loadSplit(ctx, bfr.store, i, file.GetSplits()[i])
		if err != nil {
			return nil, errors.Wrapf(err, "file %x", bfr.fileRef())
		}

		file = sub
	}

	return rebased(file.GetParts(), int64(start)), nil
}

type partResponse struct {
	index int
	blob  *proto.Blob
}

type partRequest struct {
	index int
	part  *proto.FilePart
}

func (bfr *fileReader) fileRef() []byte {
	return proto.NewObject(bfr.file).Ref().Hash
}

// getPart fetches one part's blob and fails on a missing or mistyped object.
// Inline content is a single part without a ref.
func (bfr *fileReader) getPart(ctx context.Context, index int, part *proto.FilePart) (*proto.Blob, error) {
	if part.Ref == nil {
		return &proto.Blob{Data: bfr.inline}, nil
	}

	obj, err := bfr.store.Get(ctx, part.Ref)
	if err != nil {
		return nil, errors.Wrapf(err, "part %d (%x) of file %x", index, part.Ref.Hash, bfr.fileRef())
	}

	data, err := bfr.openPart(index, part, obj)
	if err != nil {
		return nil, err
	}

	return &proto.Blob{Data: data}, nil
}

// openPart returns the plaintext of a part's stored object.
func (bfr *fileReader) openPart(index int, part *proto.FilePart, obj *proto.Object) ([]byte, error) {
	var data []byte
	var err error

	switch {
	case obj.GetSealed() != nil:
		if !obj.GetSealed().GetRef().Equal(part.Ref) {
			return nil, errors.Errorf("part %d (%x) of file %x is sealed as %x", index, part.Ref.Hash, bfr.fileRef(), obj.GetSealed().GetRef().GetHash())
		}

		if bfr.key == nil {
			return nil, errors.Wrapf(storekey.ErrNoKey, "part %d (%x) of file %x", index, part.Ref.Hash, bfr.fileRef())
		}

		data, err = OpenBlob(bfr.key, obj.GetSealed())
		if err != nil {
			return nil, errors.Wrapf(err, "part %d (%x) of file %x", index, part.Ref.Hash, bfr.fileRef())
		}
	case obj.GetBlob() != nil:
		if !proto.NewObject(obj.GetBlob()).Ref().Equal(part.Ref) {
			return nil, errors.Errorf("part %d (%x) of file %x does not hash to its ref", index, part.Ref.Hash, bfr.fileRef())
		}

		data = obj.GetBlob().Data
	default:
		return nil, errors.Errorf("part %d (%x) of file %x is not a blob", index, part.Ref.Hash, bfr.fileRef())
	}

	if uint64(len(data)) != part.Length {
		return nil, errors.Errorf("part %d (%x) of file %x has %d bytes, expected %d", index, part.Ref.Hash, bfr.fileRef(), len(data), part.Length)
	}

	return data, nil
}

// getFileParts loads every part of the file and makes them the run reads
// work through.
func (bfr *fileReader) getFileParts(ctx context.Context) ([]*proto.FilePart, error) {
	if bfr.all != nil {
		return bfr.all, nil
	}

	parts, err := bfr.loadParts(ctx)
	if err != nil {
		return nil, err
	}

	bfr.all = parts
	bfr.parts = parts
	bfr.partIndex = 0
	bfr.blob = nil
	bfr.total = 0

	if len(parts) > 0 {
		last := parts[len(parts)-1]
		bfr.total = int64(last.Offset + last.Length)
	}

	return parts, nil
}

func (bfr *fileReader) loadParts(ctx context.Context) ([]*proto.FilePart, error) {

	switch {
	case len(bfr.file.Inline) > 0:
		bfr.inline = bfr.file.Inline

		if bfr.file.InlineEncryption == proto.Encryption_SEALED {
			if bfr.key == nil {
				return nil, errors.Wrapf(storekey.ErrNoKey, "inline content of file %x", bfr.fileRef())
			}

			inline, err := bfr.key.OpenInline(bfr.file.Inline)
			if err != nil {
				return nil, errors.Wrapf(err, "inline content of file %x", bfr.fileRef())
			}

			bfr.inline = inline
		}

		return []*proto.FilePart{{Offset: 0, Length: uint64(len(bfr.inline))}}, nil
	case len(bfr.file.Splits) > 0:
		return FileParts(ctx, bfr.store, bfr.file)
	case bfr.file.Parts != nil:
		return bfr.file.Parts, nil
	default:
		return []*proto.FilePart{}, nil
	}
}

// WriteTo streams the file from the read offset on, fetching up to
// readWindow parts ahead of the writer, so memory stays bounded whatever
// the part latency.
func (bfr *fileReader) WriteTo(writer io.Writer) (int64, error) {
	fileParts, err := bfr.getFileParts(bfr.ctx)
	if err != nil {
		return 0, err
	}

	bfr.blob = nil
	first := sort.Search(len(fileParts), func(i int) bool {
		return bfr.offset < int64(fileParts[i].Offset+fileParts[i].Length)
	})

	type fetched struct {
		blob *proto.Blob
		err  error
	}

	ahead := newPrefetch(bfr.ctx, first, len(fileParts), func(ctx context.Context, i int) fetched {
		blob, err := bfr.getPart(ctx, i, fileParts[i])
		return fetched{blob, err}
	})
	defer ahead.stop()

	var written int64

	for i := first; i < len(fileParts); i++ {
		got, err := ahead.take()
		if err == nil {
			err = got.err
		}

		if err != nil {
			return written, err
		}

		data := got.blob.Data[bfr.offset-int64(fileParts[i].Offset):]

		n, err := writer.Write(data)
		written += int64(n)
		bfr.offset += int64(n)

		if err != nil {
			return written, err
		}
	}

	return written, nil
}

func (bfr *fileReader) Read(b []byte) (n int, err error) {
	if err := bfr.measure(bfr.ctx); err != nil {
		return 0, err
	}

	if bfr.offset >= bfr.size() {
		return 0, io.EOF
	}

	if err := bfr.locate(bfr.ctx); err != nil {
		return 0, err
	}

	n = len(b)

	part := bfr.parts[bfr.partIndex]

	relativeOffset := bfr.offset - int64(part.Offset)

	bytesRemaining := int64(part.Length) - relativeOffset

	if n == 0 {
		return 0, ErrEmptyBuffer
	}

	// a read stops at the part boundary and returns short
	if n > int(bytesRemaining) {
		n = int(bytesRemaining)
	}

	if bfr.blob == nil {
		blob, err := bfr.getPart(bfr.ctx, bfr.partIndex, part)
		if err != nil {
			return 0, err
		}

		bfr.blob = proto.NewObject(blob)
	}

	copy(b, bfr.blob.GetBlob().Data[relativeOffset:relativeOffset+int64(n)])

	if relativeOffset+int64(n) == int64(part.Length) {
		bfr.blob = nil
		bfr.partIndex++
	}

	bfr.offset += int64(n)

	return
}

func (bfr *fileReader) Seek(offset int64, whence int) (int64, error) {
	if err := bfr.measure(bfr.ctx); err != nil {
		return 0, err
	}

	switch whence {
	case io.SeekStart:
		bfr.offset = offset
	case io.SeekCurrent:
		bfr.offset += offset
	case io.SeekEnd:
		bfr.offset = bfr.size() + offset
	default:
		return bfr.offset, errors.New("invalid whence value")
	}

	if bfr.offset < 0 || bfr.offset > bfr.size() {
		return bfr.offset, ErrIllegalOffset
	}

	return bfr.offset, nil
}

var _ io.ReadSeeker = (*fileReader)(nil)
