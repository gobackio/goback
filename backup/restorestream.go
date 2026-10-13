package backup

import (
	"context"
	"io"
	"sort"
	"sync"

	"github.com/gobackio/goback/proto"

	"github.com/pkg/errors"
)

// streamFile writes a file front to back onto a tree that cannot write at
// offsets: into its partial file in one WriteFile, then renamed into place.
func (r *Restorer) streamFile(ctx context.Context, fsys WriteFS, path string, stat *proto.FileInfo, reader *fileReader) (Outcome, error) {
	if r.DryRun {
		r.countFile(ctx, OutcomeWouldWrite)
		return OutcomeWouldWrite, nil
	}

	partial := r.partial(path)
	stream := r.newPartStream(ctx, reader)

	n, err := fsys.WriteFile(ctx, partial, stream)
	stream.close()

	if err == nil && n != reader.size() {
		err = errors.Errorf("wrote %d of %d bytes", n, reader.size())
	}

	if err == nil {
		err = setMeta(ctx, fsys, partial, fileMode(stat), mtimeOf(stat))
	}

	if err == nil {
		err = fsys.Rename(ctx, partial, path)
	}

	if err != nil {
		return 0, errors.Wrapf(err, "restoring %s", path)
	}

	if missing := stream.holes(); len(missing) > 0 {
		r.reportHoles(path, reader, missing)
		r.countFile(ctx, OutcomeSalvaged)

		return OutcomeSalvaged, nil
	}

	r.countFile(ctx, OutcomeWritten)

	return OutcomeWritten, nil
}

// partStream reads a file's parts in order while it fetches up to
// readWindow of the next ones, each from the seeds or the cache before the
// store. Read and WriteTo both read ahead.
type partStream struct {
	r      *Restorer
	reader *fileReader
	ctx    context.Context
	cancel context.CancelFunc

	// fetched hands over each part's result in file order
	fetched chan chan streamed
	window  chan struct{}

	next    int
	pending []byte

	mtx     sync.Mutex
	missing []int
}

type streamed struct {
	data    []byte
	counter *int64
	source  string
	err     error
}

func (r *Restorer) newPartStream(ctx context.Context, reader *fileReader) *partStream {
	ctx, cancel := context.WithCancel(ctx)

	s := &partStream{
		r: r, reader: reader, ctx: ctx, cancel: cancel,
		fetched: make(chan chan streamed, readWindow),
		window:  make(chan struct{}, readWindow),
	}

	go s.fetchAll()

	return s
}

func (s *partStream) fetchAll() {
	defer close(s.fetched)

	for i := range s.reader.parts {
		select {
		case s.window <- struct{}{}:
		case <-s.ctx.Done():
			return
		}

		result := make(chan streamed, 1)
		s.fetched <- result

		go func() { result <- s.fetch(i) }()
	}
}

func (s *partStream) fetch(i int) streamed {
	r, reader := s.r, s.reader
	part := reader.parts[i]

	if part.Ref == nil {
		return streamed{data: reader.inline, counter: &r.stats.BytesFromStore, source: "store"}
	}

	if data, counter, source, ok := r.localCopy(reader, i, nil); ok {
		return streamed{data: data, counter: counter, source: source}
	}

	obj, err := r.Store.Get(s.ctx, part.Ref)
	if err != nil {
		if r.Salvage && errors.Is(err, ErrNotFound) {
			s.mtx.Lock()
			s.missing = append(s.missing, i)
			s.mtx.Unlock()

			return streamed{data: make([]byte, part.Length)}
		}

		return streamed{err: errors.Wrapf(err, "part %d (%x) of file %x", i, part.Ref.Hash, reader.fileRef())}
	}

	data, err := reader.openPart(i, part, obj)
	if err != nil {
		return streamed{err: err}
	}

	if r.Cache != nil {
		_ = r.Cache.Put(part.Ref, obj)
	}

	return streamed{data: data, counter: &r.stats.BytesFromStore, source: "store"}
}

// take waits for the next part, counts it as landed and frees its place
// in the window.
func (s *partStream) take() ([]byte, error) {
	if s.next == len(s.reader.parts) {
		return nil, io.EOF
	}

	var result chan streamed

	select {
	case result = <-s.fetched:
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}

	var got streamed

	select {
	case got = <-result:
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}

	<-s.window
	s.next++

	if got.err != nil {
		return nil, got.err
	}

	if got.counter != nil {
		s.r.countBytes(s.ctx, got.counter, got.source, int64(len(got.data)))
	}

	return got.data, nil
}

// Read implements io.Reader.
func (s *partStream) Read(b []byte) (int, error) {
	for len(s.pending) == 0 {
		data, err := s.take()
		if err != nil {
			return 0, err
		}

		s.pending = data
	}

	n := copy(b, s.pending)
	s.pending = s.pending[n:]

	return n, nil
}

// WriteTo implements io.WriterTo.
func (s *partStream) WriteTo(w io.Writer) (int64, error) {
	var written int64

	for {
		data := s.pending
		s.pending = nil

		if len(data) == 0 {
			var err error

			data, err = s.take()
			if err == io.EOF {
				return written, nil
			}

			if err != nil {
				return written, err
			}
		}

		n, err := w.Write(data)
		written += int64(n)

		if err != nil {
			return written, err
		}
	}
}

// holes are the parts no source held, which the stream wrote as zeroes.
func (s *partStream) holes() []int {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	sort.Ints(s.missing)

	return s.missing
}

// close stops fetching what was not read.
func (s *partStream) close() {
	s.cancel()
}
