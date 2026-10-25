package backup

import (
	"context"
	"io"
	"os"
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

	partial, staged, err := r.stage(ctx, path)
	if err != nil {
		return 0, errors.Wrapf(err, "restoring %s", path)
	}
	defer staged()

	stream := r.newPartStream(ctx, reader)

	n, err := fsys.WriteFile(ctx, partial, stream)
	stream.close()

	if err == nil && n != reader.size() {
		err = errors.Errorf("wrote %d of %d bytes", n, reader.size())
	}

	var mode os.FileMode
	if err == nil {
		mode, err = r.ownFile(ctx, fsys, partial, stat)
	}

	if err == nil {
		err = r.putXattrs(ctx, fsys, partial, stat)
	}

	if err == nil {
		err = setMeta(ctx, fsys, partial, mode, mtimeOf(stat))
	}

	if err == nil {
		err = fsys.Rename(ctx, partial, path)
	}

	if err == nil {
		err = r.putFlags(ctx, fsys, path, stat)
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
	r       *Restorer
	reader  *fileReader
	ctx     context.Context
	ahead   *prefetch[streamed]
	pending []byte
	// zeros is how many zero bytes come before pending
	zeros int64

	mtx     sync.Mutex
	missing []int
}

type streamed struct {
	// zeros is the length of a hole, which has no data
	zeros   int64
	data    []byte
	counter *int64
	source  string
	err     error
}

func (r *Restorer) newPartStream(ctx context.Context, reader *fileReader) *partStream {
	s := &partStream{r: r, reader: reader, ctx: ctx}
	s.ahead = newPrefetch(ctx, 0, len(reader.parts), s.fetch)

	return s
}

func (s *partStream) fetch(ctx context.Context, i int) streamed {
	r, reader := s.r, s.reader
	part := reader.parts[i]

	if part.Hole > 0 {
		return streamed{zeros: int64(part.Length)}
	}

	if part.Ref == nil {
		return streamed{data: reader.inline, counter: &r.stats.BytesFromStore, source: "store"}
	}

	if data, counter, source, ok := r.localCopy(reader, i, nil); ok {
		return streamed{data: data, counter: counter, source: source}
	}

	obj, err := r.Store.Get(ctx, part.Ref)
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

// take waits for the next part, counts it as landed and makes it what
// the stream hands over next.
func (s *partStream) take() error {
	got, err := s.ahead.take()
	if err != nil {
		return err
	}

	if got.err != nil {
		return got.err
	}

	if got.counter != nil {
		s.r.countBytes(s.ctx, got.counter, got.source, int64(len(got.data)))
	}

	if got.zeros > 0 {
		s.r.countSparse(s.ctx, got.zeros)
	}

	s.zeros, s.pending = got.zeros, got.data

	return nil
}

// Read implements io.Reader.
func (s *partStream) Read(b []byte) (int, error) {
	for s.zeros == 0 && len(s.pending) == 0 {
		if err := s.take(); err != nil {
			return 0, err
		}
	}

	if s.zeros > 0 {
		n := int(min(int64(len(b)), s.zeros))
		clear(b[:n])
		s.zeros -= int64(n)

		return n, nil
	}

	n := copy(b, s.pending)
	s.pending = s.pending[n:]

	return n, nil
}

// WriteTo implements io.WriterTo.
func (s *partStream) WriteTo(w io.Writer) (int64, error) {
	var written int64

	for {
		if s.zeros == 0 && len(s.pending) == 0 {
			err := s.take()
			if err == io.EOF {
				return written, nil
			}

			if err != nil {
				return written, err
			}
		}

		n, err := writeZeros(w, s.zeros)
		written += n
		s.zeros -= n

		if err == nil {
			var m int
			m, err = w.Write(s.pending)
			written += int64(m)
			s.pending = s.pending[m:]
		}

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
	s.ahead.stop()
}
