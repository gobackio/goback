package backup

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"os"
	"sync"
	"sync/atomic"

	"github.com/gobackio/goback/proto"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

const (
	// smallFile is the size below which RestoreFiles reads a file together
	// with others; groupBytes bounds the content of one group.
	smallFile  = 1 << 20
	groupBytes = 64 << 20
)

// FileToRestore is one file of a RestoreFiles call.
type FileToRestore struct {
	Path string
	Stat *proto.FileInfo
	Ref  *proto.Ref
	// Written, when set, is handed the size of each part as it lands in
	// the file, as WithWritten does for RestoreFile.
	Written func(n int64)
	// Rel, when set, is the file's slash-separated path under the
	// directory restored into, which a seed directory read with SeedGuess
	// is searched at first.
	Rel string
}

// RestoreFiles restores files as RestoreFile does, Workers at a time,
// asking a store that is a FilesReader for the small ones in groups and
// restoring a group it fails file by file. done is called once for every
// file with its outcome or error, possibly from several goroutines at
// once; an error it returns stops the restore.
func (r *Restorer) RestoreFiles(ctx context.Context, files []FileToRestore, done func(i int, outcome Outcome, err error) error) error {
	if err := r.check(); err != nil {
		return err
	}

	reader, grouped := r.Store.(FilesReader)
	grouped = grouped && !r.DryRun

	grp, gctx := errgroup.WithContext(ctx)
	grp.SetLimit(r.workers())

	var (
		group []int
		size  int64
	)

	flush := func() {
		members := group
		group, size = nil, 0

		grp.Go(func() error { return r.restoreGroup(gctx, reader, files, members, done) })
	}

	for i, file := range files {
		if gctx.Err() != nil {
			break
		}

		if file.Stat.GetLost() {
			r.countFile(gctx, OutcomeLost)
			if err := done(i, OutcomeLost, nil); err != nil {
				_ = grp.Wait()
				return err
			}

			continue
		}

		if cloudOnly(file.Stat) {
			atomic.AddInt64(&r.stats.CloudOnly, 1)
			if err := done(i, OutcomeCloudOnly, nil); err != nil {
				_ = grp.Wait()
				return err
			}

			continue
		}

		if !grouped || file.Stat.GetSize() >= smallFile {
			grp.Go(func() error { return r.restoreOne(gctx, i, file, done) })
			continue
		}

		if len(group) == MaxFilesPerRead || size+file.Stat.GetSize() > groupBytes {
			flush()
		}

		group = append(group, i)
		size += file.Stat.GetSize()
	}

	if len(group) > 0 && gctx.Err() == nil {
		flush()
	}

	return grp.Wait()
}

func (r *Restorer) restoreOne(ctx context.Context, i int, file FileToRestore, done func(int, Outcome, error) error) error {
	if file.Written != nil {
		ctx = WithWritten(ctx, file.Written)
	}

	r.guess(file.Rel, file.Stat)

	outcome, err := r.RestoreFile(ctx, file.Path, file.Stat, file.Ref)

	return done(i, outcome, err)
}

// group is a run of small files read from the store together.
type group struct {
	r     *Restorer
	files FilesReader
	done  func(int, Outcome, error) error

	mtx sync.Mutex
	// waiting holds, by ref, the members that still need a part
	waiting map[string][]waiter
}

// member is one file of a group, assembled in buf.
type member struct {
	i    int
	file FileToRestore
	ctx  context.Context

	reader *fileReader
	buf    []byte
	skip   []int
	// missing counts the refs still to come from the store
	missing int
	// landed is counted once the file is in place
	landed   []landed
	finished bool
	// fresh is set for a member read in one call with its parts, which
	// takes none of them from anywhere else
	fresh bool
}

type landed struct {
	counter *int64
	source  string
	n       int64
}

// waiter is a member that needs one part at every index in at.
type waiter struct {
	m  *member
	at []int
}

// restoreGroup reads the files at indexes in one call, or two when some
// of them have a destination, a partial file, seeds or a cache to take
// parts from, which needs their file objects before the parts are asked
// for. The files the calls left unfinished are restored one by one.
func (r *Restorer) restoreGroup(ctx context.Context, files FilesReader, all []FileToRestore, indexes []int, done func(int, Outcome, error) error) error {
	g := &group{r: r, files: files, done: done, waiting: map[string][]waiter{}}

	var members, inspect, fresh []*member

	fsys := r.fs()
	_, readBack := fsys.(RandomWriteFS)

	for _, i := range indexes {
		m := &member{i: i, file: all[i], ctx: ctx}
		if m.file.Written != nil {
			m.ctx = WithWritten(ctx, m.file.Written)
		}

		members = append(members, m)
		r.guess(m.file.Rel, m.file.Stat)

		existing, err := fsys.Lstat(ctx, m.file.Path)
		hasFile := err == nil && existing.Mode().IsRegular()

		if hasFile && r.Overwrite == OverwriteIfChanged && existing.Size() == m.file.Stat.GetSize() && existing.ModTime().UnixNano() == m.file.Stat.GetMtimeNs() {
			if err := g.finish(m, OutcomeSkipped, r.dropPartial(ctx, m.file.Path)); err != nil {
				return err
			}

			continue
		}

		_, err = fsys.Lstat(ctx, r.partial(m.file.Path))
		if (readBack && (hasFile || !errors.Is(err, fs.ErrNotExist))) || r.Seeds != nil || r.Cache != nil || r.parts.wasWritten(m.file.Ref) {
			inspect = append(inspect, m)
		} else {
			m.fresh = true
			fresh = append(fresh, m)
		}
	}

	stop, failed := g.read(ctx, inspect, true)
	if stop == nil && failed == nil {
		next := fresh
		for _, m := range inspect {
			if !m.finished {
				next = append(next, m)
			}
		}

		stop, failed = g.read(ctx, next, false)
	}

	if stop != nil {
		return stop
	}

	if failed != nil && (ctx.Err() != nil || errors.Is(failed, ErrQuotaExceeded)) {
		return failed
	}

	rest, rctx := errgroup.WithContext(ctx)
	rest.SetLimit(r.workers())

	for _, m := range members {
		if !m.finished {
			rest.Go(func() error { return r.restoreOne(rctx, m.i, m.file, done) })
		}
	}

	return rest.Wait()
}

// read asks the store for the members' file objects and, unless
// objectsOnly, the parts they miss, and writes every member once it is
// whole. stop is an error done returned, failed the read's.
func (g *group) read(ctx context.Context, members []*member, objectsOnly bool) (stop, failed error) {
	if len(members) == 0 {
		return nil, nil
	}

	reads := make([]FileRead, len(members))
	for j, m := range members {
		reads[j] = FileRead{Ref: m.file.Ref, Skip: m.skip}
	}

	writes, wctx := errgroup.WithContext(ctx)
	writes.SetLimit(g.r.workers())

	failed = g.files.ReadFiles(wctx, reads, objectsOnly, func(j int, obj *proto.Object) error {
		if j >= len(members) {
			return errors.Errorf("file %d of a read of %d", j, len(members))
		}

		if members[j].reader != nil {
			return nil
		}

		return g.plan(writes, members[j], obj)
	}, func(j, index int, obj *proto.Object) error {
		return g.deliver(writes, members, j, index, obj)
	})

	return writes.Wait(), failed
}

// plan opens a member's file object, takes every part it can from the
// destination, the partial file, the seeds and the cache, and leaves the
// rest waiting for the store.
func (g *group) plan(writes *errgroup.Group, m *member, obj *proto.Object) error {
	r := g.r
	path := m.file.Path

	if obj.GetFile() == nil {
		return errors.Errorf("object %x is not a file", m.file.Ref.GetHash())
	}

	reader := newFileReader(m.ctx, r.Store, obj.GetFile(), r.Key)
	if _, err := reader.getFileParts(m.ctx); err != nil {
		return err
	}

	m.reader = reader
	m.buf = make([]byte, reader.size())

	fsys := r.fs()

	existing, err := fsys.Lstat(m.ctx, path)
	hasFile := err == nil && existing.Mode().IsRegular()

	var matched, held []bool

	if hasFile {
		matched = r.fill(m.ctx, path, reader, m.buf)

		if existing.Size() == reader.size() && allTrue(matched) {
			m.landed = append(m.landed, landed{&r.stats.BytesFromDestination, "destination", reader.size()})

			writes.Go(func() error {
				err := r.dropPartial(m.ctx, path)
				if err == nil {
					err = r.placeMeta(m.ctx, fsys, path, m.file.Stat)
				}

				if err == nil {
					r.recordWhole(path, reader, m.file.Ref)
				}

				return g.finish(m, OutcomeUnchanged, err)
			})

			return nil
		}
	}

	partial := r.partial(path)
	if info, err := fsys.Lstat(m.ctx, partial); err == nil && info.Mode().IsRegular() {
		held = r.fill(m.ctx, partial, reader, m.buf)
	}

	local := r.rechunked(path, hasFile, matched)

	var fromStore []int

	for i, part := range reader.parts {
		switch {
		case part.Ref == nil:
			copy(m.buf, reader.inline)
			m.landed = append(m.landed, landed{&r.stats.BytesFromStore, "store", int64(len(reader.inline))})
		case held != nil && held[i]:
			m.landed = append(m.landed, landed{&r.stats.BytesFromDestination, "partial", int64(part.Length)})
		case matched != nil && matched[i]:
			m.landed = append(m.landed, landed{&r.stats.BytesFromDestination, "destination", int64(part.Length)})
		default:
			if m.fresh {
				fromStore = append(fromStore, i)
				continue
			}

			data, counter, source, ok := r.localCopy(reader, i, local)
			if !ok {
				fromStore = append(fromStore, i)
				continue
			}

			copy(m.buf[part.Offset:], data)
			m.landed = append(m.landed, landed{counter, source, int64(len(data))})
		}
	}

	wanted := make(map[int]bool, len(fromStore))
	for _, i := range fromStore {
		wanted[i] = true
	}

	m.skip = nil
	for i := range reader.parts {
		if !wanted[i] {
			m.skip = append(m.skip, i)
		}
	}

	copies := copiesOf(reader, fromStore)

	g.mtx.Lock()
	for leader, at := range copies {
		key := string(reader.parts[leader].Ref.Hash)
		g.waiting[key] = append(g.waiting[key], waiter{m: m, at: at})
	}

	m.missing = len(copies)
	g.mtx.Unlock()

	if len(copies) == 0 {
		writes.Go(func() error { return g.write(m) })
	}

	return nil
}

// deliver copies a part the store sent into every member waiting for it
// and writes the members it made whole.
func (g *group) deliver(writes *errgroup.Group, members []*member, j, index int, obj *proto.Object) error {
	if j >= len(members) || members[j].reader == nil || index >= len(members[j].reader.parts) || members[j].reader.parts[index].Ref == nil {
		return errors.Errorf("unrequested part %d of file %d", index, j)
	}

	reader := members[j].reader
	part := reader.parts[index]
	key := string(part.Ref.Hash)

	g.mtx.Lock()
	waiters := g.waiting[key]
	delete(g.waiting, key)
	g.mtx.Unlock()

	if len(waiters) == 0 {
		return errors.Errorf("part %d of file %x sent unasked or twice", index, members[j].file.Ref.GetHash())
	}

	data, err := reader.openPart(index, part, obj)
	if err != nil {
		return err
	}

	if g.r.Cache != nil {
		_ = g.r.Cache.Put(part.Ref, obj)
	}

	whole, err := g.land(waiters, data)
	if err != nil {
		return err
	}

	// a write takes the lock when it finishes, so none is started under it
	for _, m := range whole {
		writes.Go(func() error { return g.write(m) })
	}

	return nil
}

// land copies data into the waiters and returns the members it made
// whole.
func (g *group) land(waiters []waiter, data []byte) ([]*member, error) {
	g.mtx.Lock()
	defer g.mtx.Unlock()

	var whole []*member

	for _, w := range waiters {
		for _, i := range w.at {
			if w.m.reader.parts[i].Length != uint64(len(data)) {
				return nil, errors.Errorf("part %d of file %x has %d bytes, expected %d", i, w.m.file.Ref.GetHash(), len(data), w.m.reader.parts[i].Length)
			}

			copy(w.m.buf[w.m.reader.parts[i].Offset:], data)
			w.m.landed = append(w.m.landed, landed{&g.r.stats.BytesFromStore, "store", int64(len(data))})
		}

		w.m.missing--
		if w.m.missing == 0 {
			whole = append(whole, w.m)
		}
	}

	return whole, nil
}

// write puts a whole member in place through its partial file.
func (g *group) write(m *member) error {
	err := g.r.writeWhole(m.ctx, m.file.Path, m.file.Stat, m.file.Ref, m.reader, m.buf)
	if err != nil {
		err = errors.Wrapf(err, "restoring %s", m.file.Path)
	}

	return g.finish(m, OutcomeWritten, err)
}

// finish counts a member that was restored and hands it to done either
// way.
func (g *group) finish(m *member, outcome Outcome, err error) error {
	g.mtx.Lock()
	m.finished = true
	counted := m.landed
	g.mtx.Unlock()

	if err == nil {
		for _, l := range counted {
			g.r.countBytes(m.ctx, l.counter, l.source, l.n)
		}

		g.r.countFile(m.ctx, outcome)
	}

	return g.done(m.i, outcome, err)
}

// writeWhole puts a file whose content is all in buf at path through its
// partial file.
func (r *Restorer) writeWhole(ctx context.Context, path string, stat *proto.FileInfo, ref *proto.Ref, reader *fileReader, buf []byte) error {
	partial := r.partial(path)

	random, ok := r.fs().(RandomWriteFS)
	if !ok {
		fsys := r.fs()

		_, err := fsys.WriteFile(ctx, partial, bytes.NewReader(buf))
		if err == nil {
			err = r.putStreams(ctx, fsys, partial, stat)
		}

		var mode os.FileMode
		if err == nil {
			mode, err = r.ownFile(ctx, fsys, partial, stat)
		}

		if err == nil {
			err = setMeta(ctx, fsys, partial, mode, mtimeOf(stat))
		}

		if err == nil {
			err = fsys.Rename(ctx, partial, path)
		}

		if err == nil {
			err = r.putSecurity(ctx, fsys, path, stat)
		}

		return err
	}

	tmp, err := openPartial(ctx, random, partial)
	if err != nil {
		return err
	}

	if err := writeSparse(tmp, reader, buf); err != nil {
		_ = tmp.Close()
		return err
	}

	if err := r.settle(ctx, random, tmp, nil, nil, partial, path, stat, reader, nil); err != nil {
		return err
	}

	r.recordWhole(path, reader, ref)

	return nil
}

// writeSparse writes buf into tmp, leaving a hole for each part that is
// all zeroes when tmp is empty.
func writeSparse(tmp RandomFile, reader *fileReader, buf []byte) error {
	if info, err := tmp.Stat(); err != nil || info.Size() > 0 || len(reader.parts) == 0 || reader.parts[0].Ref == nil {
		_, err := tmp.WriteAt(buf, 0)
		return err
	}

	for _, part := range reader.parts {
		data := buf[part.Offset : part.Offset+part.Length]
		if allZero(data) {
			continue
		}

		if _, err := tmp.WriteAt(data, int64(part.Offset)); err != nil {
			return err
		}
	}

	return nil
}

// fill copies into buf every part the file at name holds at its offset,
// as far as buf reaches, and reports which ones it held; on a tree that is
// not read back it holds none.
func (r *Restorer) fill(ctx context.Context, name string, reader *fileReader, buf []byte) []bool {
	held := make([]bool, len(reader.parts))

	random, ok := r.fs().(RandomWriteFS)
	if !ok {
		return held
	}

	f, err := random.Open(ctx, name)
	if err != nil {
		return held
	}
	defer f.Close()

	content := make([]byte, len(buf))
	n, _ := io.ReadFull(f, content)

	for i, part := range reader.parts {
		end := part.Offset + part.Length
		if end > uint64(n) {
			continue
		}

		if r.matches(reader, i, content[part.Offset:end]) {
			copy(buf[part.Offset:], content[part.Offset:end])
			held[i] = true
		}
	}

	return held
}
