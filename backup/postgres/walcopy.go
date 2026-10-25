package postgres

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
)

// copyWAL copies the WAL recovery reads from the WAL commit into dir's
// pg_wal, pg_xlog before Postgres 10, and returns the names it wrote there.
func (r *Restore) copyWAL(ctx context.Context, dir string, base *proto.Commit, walRef *proto.Ref, wal *walCommit, major int) ([]string, error) {
	ctx, end, err := backup.HoldRestore(ctx, r.Objects, wal.commit.GetBackupSet(), walRef)
	if err != nil {
		return nil, err
	}
	defer end()

	// before 13, recovery that runs out of WAL short of its target promotes
	// there without a word, so nothing tells a copy cut too short
	var until time.Time
	if major >= 13 {
		until = r.At
	}

	fsys := r.fs()

	pgWAL := fsys.Join(dir, "pg_wal")
	if major < 10 {
		pgWAL = fsys.Join(dir, "pg_xlog")
	}

	if err := fsys.MkdirAll(ctx, pgWAL); err != nil {
		return nil, err
	}

	var (
		copied  []string
		indexes []int
		files   []backup.FileToRestore
		pads    []bool
	)

	for _, i := range wal.files.copies(wal.path, base.GetMetadata()[MetaStartWALFile], until) {
		name := wal.files.names[i]

		segment, partial := isPartial(name)
		if partial {
			name = segment
		}

		node := wal.files.nodes[i]
		files = append(files, backup.FileToRestore{Path: fsys.Join(pgWAL, name), Stat: node.Stat, Ref: node.Ref})
		indexes = append(indexes, i)
		pads = append(pads, partial)
		copied = append(copied, name)
	}

	random, ok := fsys.(backup.RandomWriteFS)
	if !ok {
		for k, file := range files {
			if err := wal.files.restoreWAL(ctx, fsys, file.Path, indexes[k], pads[k]); err != nil {
				return nil, fmt.Errorf("copying %s: %w", copied[k], err)
			}
		}

		return copied, nil
	}

	restorer := &backup.Restorer{Store: wal.files.objects, Key: wal.files.key, FS: fsys, Workers: r.Workers}

	err = restorer.RestoreFiles(ctx, files, func(k int, outcome backup.Outcome, err error) error {
		if err == nil && outcome == backup.OutcomeLost {
			err = backup.ErrLost
		}

		if err == nil && pads[k] {
			err = padSegment(ctx, random, files[k].Path)
		}

		if err != nil {
			return fmt.Errorf("copying %s: %w", copied[k], err)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return copied, nil
}

// copies are the files of the tree a restore copies into pg_wal, in the
// order recovery reads them: every timeline history, then every segment
// from the segment start on that lies on h, the history of the tree's
// newest timeline, and the partial segment the tree serves. With until
// set, the segments end one after the first that closed at or after it.
func (t *commitFiles) copies(h history, start string, until time.Time) []int {
	var histories, segments []int

	from := Carry(start)
	newest := newestTimeline(t.names)

	for i, name := range t.names {
		switch {
		case isHistory(name):
			histories = append(histories, i)
		case !from(name):
		case isSegment(name):
			if segmentTimeline(name) == newest || t.onHistory(h, i) {
				segments = append(segments, i)
			}
		default:
			if segment, ok := isPartial(name); ok && servesPartial(t.names, segment) {
				segments = append(segments, i)
			}
		}
	}

	slices.SortStableFunc(segments, func(a, b int) int {
		x, y := t.names[a], t.names[b]
		return cmp.Or(strings.Compare(x[8:walNameLen], y[8:walNameLen]), strings.Compare(x[:8], y[:8]))
	})

	if !until.IsZero() {
		closed := slices.IndexFunc(segments, func(i int) bool { return t.nodes[i].Stat.GetMtimeNs() >= until.UnixNano() })
		if closed >= 0 {
			segments = segments[:min(closed+2, len(segments))]
		}
	}

	return append(histories, segments...)
}

// onHistory reports whether the whole segment i of the tree lies on h; a
// nil h holds every segment.
func (t *commitFiles) onHistory(h history, i int) bool {
	// a whole segment's size is the cluster's segment size
	size := uint64(t.nodes[i].Stat.GetSize())

	return h == nil || size == 0 || h.holds(t.names[i], size)
}

// segmentSize is the cluster's segment size, as the header of the tree's
// first segment records it; zero when the tree holds no segment.
func (t *commitFiles) segmentSize(ctx context.Context) (uint64, error) {
	for i, name := range t.names {
		if _, partial := isPartial(name); !partial && !isSegment(name) {
			continue
		}

		file, err := backup.NewBackupReader(t.objects).WithKey(t.key).ReadFile(ctx, t.nodes[i].Ref)
		if err != nil {
			return 0, fmt.Errorf("reading %s: %w", name, err)
		}

		hdr, err := readSegmentHeader(file)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", name, err)
		}

		return uint64(hdr.segSize), nil
	}

	return 0, nil
}

// gap names the first segment recovery from a base that started in start
// and stopped at stop (zero when unknown) cannot reach, because the segment
// before it is missing from the tree; "" when it reaches every one of
// copies(h, start, until). The base itself holds its WAL up to its stop.
func (t *commitFiles) gap(h history, start string, stop, segSize uint64, until time.Time) string {
	if segSize == 0 {
		return ""
	}

	next := segmentNumber(start, segSize)
	if stop > 0 {
		next = max(next, (stop-1)/segSize+1)
	}

	for _, i := range t.copies(h, start, until) {
		segment, ok := isPartial(t.names[i])
		if !ok {
			if segment = t.names[i]; !isSegment(segment) {
				continue
			}
		}

		n := segmentNumber(segment, segSize)
		if n > next {
			return segment
		}

		next = max(next, n+1)
	}

	return ""
}

// restoreWAL writes file i of the tree to path on fsys; pad extends a
// partial segment with zeros to the size its header records, as outside
// standby mode Postgres refuses a restored segment of any other size.
func (t *commitFiles) restoreWAL(ctx context.Context, fsys backup.WriteFS, path string, i int, pad bool) error {
	node := t.nodes[i]
	restorer := &backup.Restorer{Store: t.objects, Key: t.key, FS: fsys}

	random, ok := fsys.(backup.RandomWriteFS)
	if pad && !ok {
		_, name := fsys.Split(path)
		restorer.FS = padFS{WriteFS: fsys, name: name, size: node.Stat.GetSize()}
	}

	outcome, err := restorer.RestoreFile(ctx, path, node.Stat, node.Ref)
	if err == nil && outcome == backup.OutcomeLost {
		err = fmt.Errorf("%s: %w", path, backup.ErrLost)
	}

	if err != nil {
		return err
	}

	if pad && ok {
		return padSegment(ctx, random, path)
	}

	return nil
}

// padSegment extends the partial segment at path with zeros to the size
// its header records.
func padSegment(ctx context.Context, fsys backup.RandomWriteFS, path string) error {
	f, err := fsys.OpenFile(ctx, path)
	if err != nil {
		return err
	}

	hdr, err := readSegmentHeader(io.NewSectionReader(f, 0, math.MaxInt64))
	if err != nil {
		_ = f.Close()
		return err
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}

	if info.Size() < int64(hdr.segSize) {
		if err := f.Truncate(int64(hdr.segSize)); err != nil {
			_ = f.Close()
			return err
		}
	}

	return f.Close()
}

// padFS writes a partial segment of size bytes, named name, onto a tree
// written front to back, followed by the zeros padSegment would add.
type padFS struct {
	backup.WriteFS
	name string
	size int64
}

// WriteFile implements backup.WriteFS. It counts only the segment's own
// bytes as written, and writes every other file as it is.
func (p padFS) WriteFile(ctx context.Context, path string, r io.Reader) (int64, error) {
	if _, name := p.Split(path); name != p.name {
		return p.WriteFS.WriteFile(ctx, path, r)
	}

	var head bytes.Buffer

	hdr, err := readSegmentHeader(io.TeeReader(r, &head))
	if err != nil {
		return 0, err
	}

	pad := max(int64(hdr.segSize)-p.size, 0)

	n, err := p.WriteFS.WriteFile(ctx, path, io.MultiReader(&head, r, io.LimitReader(zeros{}, pad)))

	return min(n, p.size), err
}

type zeros struct{}

func (zeros) Read(b []byte) (int, error) {
	clear(b)
	return len(b), nil
}
