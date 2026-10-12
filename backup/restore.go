package backup

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gobackio/goback/backup/blobcache"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/errgroup"
)

// OverwriteMode says how an existing destination file is treated.
type OverwriteMode int

const (
	// OverwriteAlways hashes every part of the existing file and rewrites
	// the ones that differ.
	OverwriteAlways OverwriteMode = iota
	// OverwriteIfChanged trusts a size and mtime match and leaves such a
	// file unread.
	OverwriteIfChanged
)

// String is the mode's name, as ParseOverwriteMode reads it.
func (m OverwriteMode) String() string {
	if m == OverwriteIfChanged {
		return "if-changed"
	}

	return "always"
}

// ParseOverwriteMode reads "always" or "if-changed".
func ParseOverwriteMode(name string) (OverwriteMode, error) {
	switch name {
	case "always":
		return OverwriteAlways, nil
	case "if-changed":
		return OverwriteIfChanged, nil
	}

	return 0, fmt.Errorf("unknown overwrite mode %q: use always or if-changed", name)
}

// Outcome is what RestoreFile did with one file.
type Outcome int

const (
	// OutcomeWritten means the file was written.
	OutcomeWritten Outcome = iota
	// OutcomeUnchanged means the destination already held every part and
	// only its stat was applied.
	OutcomeUnchanged
	// OutcomeSkipped means the destination matched by size and mtime under
	// OverwriteIfChanged and was left unread.
	OutcomeSkipped
	// OutcomeWouldWrite means a dry run would have written the file.
	OutcomeWouldWrite
	// OutcomeSalvaged means the file was written with holes where parts
	// could not be read.
	OutcomeSalvaged
)

// A Hole is a range of a salvaged file that no source could fill; the
// restored file holds zeroes there.
type Hole struct {
	Path   string
	Offset int64
	Length int64
	// Ref is the part that could not be read.
	Ref *proto.Ref
}

// String implements fmt.Stringer.
func (o Outcome) String() string {
	switch o {
	case OutcomeWritten:
		return "written"
	case OutcomeUnchanged:
		return "unchanged"
	case OutcomeSkipped:
		return "skipped"
	case OutcomeWouldWrite:
		return "would write"
	case OutcomeSalvaged:
		return "salvaged"
	}

	return "unknown"
}

// RestoreStats counts what a Restorer did and where the bytes came from.
type RestoreStats struct {
	Files     int64 `json:"files"`
	Written   int64 `json:"written"`
	Unchanged int64 `json:"unchanged"`
	Skipped   int64 `json:"skipped"`
	// Salvaged counts files written with holes, and MissingBytes the bytes
	// of those holes.
	Salvaged     int64 `json:"salvaged"`
	MissingBytes int64 `json:"missing_bytes"`

	BytesFromDestination int64 `json:"bytes_from_destination"`
	BytesFromSeeds       int64 `json:"bytes_from_seeds"`
	BytesFromCache       int64 `json:"bytes_from_cache"`
	BytesFromStore       int64 `json:"bytes_from_store"`
}

// DefaultRestoreWorkers is how much of a restore runs at once when nothing
// says otherwise: twice the cpu count, since a file spends most of its
// time waiting on the store or on the disk rather than on a core.
func DefaultRestoreWorkers() int { return 2 * runtime.NumCPU() }

var (
	restoreMeter = otel.Meter("goback.io/backup")
	restoreBytes = mustInstrument(restoreMeter.Int64Counter("goback.backup.restore.bytes", metric.WithDescription("bytes of restored files by source"), metric.WithUnit("By")))
	restoreFiles = mustInstrument(restoreMeter.Int64Counter("goback.backup.restore.files", metric.WithDescription("restored files by outcome")))

	keySource  = attribute.Key("source")
	keyOutcome = attribute.Key("outcome")
)

func mustInstrument[T any](instrument T, err error) T {
	if err != nil {
		panic(err)
	}

	return instrument
}

// Restorer writes files from the store, taking every part it can from the
// destination file, the seeds and the blob cache before the network. Local
// bytes are used only after they hash to the recorded ref.
type Restorer struct {
	Store ObjectStore
	Key   *storekey.Key

	Seeds *SeedMap
	Cache *blobcache.Cache

	// Workers bounds how much of a restore runs at once, files and the
	// parts of one; 0 means DefaultRestoreWorkers.
	Workers   int
	Overwrite OverwriteMode
	// Verify rehashes every written file before it is renamed into place.
	Verify bool
	// DryRun reports what would change and writes nothing.
	DryRun bool
	// Salvage writes a file whose parts cannot all be read, leaving the
	// unreadable ranges as zeroes, instead of failing it.
	Salvage bool
	// Rechunk cuts a destination that does not match at the recorded
	// offsets with the backup's chunker, so parts that only moved are
	// taken from it instead of the network. It costs a read of the file.
	Rechunk bool
	// OnHole, when set, is called for every hole a salvaged file was left
	// with, possibly from several goroutines at once.
	OnHole func(Hole)

	stats RestoreStats
}

// Stats returns what the restorer has done so far.
func (r *Restorer) Stats() RestoreStats {
	return RestoreStats{
		Files:                atomic.LoadInt64(&r.stats.Files),
		Written:              atomic.LoadInt64(&r.stats.Written),
		Unchanged:            atomic.LoadInt64(&r.stats.Unchanged),
		Skipped:              atomic.LoadInt64(&r.stats.Skipped),
		Salvaged:             atomic.LoadInt64(&r.stats.Salvaged),
		MissingBytes:         atomic.LoadInt64(&r.stats.MissingBytes),
		BytesFromDestination: atomic.LoadInt64(&r.stats.BytesFromDestination),
		BytesFromSeeds:       atomic.LoadInt64(&r.stats.BytesFromSeeds),
		BytesFromCache:       atomic.LoadInt64(&r.stats.BytesFromCache),
		BytesFromStore:       atomic.LoadInt64(&r.stats.BytesFromStore),
	}
}

func (r *Restorer) countBytes(ctx context.Context, counter *int64, source string, n int64) {
	atomic.AddInt64(counter, n)
	restoreBytes.Add(ctx, n, metric.WithAttributes(keySource.String(source)))

	if written, ok := ctx.Value(writtenKey{}).(func(int64)); ok {
		written(n)
	}
}

type writtenKey struct{}

// WithWritten makes a RestoreFile called with the returned context hand
// written the size of each part as it lands in the file, from whichever
// source, possibly from several goroutines at once.
func WithWritten(ctx context.Context, written func(n int64)) context.Context {
	return context.WithValue(ctx, writtenKey{}, written)
}

func (r *Restorer) countFile(ctx context.Context, outcome Outcome) {
	atomic.AddInt64(&r.stats.Files, 1)

	switch outcome {
	case OutcomeWritten:
		atomic.AddInt64(&r.stats.Written, 1)
	case OutcomeUnchanged:
		atomic.AddInt64(&r.stats.Unchanged, 1)
	case OutcomeSkipped:
		atomic.AddInt64(&r.stats.Skipped, 1)
	}

	restoreFiles.Add(ctx, 1, metric.WithAttributes(keyOutcome.String(outcome.String())))
}

func (r *Restorer) workers() int {
	if r.Workers > 0 {
		return r.Workers
	}

	return DefaultRestoreWorkers()
}

// RestoreFile puts the file ref describes at path with the recorded mode
// and mtime, assembling it at PartialPath(path) and renaming into place.
// A failed restore leaves the partial file behind, and the next one takes
// every part from it that still hashes to its ref.
func (r *Restorer) RestoreFile(ctx context.Context, path string, stat *proto.FileInfo, ref *proto.Ref) (Outcome, error) {
	obj, err := r.Store.Get(ctx, ref)
	if err != nil {
		return 0, errors.Wrapf(err, "file %x", ref.Hash)
	}

	if obj.GetFile() == nil {
		return 0, errors.Errorf("object %x is not a file", ref.Hash)
	}

	reader := newFileReader(ctx, r.Store, obj.GetFile(), r.Key)
	if _, err := reader.getFileParts(ctx); err != nil {
		return 0, err
	}

	existing, err := os.Lstat(path)
	hasFile := err == nil && existing.Mode().IsRegular()

	if hasFile && r.Overwrite == OverwriteIfChanged && existing.Size() == stat.Size && existing.ModTime().UnixNano() == stat.MtimeNs {
		if err := r.dropPartial(path); err != nil {
			return 0, err
		}

		r.countFile(ctx, OutcomeSkipped)
		return OutcomeSkipped, nil
	}

	var source *os.File
	var matched []bool

	if hasFile {
		source, err = os.Open(path)
		if err != nil {
			return 0, err
		}
		defer source.Close()

		matched = r.verifyParts(source, reader)

		if existing.Size() == reader.size() && allTrue(matched) {
			if err := r.dropPartial(path); err != nil {
				return 0, err
			}

			if !r.DryRun {
				if err := applyStat(path, stat); err != nil {
					return 0, err
				}
			}

			r.countBytes(ctx, &r.stats.BytesFromDestination, "destination", reader.size())
			r.countFile(ctx, OutcomeUnchanged)

			return OutcomeUnchanged, nil
		}
	}

	if r.DryRun {
		r.countFile(ctx, OutcomeWouldWrite)
		return OutcomeWouldWrite, nil
	}

	local := r.rechunked(path, hasFile, matched)

	tmp, err := openPartial(PartialPath(path))
	if err != nil {
		return 0, err
	}

	held := r.verifyParts(tmp, reader)

	missing, err := r.assemble(ctx, tmp, source, reader, matched, held, local, ref)
	if err == nil {
		err = r.settle(tmp, source, path, stat, reader, missing)
	} else {
		_ = tmp.Close()
	}

	if err != nil {
		return 0, errors.Wrapf(err, "restoring %s", path)
	}

	if len(missing) > 0 {
		r.reportHoles(path, reader, missing)
		r.countFile(ctx, OutcomeSalvaged)

		return OutcomeSalvaged, nil
	}

	r.countFile(ctx, OutcomeWritten)

	return OutcomeWritten, nil
}

// settle cuts the assembled partial file to the file's size, rehashes it
// when Verify is set, applies stat and renames it over path, closing tmp
// and source, the destination it was read from, on the way.
func (r *Restorer) settle(tmp, source *os.File, path string, stat *proto.FileInfo, reader *fileReader, missing []int) error {
	// the partial may be longer, from another version, or end in a hole
	err := tmp.Truncate(reader.size())

	if err == nil && r.Verify {
		err = r.verifyWritten(tmp, reader, missing)
	}

	if err == nil {
		err = tmp.Sync()
	}

	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}

	if err == nil {
		err = applyStat(tmp.Name(), stat)
	}

	if source != nil {
		// the destination must be closed before it is replaced
		_ = source.Close()
	}

	if err == nil {
		err = replace(tmp.Name(), path)
	}

	return err
}

// PartialPath is the file RestoreFile assembles path in, next to it.
func PartialPath(path string) string {
	return filepath.Join(filepath.Dir(path), "."+filepath.Base(path)+".goback.partial")
}

// openPartial opens the partial file for writing, keeping what an earlier
// restore left in it; a partial left read-only by a failed rename is made
// writable, and anything there that is not a regular file is removed
// rather than written through.
func openPartial(name string) (*os.File, error) {
	if info, err := os.Lstat(name); err == nil {
		switch {
		case !info.Mode().IsRegular():
			if err := os.Remove(name); err != nil {
				return nil, err
			}
		case info.Mode()&0o200 == 0:
			if err := os.Chmod(name, 0o600); err != nil {
				return nil, err
			}
		}
	}

	return os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o600)
}

// dropPartial removes the partial file of a destination that turned out
// not to need writing.
func (r *Restorer) dropPartial(path string) error {
	if r.DryRun {
		return nil
	}

	err := os.Remove(PartialPath(path))
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

// reportHoles counts the parts a salvaged file went without and hands each
// one to OnHole.
func (r *Restorer) reportHoles(path string, reader *fileReader, missing []int) {
	atomic.AddInt64(&r.stats.Salvaged, 1)

	for _, i := range missing {
		part := reader.parts[i]
		atomic.AddInt64(&r.stats.MissingBytes, int64(part.Length))

		if r.OnHole != nil {
			r.OnHole(Hole{Path: path, Offset: int64(part.Offset), Length: int64(part.Length), Ref: part.Ref})
		}
	}
}

// replace renames tmp over path and makes the rename durable; a read-only
// destination, which Windows refuses to replace, is made writable first.
func replace(tmp, path string) error {
	err := os.Rename(tmp, path)
	if err != nil {
		info, statErr := os.Lstat(path)
		if statErr != nil || info.Mode()&0o200 != 0 {
			return err
		}

		if chmodErr := os.Chmod(path, info.Mode().Perm()|0o200); chmodErr != nil {
			return err
		}

		if err = os.Rename(tmp, path); err != nil {
			return err
		}
	}

	return syncDir(filepath.Dir(path))
}

// syncDir flushes a directory's entries; Windows has no directory fsync
// and its rename is already durable once the volume flushes.
func syncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}

	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()

	return d.Sync()
}

func allTrue(flags []bool) bool {
	for _, f := range flags {
		if !f {
			return false
		}
	}

	return true
}

func applyStat(path string, stat *proto.FileInfo) error {
	err := os.Chtimes(path, time.Now(), time.Unix(0, stat.MtimeNs))
	if err != nil {
		return err
	}

	return os.Chmod(path, os.FileMode(stat.Mode))
}

// verifyParts reports for every part whether the file holds it at the
// recorded offset.
func (r *Restorer) verifyParts(file *os.File, reader *fileReader) []bool {
	matched := make([]bool, len(reader.parts))

	info, err := file.Stat()
	if err != nil {
		return matched
	}

	for i, part := range reader.parts {
		if int64(part.Offset+part.Length) > info.Size() {
			continue
		}

		buf := make([]byte, part.Length)

		_, err := file.ReadAt(buf, int64(part.Offset))
		if err != nil {
			continue
		}

		matched[i] = r.matches(reader, i, buf)
	}

	return matched
}

// verifyWritten rehashes every part of the assembled file, except the
// holes a salvage left.
func (r *Restorer) verifyWritten(file *os.File, reader *fileReader, missing []int) error {
	holes := make(map[int]bool, len(missing))
	for _, i := range missing {
		holes[i] = true
	}

	for i, part := range reader.parts {
		if holes[i] {
			continue
		}

		buf := make([]byte, part.Length)

		if _, err := file.ReadAt(buf, int64(part.Offset)); err != nil {
			return errors.Wrapf(err, "verifying part %d", i)
		}

		if !r.matches(reader, i, buf) {
			return errors.Errorf("part %d at offset %d does not hash to its ref after writing", i, part.Offset)
		}
	}

	return nil
}

// matches reports whether chunk is the content of part i: its ref under
// the store key or in the clear must be the part's.
func (r *Restorer) matches(reader *fileReader, i int, chunk []byte) bool {
	part := reader.parts[i]
	if uint64(len(chunk)) != part.Length {
		return false
	}

	if part.Ref == nil {
		return bytes.Equal(chunk, reader.inline)
	}

	if r.Key != nil && BlobRef(r.Key, chunk).Equal(part.Ref) {
		return true
	}

	return proto.NewObject(&proto.Blob{Data: chunk}).Ref().Equal(part.Ref)
}

// rechunked cuts the destination with the backup's chunker when it did not
// match at the recorded offsets, so parts that only moved are still found.
// It returns nil when there is nothing to gain or Rechunk is off.
func (r *Restorer) rechunked(path string, hasFile bool, matched []bool) *SeedMap {
	if !r.Rechunk || !hasFile || allTrue(matched) {
		return nil
	}

	local := NewSeedMap(r.Key)
	if err := local.Add(path); err != nil {
		return nil
	}

	return local
}

// assemble writes every part at its offset that dst does not already hold:
// matched parts are copied from source, the rest come from the re-cut
// destination, the seeds, the cache or the store. It returns the parts no
// source held, which is empty unless Salvage is set.
func (r *Restorer) assemble(ctx context.Context, dst, source *os.File, reader *fileReader, matched, held []bool, local *SeedMap, ref *proto.Ref) ([]int, error) {
	var fromStore []int

	for i, part := range reader.parts {
		offset := int64(part.Offset)

		if part.Ref == nil {
			if _, err := dst.WriteAt(reader.inline, 0); err != nil {
				return nil, err
			}

			r.countBytes(ctx, &r.stats.BytesFromStore, "store", int64(len(reader.inline)))

			continue
		}

		if held[i] {
			r.countBytes(ctx, &r.stats.BytesFromDestination, "partial", int64(part.Length))

			continue
		}

		if matched != nil && matched[i] {
			buf := make([]byte, part.Length)
			if _, err := source.ReadAt(buf, offset); err != nil {
				return nil, errors.Wrapf(err, "rereading part %d of %s", i, source.Name())
			}

			if _, err := dst.WriteAt(buf, offset); err != nil {
				return nil, err
			}

			r.countBytes(ctx, &r.stats.BytesFromDestination, "destination", int64(part.Length))

			continue
		}

		if data, counter, source, ok := r.localCopy(reader, i, local); ok {
			if _, err := dst.WriteAt(data, offset); err != nil {
				return nil, err
			}

			r.countBytes(ctx, counter, source, int64(len(data)))

			continue
		}

		fromStore = append(fromStore, i)
	}

	if len(fromStore) == 0 {
		return nil, nil
	}

	copies := copiesOf(reader, fromStore)

	// a stream reports the file, not the part, so salvage asks for each
	// part on its own and learns exactly which ones are gone
	if parts, ok := r.Store.(PartReader); ok && !r.Salvage {
		return nil, r.stream(ctx, parts, dst, reader, copies, ref)
	}

	var (
		mu      sync.Mutex
		missing []int
	)

	grp, gctx := errgroup.WithContext(ctx)
	grp.SetLimit(r.workers())

	for i, at := range copies {
		grp.Go(func() error {
			part := reader.parts[i]

			obj, err := r.Store.Get(gctx, part.Ref)
			if err != nil {
				if r.Salvage && errors.Is(err, ErrNotFound) {
					mu.Lock()
					missing = append(missing, at...)
					mu.Unlock()

					return nil
				}

				return errors.Wrapf(err, "part %d (%x) of file %x", i, part.Ref.Hash, reader.fileRef())
			}

			return r.writePart(gctx, dst, reader, at, obj)
		})
	}

	if err := grp.Wait(); err != nil {
		return nil, err
	}

	sort.Ints(missing)

	return missing, nil
}

// localCopy returns part i from the re-cut destination, the seeds or the
// cache, whichever holds it first, with the counter and source it is
// counted under.
func (r *Restorer) localCopy(reader *fileReader, i int, local *SeedMap) ([]byte, *int64, string, bool) {
	part := reader.parts[i]

	if local != nil {
		if buf, ok := local.read(part); ok && r.matches(reader, i, buf) {
			return buf, &r.stats.BytesFromDestination, "destination", true
		}
	}

	if r.Seeds != nil {
		if buf, ok := r.Seeds.read(part); ok && r.matches(reader, i, buf) {
			return buf, &r.stats.BytesFromSeeds, "seed", true
		}
	}

	if r.Cache != nil {
		if obj, ok := r.Cache.Get(part.Ref); ok {
			data, err := reader.openPart(i, part, obj)
			if err == nil {
				return data, &r.stats.BytesFromCache, "cache", true
			}

			r.Cache.Drop(part.Ref)
		}
	}

	return nil, nil, "", false
}

// copiesOf groups the parts by ref under the first part holding it, so a
// file holding the same part several times fetches it once.
func copiesOf(reader *fileReader, indexes []int) map[int][]int {
	first := make(map[string]int, len(indexes))
	copies := make(map[int][]int, len(indexes))

	for _, i := range indexes {
		key := string(reader.parts[i].Ref.Hash)

		leader, ok := first[key]
		if !ok {
			first[key] = i
			leader = i
		}

		copies[leader] = append(copies[leader], i)
	}

	return copies
}

const (
	// stripeParts is how many parts a file needs per ReadParts call it
	// is read through, up to maxStripes calls: a server feeds one call
	// only so fast when it reads the parts itself.
	stripeParts = 256
	maxStripes  = 8
)

// stream fetches one copy of every wanted part through ReadParts calls
// that each ask for a contiguous stripe of them, skipping every other
// part, so a server that can locate parts still finds them side by side.
func (r *Restorer) stream(ctx context.Context, parts PartReader, dst *os.File, reader *fileReader, copies map[int][]int, ref *proto.Ref) error {
	leaders := make([]int, 0, len(copies))
	for i := range copies {
		leaders = append(leaders, i)
	}

	sort.Ints(leaders)

	n := min(maxStripes, r.workers(), max(1, len(leaders)/stripeParts))
	stripe := make(map[int]int, len(leaders))

	for k, i := range leaders {
		stripe[i] = k * n / len(leaders)
	}

	var (
		mu   sync.Mutex
		seen = make(map[int]bool, len(copies))
	)

	grp, gctx := errgroup.WithContext(ctx)

	for j := range n {
		skip := make([]int, 0, len(reader.parts))
		for i := range reader.parts {
			if s, ok := stripe[i]; !ok || s != j {
				skip = append(skip, i)
			}
		}

		grp.Go(func() error {
			return parts.ReadParts(gctx, ref, skip, func(i int, obj *proto.Object) error {
				if s, ok := stripe[i]; !ok || s != j {
					return errors.Errorf("file %x: unrequested part %d", ref.Hash, i)
				}

				mu.Lock()
				again := seen[i]
				seen[i] = true
				mu.Unlock()

				if again {
					return errors.Errorf("file %x: part %d streamed twice", ref.Hash, i)
				}

				return r.writePart(gctx, dst, reader, copies[i], obj)
			})
		})
	}

	if err := grp.Wait(); err != nil {
		return errors.Wrapf(err, "file %x", ref.Hash)
	}

	if len(seen) != len(copies) {
		return errors.Errorf("file %x: %d of %d parts streamed", ref.Hash, len(seen), len(copies))
	}

	return nil
}

// writePart opens a part's stored object, writes it at the offset of
// every part in at, which all hold it, and caches it.
func (r *Restorer) writePart(ctx context.Context, dst *os.File, reader *fileReader, at []int, obj *proto.Object) error {
	part := reader.parts[at[0]]

	data, err := reader.openPart(at[0], part, obj)
	if err != nil {
		return err
	}

	for _, i := range at {
		if _, err := dst.WriteAt(data, int64(reader.parts[i].Offset)); err != nil {
			return err
		}

		r.countBytes(ctx, &r.stats.BytesFromStore, "store", int64(len(data)))
	}

	if r.Cache != nil {
		_ = r.Cache.Put(part.Ref, obj)
	}

	return nil
}
