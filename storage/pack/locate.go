package pack

import (
	"context"
	"errors"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
)

// ErrNoSignedURL is a RangeSigner's answer when it cannot address the
// bytes it holds.
var ErrNoSignedURL = errors.New("this storage hands out no URLs")

// A RangeSigner is an ArchiveStorage whose files can be fetched directly,
// one byte range at a time.
type RangeSigner interface {
	// SignRange returns a GET whose body is bytes [offset, offset+length)
	// of name, good for ttl, carrying whatever headers the request must
	// repeat. It returns ErrNoSignedURL when it cannot sign.
	SignRange(ctx context.Context, name string, offset, length int64, ttl time.Duration) (*proto.Location, error)
}

const (
	// runGap is how far apart two records the store reads itself may sit
	// and still share a run; runSpan bounds the bytes one run covers.
	runGap  = 64 << 10
	runSpan = 16 << 20

	// runTTL is long enough for a reader that follows a file's runs a
	// few at a time, so the last of them waits behind the others.
	runTTL = 15 * time.Minute
)

var _ backup.RecordLocator = (*PackStorage)(nil)

type locatedRecord struct {
	index int
	rec   *IndexRecord
}

// LocateRecords implements backup.RecordLocator for objects in archives
// the storage can sign ranges of. Every byte a run covers is a record of
// one of refs. A commit, tree or file, and any object the cache holds, is
// never located: the store serves those itself, though a run spans them
// where they lie between records it locates.
func (ps *PackStorage) LocateRecords(ctx context.Context, refs []*proto.Ref) ([]*proto.LocatedRun, error) {
	signer, ok := ps.storage.(RangeSigner)
	if !ok {
		return nil, nil
	}

	byArchive, err := ps.locateRecords(ScopeOf(ctx), refs, true)
	if err != nil {
		return nil, err
	}

	return ps.signRuns(ctx, signer, byArchive, func(r locatedRecord) bool { return ps.serves(ctx, refs[r.index], r.rec) })
}

// serves reports whether the store answers ref itself rather than
// locating it: metadata always, as it is small and read through into the
// cache, and anything else when the cache already holds it.
func (ps *PackStorage) serves(ctx context.Context, ref *proto.Ref, rec *IndexRecord) bool {
	if proto.ObjectType(rec.Type).Metadata() {
		return true
	}

	if ps.cache == nil {
		return false
	}

	held, err := ps.cache.Has(ctx, ref)

	return err == nil && held
}

// locateRecords finds a committed copy of each of refs in one lookup,
// live only when liveOnly, and groups them by archive, each archive's
// records in offset order.
func (ps *PackStorage) locateRecords(scope Scope, refs []*proto.Ref, liveOnly bool) (map[*archive][]locatedRecord, error) {
	archives, records, err := ps.committedCopies(scope, refs, liveOnly)
	if err != nil {
		return nil, err
	}

	byArchive := make(map[*archive][]locatedRecord)

	for i, a := range archives {
		if a != nil {
			byArchive[a] = append(byArchive[a], locatedRecord{index: i, rec: records[i]})
		}
	}

	for _, located := range byArchive {
		sort.Slice(located, func(i, j int) bool { return located[i].rec.Offset < located[j].rec.Offset })
	}

	return byArchive, nil
}

// signRuns signs the runs of byArchive, leaving out the records served
// reports, or returns none when the storage cannot sign them.
func (ps *PackStorage) signRuns(ctx context.Context, signer RangeSigner, byArchive map[*archive][]locatedRecord, served func(locatedRecord) bool) ([]*proto.LocatedRun, error) {
	var runs []*proto.LocatedRun

	for a, records := range byArchive {
		for start := 0; start < len(records); {
			// a signed run covers records asked for and no byte between them
			end, _ := runOf(records, start, 0)

			located := slices.DeleteFunc(slices.Clone(records[start:end]), served)
			start = end

			if len(located) == 0 {
				continue
			}

			_, span := runOf(located, 0, runSpan)

			run, err := ps.signRun(ctx, signer, a, located, span)
			if errors.Is(err, ErrNoSignedURL) {
				return nil, nil
			}

			if err != nil {
				return nil, err
			}

			runs = append(runs, run)
		}
	}

	return runs, nil
}

var _ backup.RecordReader = (*PackStorage)(nil)

// recordReaders bounds the runs one ReadRecords call reads at once.
const recordReaders = 16

// ReadRecords implements backup.RecordReader with one range read per run
// of neighbouring records.
func (ps *PackStorage) ReadRecords(ctx context.Context, refs []*proto.Ref) ([]*proto.Object, error) {
	return ps.readRecords(ctx, refs, true)
}

// readRecords is ReadRecords, reading copies the last collection found
// unreachable too unless liveOnly.
func (ps *PackStorage) readRecords(ctx context.Context, refs []*proto.Ref, liveOnly bool) ([]*proto.Object, error) {
	ps.touchSessionOf(ctx)

	byArchive, err := ps.locateRecords(ScopeOf(ctx), refs, liveOnly)
	if err != nil {
		return nil, err
	}

	objects := make([]*proto.Object, len(refs))

	var reads sync.WaitGroup
	slots := make(chan struct{}, recordReaders)

	for a, located := range byArchive {
		if !a.readOnlyNow() {
			continue
		}

		var uncached []locatedRecord

		for _, r := range located {
			if cached := ps.getCache(ctx, refs[r.index]); cached != nil {
				objects[r.index] = cached
			} else {
				uncached = append(uncached, r)
			}
		}

		for start := 0; start < len(uncached); {
			end, span := runOf(uncached, start, runGap)
			run := uncached[start:end]

			slots <- struct{}{}
			reads.Go(func() {
				defer func() { <-slots }()

				ps.readRun(ctx, a, refs, run, span, objects)
			})

			start = end
		}
	}

	reads.Wait()

	return objects, nil
}

// readRun reads the records of one run in a single range read and fills
// in their objects; one it cannot read stays nil.
func (ps *PackStorage) readRun(ctx context.Context, a *archive, refs []*proto.Ref, run []locatedRecord, span int64, objects []*proto.Object) {
	from := int64(run[0].rec.Offset)
	start := time.Now()

	buf, _, err := a.readSpan(ctx, from, span)
	if err != nil {
		ps.logger.Warn("reading a run of records failed, reading them one at a time", "archive", a.name, "offset", from, "length", span, "err", err)
		return
	}

	latency := float64(time.Since(start)) / float64(time.Millisecond)

	for _, r := range run {
		offset := int64(r.rec.Offset) - from

		obj, err := a.objectFromRecord(ctx, refs[r.index], r.rec, buf[offset:offset+int64(r.rec.Length)], latency)
		if err != nil {
			continue
		}

		objects[r.index] = obj
		_ = ps.putWriteCache(ctx, obj, nil)
	}
}

// runOf extends the run starting at start while each record lies at most
// gap bytes past the run, and returns where it ends and how many bytes it
// covers. The same record twice overlaps itself, which a run takes in its
// stride.
func runOf(records []locatedRecord, start int, gap int64) (int, int64) {
	from := int64(records[start].rec.Offset)
	reach := from + int64(records[start].rec.Length)

	end := start + 1
	for ; end < len(records); end++ {
		offset := int64(records[end].rec.Offset)
		grown := max(reach, offset+int64(records[end].rec.Length))

		if offset-reach > gap || grown-from > runSpan {
			break
		}

		reach = grown
	}

	return end, reach - from
}

func (ps *PackStorage) signRun(ctx context.Context, signer RangeSigner, a *archive, records []locatedRecord, span int64) (*proto.LocatedRun, error) {
	from := int64(records[0].rec.Offset)

	location, err := signer.SignRange(ctx, a.archiveName(), from, span, runTTL)
	if err != nil {
		return nil, err
	}

	if a.atRest != nil {
		location.AtRestKey = a.atRest.shared
	}

	run := &proto.LocatedRun{Location: location, Records: make([]*proto.LocatedRecord, len(records))}
	for i, r := range records {
		run.Records[i] = &proto.LocatedRecord{Index: uint32(r.index), Offset: int64(r.rec.Offset) - from, Length: int64(r.rec.Length)}
	}

	return run, nil
}

// DecodeRecord turns the body a Location yields back into its object,
// opening it with the location's at-rest key when it is sealed.
func DecodeRecord(record []byte, atRestKey []byte) (*proto.Object, error) {
	headerBytes, sealed, err := splitRecord(record)
	if err != nil {
		return nil, err
	}

	header, err := proto.NewObjectHeaderFromBytes(headerBytes)
	if err != nil {
		return nil, err
	}

	var key *archiveKey
	if len(atRestKey) != 0 {
		key, err = parseArchiveKey(atRestKey)
		if err != nil {
			return nil, err
		}
	}

	stored, err := openAtRest(key, header, sealed)
	if err != nil {
		return nil, err
	}

	return proto.ObjectFromStored(header, stored)
}
