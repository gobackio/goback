package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/cache"
)

// WALBackup commits what a receiver's spool holds to the WAL set. Every
// commit holds each WAL file back to the oldest base backup inside Window,
// so any one commit replays on its own.
type WALBackup struct {
	// Walker stores the commit; its Root is the spool's directory and its
	// Set the WAL set. Run sets its Include, Carry, Growing and Metadata.
	Walker *backup.Walker
	Spool  Spool
	// BaseSet is the set the cluster's base backups go to.
	BaseSet string
	Window  time.Duration
	Now     func() time.Time
	// Files are the spool's whole files to commit.
	Files []string
	// Open, when set, is the segment still being received; the commit
	// holds it up to its length as <name>.partial, in place of the previous
	// commit's partial segment of its timeline.
	Open *OpenSegment
}

// OpenSegment is a WAL segment a receiver is still writing.
type OpenSegment struct {
	// Name is the segment's name.
	Name string
	// Path is where it is on disk.
	Path string
	// Size is how much of it holds whole records.
	Size int64
}

// maxBases bounds the base backups read to find the cutoff.
const maxBases = 1000

// Run commits Files and Open and then deletes Files from the spool. It
// commits nothing, and returns nil, when both are empty. It refuses WAL
// from another cluster than the set's and WAL that skips a segment, and
// then leaves the spool as it was.
func (b *WALBackup) Run(ctx context.Context) (*backup.WalkResult, error) {
	spooled := b.Files

	if len(spooled) == 0 && b.Open == nil {
		return nil, nil
	}

	w := b.Walker

	// the walk diffs against the commit and tree previous reads
	objects := w.Objects
	w.Objects = cache.New(cache.NewMemory(), objects)
	defer func() { w.Objects = objects }()

	held, systemID, err := b.previous(ctx)
	if err != nil {
		return nil, err
	}

	bases, err := w.Index.CommitInfo(ctx, b.BaseSet, b.Now(), maxBases)
	if err != nil {
		return nil, fmt.Errorf("listing the base backups: %w", err)
	}

	cutoff := func(string) bool { return true }
	if name, ok := Cutoff(bases, b.Window, b.Now()); ok {
		cutoff = Carry(name)
	}

	starts := make(map[string]bool, len(bases))
	for _, base := range bases {
		starts[base.GetMetadata()[MetaStartWALFile]] = true
	}

	var segSize uint64
	for _, name := range spooled {
		segment, ok := isPartial(name)
		if !ok {
			if !isSegment(name) {
				continue
			}

			segment = name
		}

		id, size, err := checkFile(b.Spool.path(name), segment, systemID)
		if err != nil {
			return nil, err
		}

		systemID, segSize = id, size
	}

	if b.Open != nil {
		id, size, err := checkFile(b.Open.Path, b.Open.Name, systemID)
		if err != nil {
			return nil, err
		}

		systemID, segSize = id, size
	}

	growing := b.growing(spooled, held)
	carry := b.carry(cutoff, spooled)

	names := slices.Clone(spooled)
	for _, g := range growing {
		if !slices.Contains(names, g.Name) {
			names = append(names, g.Name)
		}
	}

	for _, name := range held {
		if carry(name) && !slices.Contains(names, name) {
			names = append(names, name)
		}
	}

	slices.Sort(names)

	if segSize != 0 {
		if err := checkContiguous(names, segSize, starts); err != nil {
			return nil, err
		}
	}

	w.Include = func(rel string) bool {
		return slices.Contains(spooled, rel) && !slices.ContainsFunc(growing, func(g backup.Growing) bool { return g.Name == rel })
	}
	w.Carry = carry
	w.Growing = growing
	w.Metadata = map[string]string{
		MetaFirstWALFile: firstSegment(names),
		MetaLastWALFile:  lastSegment(names),
		MetaCapture:      CaptureReplication,
	}

	if systemID != 0 {
		w.Metadata[MetaSystemID] = strconv.FormatUint(systemID, 10)
	}

	if timeline, end, ok := b.end(names, segSize); ok {
		w.Metadata[MetaTimeline] = strconv.FormatUint(uint64(timeline), 10)
		w.Metadata[MetaEndLSN] = FormatLSN(end)
	}

	result, err := w.Run(ctx)
	if err != nil {
		return nil, err
	}

	return result, b.Spool.Remove(spooled...)
}

// growing lists the files the commit continues from the previous commit's
// versions instead of reading them whole: the open segment, and the
// segments that were open then and are whole now.
func (b *WALBackup) growing(spooled, held []string) []backup.Growing {
	var growing []backup.Growing

	if b.Open != nil {
		growing = append(growing, backup.Growing{Name: b.Open.Name + partialSuffix, Path: b.Open.Path, Size: b.Open.Size})
	}

	for _, name := range spooled {
		from := name + partialSuffix
		if _, ok := isPartial(name); ok {
			from = name
		}

		if !slices.Contains(held, from) {
			continue
		}

		info, err := os.Stat(b.Spool.path(name))
		if err != nil {
			continue
		}

		growing = append(growing, backup.Growing{Name: name, Path: b.Spool.path(name), Size: info.Size(), From: from})
	}

	return growing
}

// carry keeps a previous commit's file inside the cutoff unless the commit
// replaces it: a partial segment whose segment is whole now, or whose
// timeline has another segment open.
func (b *WALBackup) carry(cutoff func(string) bool, spooled []string) func(string) bool {
	return func(name string) bool {
		if !cutoff(name) {
			return false
		}

		segment, ok := isPartial(name)
		if !ok {
			return true
		}

		if slices.Contains(spooled, segment) {
			return false
		}

		return b.Open == nil || b.Open.Name[:8] != segment[:8] || b.Open.Name == segment
	}
}

// end is where the WAL of names ends: on the newest timeline, at the end of
// the open segment or of the last whole one.
func (b *WALBackup) end(names []string, segSize uint64) (uint32, uint64, bool) {
	if segSize == 0 {
		return 0, 0, false
	}

	var (
		timeline uint32
		end      uint64
	)

	consider := func(t uint32, lsn uint64) {
		if t > timeline || t == timeline && lsn > end {
			timeline, end = t, lsn
		}
	}

	for _, name := range names {
		if isSegment(name) {
			consider(segmentTimeline(name), (segmentNumber(name, segSize)+1)*segSize)
		}
	}

	if b.Open != nil {
		consider(segmentTimeline(b.Open.Name), segmentNumber(b.Open.Name, segSize)*segSize+uint64(b.Open.Size))
	}

	return timeline, end, end != 0
}

// previous lists the WAL files the set's latest commit holds, and the
// cluster it belongs to; a set without commits holds none.
func (b *WALBackup) previous(ctx context.Context) ([]string, uint64, error) {
	w := b.Walker

	commit, err := latestCommit(ctx, w.Index, w.Objects, w.Set)
	if commit == nil || err != nil {
		return nil, 0, err
	}

	systemID, err := commitSystemID(commit)
	if err != nil {
		return nil, 0, fmt.Errorf("the WAL set's %w", err)
	}

	trees, err := prefetch(ctx, w.Objects, commit.GetTree())
	if err != nil {
		return nil, 0, err
	}

	key, err := backup.CommitKey(ctx, trees, commit, w.Key)
	if err != nil {
		return nil, 0, err
	}

	tree, err := backup.OpenTree(ctx, trees, commit.GetTree(), key, nil)
	if err != nil {
		return nil, 0, err
	}

	names := make([]string, 0, len(tree.Nodes))
	for _, node := range tree.Nodes {
		names = append(names, string(node.Stat.Name))
	}

	return names, systemID, nil
}

// latestCommit is the set's latest commit, or nil for a set without one.
func latestCommit(ctx context.Context, index backup.Index, objects backup.Getter, set string) (*proto.Commit, error) {
	ref, err := index.LatestCommit(ctx, set)
	if errors.Is(err, backup.ErrNotFound) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	obj, err := objects.Get(ctx, ref)
	if err != nil {
		return nil, err
	}

	if obj.GetCommit() == nil {
		return nil, fmt.Errorf("object %x is not a commit", ref.Hash)
	}

	return obj.GetCommit(), nil
}

// prefetch serves the tree at ref and its splits from one GetTree where
// the store streams trees: a WAL set's tree splits into hundreds.
func prefetch(ctx context.Context, store backup.Getter, ref *proto.Ref) (backup.Getter, error) {
	fetcher, ok := store.(backup.TreeFetcher)
	if !ok {
		return store, nil
	}

	objects, err := fetcher.GetTree(ctx, ref, 0)
	if errors.Is(err, backup.ErrNotImplemented) {
		return store, nil
	}

	if err != nil {
		return nil, err
	}

	fetched := prefetched{Getter: store, objects: make(map[string]*proto.Object, len(objects))}
	for _, obj := range objects {
		fetched.objects[string(obj.Ref().Hash)] = obj
	}

	return fetched, nil
}

type prefetched struct {
	backup.Getter
	objects map[string]*proto.Object
}

func (p prefetched) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	if obj, ok := p.objects[string(ref.GetHash())]; ok {
		return obj, nil
	}

	return p.Getter.Get(ctx, ref)
}

// checkFile checks the segment at path as checkSegment does, and returns
// the cluster's segment size too.
func checkFile(path, segment string, systemID uint64) (uint64, uint64, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()

	hdr, err := readSegmentHeader(file)
	if err != nil {
		return 0, 0, fmt.Errorf("%s: %w", segment, err)
	}

	if _, err := file.Seek(0, 0); err != nil {
		return 0, 0, err
	}

	id, err := checkSegment(segment, file, systemID)

	return id, uint64(hdr.segSize), err
}

// firstSegment is the first segment names hold, whole or partial.
func firstSegment(names []string) string {
	for _, name := range names {
		if isSegment(name) {
			return name
		}

		if segment, ok := isPartial(name); ok {
			return segment
		}
	}

	return ""
}

func lastSegment(names []string) string {
	for i := len(names) - 1; i >= 0; i-- {
		if isSegment(names[i]) {
			return names[i]
		}
	}

	return ""
}
