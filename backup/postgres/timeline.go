package postgres

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"
)

// endless is where the newest timeline of a history ends.
const endless = math.MaxUint64

// history is the WAL a timeline replays, as its history file tells
// Postgres: the LSNs each timeline holds, oldest first, the timeline
// itself last. Where two timelines claim the same LSNs, the newer one holds
// them, as Postgres looks them up; a timeline left holding none has an
// empty span.
type history []span

// span is the WAL of one timeline in a history: from begin up to end.
type span struct {
	timeline   uint32
	begin, end uint64
}

// timelineHistory is the history of a timeline that has no history file:
// timeline 1.
func timelineHistory(timeline uint32) history {
	return history{{timeline: timeline, end: endless}}
}

// parseHistory reads the history file of timeline.
func parseHistory(timeline uint32, content []byte) (history, error) {
	var (
		h    history
		prev uint64
	)

	for _, line := range strings.Split(string(content), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 2 {
			return nil, fmt.Errorf("%s: %q is no timeline switch", historyName(timeline), line)
		}

		t, err := strconv.ParseUint(fields[0], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("%s: %q is no timeline switch", historyName(timeline), line)
		}

		lsn, err := ParseLSN(fields[1])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", historyName(timeline), err)
		}

		if len(h) > 0 && uint32(t) <= h[len(h)-1].timeline {
			return nil, fmt.Errorf("%s: timeline %d follows timeline %d", historyName(timeline), t, h[len(h)-1].timeline)
		}

		h = append(h, span{timeline: uint32(t), begin: prev, end: lsn})
		prev = lsn
	}

	if len(h) > 0 && timeline <= h[len(h)-1].timeline {
		return nil, fmt.Errorf("%s: names timeline %d", historyName(timeline), h[len(h)-1].timeline)
	}

	h = append(h, span{timeline: timeline, begin: prev, end: endless})

	floor := uint64(endless)
	for i := len(h) - 1; i >= 0; i-- {
		h[i].end = min(h[i].end, floor)
		floor = min(floor, h[i].begin)
	}

	return h, nil
}

func (h history) newest() uint32 {
	return h[len(h)-1].timeline
}

// span is the LSNs timeline holds in h; ok is false when it holds none.
func (h history) span(timeline uint32) (span, bool) {
	for _, s := range h {
		if s.timeline == timeline && s.begin < s.end {
			return s, true
		}
	}

	return span{}, false
}

// at is the timeline that holds lsn.
func (h history) at(lsn uint64) uint32 {
	for _, s := range h {
		if s.begin <= lsn && lsn < s.end {
			return s.timeline
		}
	}

	return 0
}

// holds reports whether the segment or partial segment name holds WAL of h.
func (h history) holds(name string, segSize uint64) bool {
	segment, ok := isPartial(name)
	if !ok {
		segment = name
	}

	s, ok := h.span(segmentTimeline(segment))
	n := segmentNumber(segment, segSize)

	return ok && n*segSize < s.end && s.begin < (n+1)*segSize
}

// leaves is the first LSN, up to end, that other places on another timeline
// than h does; ok is false when the two agree up to end.
func (h history) leaves(other history, end uint64) (uint64, bool) {
	points := []uint64{0}
	for _, s := range append(slices.Clone(h), other...) {
		if s.begin < s.end && s.begin <= end {
			points = append(points, s.begin)
		}
	}

	slices.Sort(points)

	for _, p := range points {
		if h.at(p) != other.at(p) {
			return p, true
		}
	}

	return 0, false
}

// newestTimeline is the newest timeline a segment, partial segment or
// history file of names belongs to; zero when they hold none.
func newestTimeline(names []string) uint32 {
	var newest uint32

	for _, name := range names {
		if len(name) >= 8 && (isSegment(name) || strings.HasSuffix(name, partialSuffix) || strings.HasSuffix(name, ".history")) {
			if t, err := strconv.ParseUint(name[:8], 16, 32); err == nil {
				newest = max(newest, uint32(t))
			}
		}
	}

	return newest
}

// walHistory is the history of the newest timeline of names, read with read
// from its history file; nil when names lack that file.
func walHistory(names []string, read func(name string) ([]byte, error)) (history, error) {
	newest := newestTimeline(names)
	if newest <= 1 {
		return timelineHistory(1), nil
	}

	name := historyName(newest)
	if !slices.Contains(names, name) {
		return nil, nil
	}

	content, err := read(name)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", name, err)
	}

	return parseHistory(newest, content)
}

// commitFiles is the files at the top of a commit's tree.
type commitFiles struct {
	names   []string
	nodes   []*proto.TreeNode
	objects backup.ObjectStore
	// key opens the commit's files; nil for a commit written plain.
	key *storekey.Key
}

// openCommitFiles opens commit's tree, reading trees from trees and files
// from objects.
func openCommitFiles(ctx context.Context, trees backup.Getter, objects backup.ObjectStore, commit *proto.Commit, key *storekey.Key) (*commitFiles, error) {
	key, err := backup.CommitKey(ctx, trees, commit, key)
	if err != nil {
		return nil, err
	}

	tree, err := backup.OpenTree(ctx, trees, commit.GetTree(), key, nil)
	if err != nil {
		return nil, err
	}

	names := make([]string, len(tree.Nodes))
	for i, node := range tree.Nodes {
		names[i] = string(node.Stat.Name)
	}

	return &commitFiles{names: names, nodes: tree.Nodes, objects: objects, key: key}, nil
}

// read reads the file name of the tree whole into memory.
func (t *commitFiles) read(ctx context.Context, name string) ([]byte, error) {
	i := slices.Index(t.names, name)
	if i < 0 {
		return nil, fmt.Errorf("%w: %s", backup.ErrNotFound, name)
	}

	return (&backup.Restorer{Store: t.objects, Key: t.key}).ReadFile(ctx, t.nodes[i].Stat, t.nodes[i].Ref)
}

// history is the history of the tree's newest timeline; nil when the tree
// lacks its history file.
func (t *commitFiles) history(ctx context.Context) (history, error) {
	return walHistory(t.names, func(name string) ([]byte, error) { return t.read(ctx, name) })
}
