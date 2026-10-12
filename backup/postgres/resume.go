package postgres

import "fmt"

// reach reads how far the WAL of names goes on h's path: from, where
// streaming starts again after a restart, at the end of the newest whole
// segment or the start of the newest segment held only as partial, which is
// received again from its first byte, on the timeline that goes on there;
// extent, the end of the newest segment either way; and first, the start of
// the oldest segment. A timeline's WAL counts only up to where h leaves it.
// With h nil every segment counts, and on a tie the newer timeline wins. ok
// is false when names hold no segment of h's path.
func (h history) reach(names []string, segSize uint64) (from position, extent, first uint64, ok bool) {
	for _, name := range names {
		segment, partial := isPartial(name)
		if !partial {
			if !isSegment(name) {
				continue
			}

			segment = name
		}

		t, n := segmentTimeline(segment), segmentNumber(segment, segSize)
		start, end := n*segSize, (n+1)*segSize

		limit := uint64(endless)
		if h != nil {
			if !h.holds(segment, segSize) {
				continue
			}

			s, _ := h.span(t)
			limit = s.end
		}

		resume := end
		if partial {
			resume = start
		}

		resume = min(resume, limit)

		if !ok || resume > from.lsn || resume == from.lsn && t > from.timeline {
			from = position{timeline: t, lsn: resume}
		}

		if !ok || start < first {
			first = start
		}

		extent = max(extent, min(end, limit))
		ok = true
	}

	if ok && h != nil {
		if s, _ := h.span(from.timeline); from.lsn >= s.end {
			from.timeline = h.at(from.lsn)
		}
	}

	from.lsn -= from.lsn % segSize

	return from, extent, first, ok
}

// resumption is where streaming starts again.
type resumption struct {
	position
	// fork is set when it starts where the server's history leaves the
	// WAL set's.
	fork bool
}

// resumeAt is where streaming starts again on a server with history server
// after the names the WAL set and spool hold; set is the history of their
// newest timeline, nil without its file. Where the server's history leaves
// set's before the WAL of names ends, as after a restore of the cluster to
// an earlier moment, it starts at the fork's segment on the server's
// timeline there. It is nil when names hold no segment. A fork before the
// WAL of names is a *gapError, and one onto a timeline no newer than
// theirs ErrGap, since restore follows the newest timeline the set holds.
func resumeAt(names []string, set, server history, segSize uint64) (*resumption, error) {
	from, extent, first, ok := set.reach(names, segSize)
	if !ok {
		return nil, nil
	}

	if set == nil {
		return &resumption{position: from}, nil
	}

	fork, ok := set.leaves(server, extent)
	if !ok {
		return &resumption{position: from}, nil
	}

	next := server.at(fork)

	if newest := newestTimeline(names); next <= newest {
		return nil, fmt.Errorf("%w: the server's history leaves the WAL set's at %s for timeline %d, and the set already holds timeline %d", ErrGap, FormatLSN(fork), next, newest)
	}

	if fork < first {
		return nil, &gapError{what: fmt.Sprintf("the server's history leaves the WAL set's at %s, before the oldest WAL the set holds", FormatLSN(fork)), from: fork}
	}

	return &resumption{position: position{timeline: next, lsn: fork - fork%segSize}, fork: true}, nil
}

// gapStart is where streaming continues past WAL the WAL set can't go on
// with from from: the start of the oldest base backup on the server's
// history that started after from, which begins a sequence the WAL set can
// restore again. ok is false without one.
func gapStart(starts []string, from uint64, server history, segSize uint64) (timeline uint32, lsn uint64, ok bool) {
	for _, start := range starts {
		if !isSegment(start) || !server.holds(start, segSize) {
			continue
		}

		n := segmentNumber(start, segSize)
		if n*segSize <= from {
			continue
		}

		if !ok || n*segSize < lsn {
			timeline, lsn, ok = segmentTimeline(start), n*segSize, true
		}
	}

	return timeline, lsn, ok
}
