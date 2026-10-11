package postgres

// resumePoint is where streaming starts again after a restart: the start
// of the segment after the newest whole segment of names, or of the
// newest segment names hold only as partial. Streaming always restarts at
// a segment's start, and the open segment is received again from its
// first byte. ok is false when names hold no segment.
func resumePoint(names []string, segSize uint64) (timeline uint32, lsn uint64, ok bool) {
	var best uint64

	for _, name := range names {
		var n uint64

		if segment, partial := isPartial(name); partial {
			n = segmentNumber(segment, segSize)
		} else if isSegment(name) {
			n = segmentNumber(name, segSize) + 1
		} else {
			continue
		}

		t := segmentTimeline(name)
		if !ok || n > best || n == best && t > timeline {
			best, timeline, ok = n, t, true
		}
	}

	return timeline, best * segSize, ok
}

// gapStart is where streaming continues past WAL the server no longer has:
// the start of the oldest base backup that started after from, which
// begins a sequence the WAL set can restore again. ok is false without
// one.
func gapStart(starts []string, from, segSize uint64) (timeline uint32, lsn uint64, ok bool) {
	for _, start := range starts {
		if !isSegment(start) {
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
