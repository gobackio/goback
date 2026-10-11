package mysql

// resumePoint is where receiving starts again: the end of the binlog being
// received, else the start of the newest binlog names hold only as
// partial, else the start of the one after the newest whole one, else the
// start of the binlog dumpStart, the newest dump's. ok is false when there
// is none of these.
func resumePoint(open string, openEnd int64, names []string, dumpStart string) (name string, pos int64, ok bool) {
	if open != "" {
		return open, openEnd, true
	}

	newest, partial := "", false

	for _, n := range names {
		if _, _, isBinlog := splitBinlog(n); !isBinlog {
			continue
		}

		binlog, p := isPartial(n)

		c := compareBinlogs(binlog, newest)
		if newest == "" || c > 0 || c == 0 && !p {
			newest, partial = binlog, p
		}
	}

	switch {
	case newest == "" && dumpStart == "":
		return "", 0, false
	case newest == "":
		return dumpStart, binlogHeaderBytes, true
	case partial:
		return newest, binlogHeaderBytes, true
	}

	base, n, _ := splitBinlog(newest)

	return binlogName(base, n+1, newest), binlogHeaderBytes, true
}

// What error 1236 at a resume point means.
const (
	// lostUnknown is none of the others.
	lostUnknown = iota
	// lostPurged: the server purged the binlog before it was received.
	lostPurged
	// lostReset: the server's binlogs started over, after RESET MASTER or a
	// restore in place.
	lostReset
	// lostTail: the binlog is shorter than what was received of it.
	lostTail
)

// classifyLost says why a server listing logs refused to send binlog file
// from pos.
func classifyLost(logs []Binlog, file string, pos int64) int {
	if len(logs) == 0 {
		return lostUnknown
	}

	oldest, newest := logs[0].Name, logs[len(logs)-1].Name

	base, _, _ := splitBinlog(file)
	if serverBase, _, _ := splitBinlog(newest); serverBase != base {
		return lostReset
	}

	for _, log := range logs {
		if log.Name == file {
			if log.Size < pos {
				return lostTail
			}

			return lostUnknown
		}
	}

	switch {
	case compareBinlogs(newest, file) <= 0:
		return lostReset
	case compareBinlogs(oldest, file) > 0:
		return lostPurged
	}

	return lostUnknown
}
