package mysql

import (
	"fmt"
	"slices"
	"time"

	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"
)

// binlogCommit is what the binlog set's latest commit holds.
type binlogCommit struct {
	meta     map[string]string
	identity Identity
	hasID    bool
	chain    int
	// chains are the files of each chain, by number
	chains map[int][]*proto.TreeNode
	// key opens the commit's files
	key *storekey.Key
}

// Window decides which binlogs a binlog set keeps from its dumps, newest
// first: in each chain, those from the start file of the chain's oldest
// dump received within window on. The newest dump counts as received
// within window. Chains no such dump belongs to are let go. ok is false
// when no dump records a position, and then every binlog is kept.
func Window(dumps []*proto.Commit, window time.Duration, now time.Time) (map[int]string, bool) {
	since := now.Add(-window).UnixNano()
	cutoffs := map[int]string{}

	for i, dump := range dumps {
		meta := dump.GetMetadata()

		file := meta[MetaBinlogFile]
		if _, _, ok := splitBinlog(file); !ok {
			continue
		}

		if i > 0 && dump.GetReceivedAtNs() < since {
			continue
		}

		chain := chainOf(meta)
		if was, ok := cutoffs[chain]; !ok || compareBinlogs(file, was) < 0 {
			cutoffs[chain] = file
		}
	}

	return cutoffs, len(cutoffs) > 0
}

// newestStart is the start file of the newest dump of chain, or empty when
// the newest dump is of another chain.
func newestStart(dumps []*proto.Commit, chain int) string {
	if len(dumps) == 0 || chainOf(dumps[0].GetMetadata()) != chain {
		return ""
	}

	return dumps[0].GetMetadata()[MetaBinlogFile]
}

// checkContiguous reports binlogs missing from names, sorted binlogs of one
// chain, from start on; over all of names when start is empty. A partial
// binlog lacks its end, so only the last may be one.
func checkContiguous(names []string, start string) error {
	i := 0
	if start != "" {
		i = slices.IndexFunc(names, func(name string) bool { return compareBinlogs(name, start) >= 0 })
		if i < 0 {
			return nil
		}

		if compareBinlogs(names[i], start) != 0 {
			return gap(start, names[i], true)
		}
	}

	for ; i+1 < len(names); i++ {
		if binlog, ok := isPartial(names[i]); ok {
			return fmt.Errorf("%w: the end of %s; the server purged it before goback received it, and a new dump is needed", ErrGap, binlog)
		}

		if !follows(names[i], names[i+1]) {
			return gap(names[i], names[i+1], false)
		}
	}

	return nil
}

// gap names the binlogs missing before next: from first on when
// inclusive, else after it.
func gap(first, next string, inclusive bool) error {
	base, from, _ := splitBinlog(first)
	_, to, _ := splitBinlog(next)

	if !inclusive {
		from++
	}

	missing := binlogName(base, from, next)
	if to-1 > from {
		missing += " to " + binlogName(base, to-1, next)
	}

	return fmt.Errorf("%w: %s; the server purged them before goback read them, and a new dump is needed", ErrGap, missing)
}
