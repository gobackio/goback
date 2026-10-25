package mysql

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

// spoolCommit commits what a receiver's spool holds to a chain of the
// binlog set: the whole binlogs, and the one being received as
// <binlog>.partial in place of the chain's previous partial one. Each
// holds every binlog back to the chain's oldest dump inside Window.
type spoolCommit struct {
	// Walker stores the commit; its Root is the spool's directory.
	Walker  *backup.Walker
	Spool   Spool
	DumpSet string
	Window  time.Duration
	Now     func() time.Time
	Chain   int
	Server  *Server
	ID      Identity
	// Files are the spool's whole binlogs to commit.
	Files []string
	Open  *OpenBinlog
	// Confirmed, when set, is when the server last confirmed it had
	// nothing past what Files and Open hold.
	Confirmed time.Time

	// copies, when set, keeps what one commit reads and writes for the
	// next; Run rotates it
	copies *cache.Recent
}

// Run commits and then deletes the committed whole binlogs from the spool.
// It commits nothing, and returns nil, when there is nothing to commit. It
// refuses binlogs of another server and binlogs missing since the newest
// dump of the chain, and then leaves the spool as it was.
func (c *spoolCommit) Run(ctx context.Context) (*backup.WalkResult, error) {
	if len(c.Files) == 0 && c.Open == nil {
		return nil, nil
	}

	w := c.Walker

	// the walk diffs against the commit and tree previous reads
	copies := cache.Copies(cache.NewMemory())
	if c.copies != nil {
		c.copies.Rotate()
		copies = c.copies
	}

	objects := w.Objects
	w.Objects = cache.New(copies, objects)
	defer func() { w.Objects = objects }()

	var held *binlogCommit

	ref, err := w.Index.LatestCommit(ctx, w.Set)
	switch {
	case errors.Is(err, backup.ErrNotFound):
	case err != nil:
		return nil, err
	default:
		if held, err = readBinlogCommit(ctx, w.Objects, w.Key, ref); err != nil {
			return nil, err
		}
	}

	if held != nil && held.hasID && !held.identity.Same(c.ID) {
		return nil, fmt.Errorf("%w: the server is %s, the set's %s", ErrForeignBinlog, c.ID, held.identity)
	}

	commits, err := w.Index.CommitInfo(ctx, c.DumpSet, c.Now(), maxDumps)
	if err != nil {
		return nil, fmt.Errorf("listing the dumps: %w", err)
	}

	dumps := dumpCommits(commits, c.ID)
	cutoffs, windowed := Window(dumps, c.Window, c.Now())
	cutoff := cutoffs[c.Chain]

	keep := func(name string) bool { return cutoff == "" || compareBinlogs(name, cutoff) >= 0 }
	needed := func(n int) bool { _, ok := cutoffs[n]; return !windowed || ok }

	var previous []string
	if held != nil {
		for _, node := range held.chains[c.Chain] {
			previous = append(previous, string(node.Stat.Name))
		}
	}

	opened := make(map[string]time.Time, len(c.Files)+1)

	for _, name := range c.Files {
		at, err := c.check(c.Spool.path(name), name)
		if err != nil {
			return nil, err
		}

		opened[name] = at
	}

	if c.Open != nil {
		at, err := c.check(c.Open.Path, c.Open.Name)
		if err != nil {
			return nil, err
		}

		opened[c.Open.Name+partialSuffix] = at
	}

	included := make(map[string]bool, len(c.Files))
	for _, name := range c.Files {
		included[name] = true
	}

	var growing []backup.Growing

	open := ""
	if c.Open != nil {
		open = c.Open.Name + partialSuffix
		growing = append(growing, backup.Growing{Name: open, Path: c.Open.Path, Size: c.Open.Size, ModTime: c.Open.ModTime})
	}

	for _, name := range c.Files {
		if !slices.Contains(previous, name+partialSuffix) {
			continue
		}

		info, err := os.Stat(c.Spool.path(name))
		if err != nil {
			return nil, err
		}

		growing = append(growing, backup.Growing{Name: name, Path: c.Spool.path(name), Size: info.Size(), From: name + partialSuffix})
	}

	// a partial binlog gives way to its whole one or a newer partial one of
	// it; one whose end the server purged stays
	carry := func(name string) bool {
		if !keep(name) || included[name] || name == open {
			return false
		}

		binlog, partial := isPartial(name)

		return !partial || !included[binlog] && (c.Open == nil || c.Open.Name != binlog)
	}

	names := slices.Clone(c.Files)
	if open != "" {
		names = append(names, open)
	}

	for _, name := range previous {
		if carry(name) {
			names = append(names, name)
		}
	}

	slices.SortStableFunc(names, compareBinlogs)

	if err := checkContiguous(names, newestStart(dumps, c.Chain)); err != nil {
		return nil, err
	}

	w.Subdir = chainDir(c.Chain)
	w.Include = func(rel string) bool {
		return included[rel] && !slices.ContainsFunc(growing, func(g backup.Growing) bool { return g.Name == rel })
	}
	w.Carry = func(name string) bool {
		if n, ok := parseChainDir(name); ok {
			return needed(n)
		}

		return carry(name)
	}
	w.Growing = growing

	w.Metadata = map[string]string{
		MetaFlavor:       "mysql",
		MetaVersion:      c.Server.Version,
		MetaChain:        strconv.Itoa(c.Chain),
		MetaBinlogFormat: c.Server.BinlogFormat,
		MetaCapture:      CaptureReplication,
		MetaFirstBinlog:  names[0],
		MetaLastBinlog:   lastWhole(names),
		MetaDumpSet:      c.DumpSet,
		MetaWindow:       c.Window.String(),
		backup.MetaKind:  backup.KindMySQLBinlog,
	}
	c.ID.metadata(w.Metadata)

	last := names[len(names)-1]
	w.Metadata[MetaEndBinlog], _ = isPartial(last)

	var lastAt time.Time

	switch {
	case last == open:
		w.Metadata[MetaEndPos] = strconv.FormatInt(c.Open.Size, 10)
		lastAt = c.Open.ModTime
	case included[last]:
		if info, err := os.Stat(c.Spool.path(last)); err == nil {
			w.Metadata[MetaEndPos] = strconv.FormatInt(info.Size(), 10)
			lastAt = info.ModTime()
		}
	default:
		for _, node := range held.chains[c.Chain] {
			if string(node.Stat.Name) == last {
				w.Metadata[MetaEndPos] = strconv.FormatInt(node.Stat.GetSize(), 10)
				lastAt = time.Unix(0, node.Stat.GetMtimeNs())
			}
		}
	}

	from, to := c.coverage(held, names, opened, lastAt)
	if !from.IsZero() {
		w.Metadata[MetaCoveredFrom] = formatTime(from)
	}

	if !to.IsZero() {
		w.Metadata[MetaCoveredTo] = formatTime(to)
	}

	result, err := w.Run(ctx)
	if err != nil {
		return nil, err
	}

	return result, c.Spool.Remove(c.Files...)
}

// check checks that the binlog at path opens as one the server wrote, and
// returns when the server opened it.
func (c *spoolCommit) check(path, name string) (time.Time, error) {
	h, err := readBinlogFileStart(path)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s: %w", name, err)
	}

	if h.serverID != c.Server.ServerID {
		return time.Time{}, fmt.Errorf("%w: %s was written by server_id %d, the server is %d", ErrForeignBinlog, name, h.serverID, c.Server.ServerID)
	}

	return time.Unix(int64(h.timestamp), 0), nil
}

// coverage is when the stretch of names, the commit's binlogs of its
// chain, that runs unbroken to their end starts, and up to when it is
// complete, given when the server opened the binlogs read from the spool
// and the time of the last event; a zero time is one it can't tell.
func (c *spoolCommit) coverage(held *binlogCommit, names []string, opened map[string]time.Time, lastAt time.Time) (time.Time, time.Time) {
	start := stretchStart(names)
	to := latest(lastAt, c.Confirmed)

	var previous []*proto.TreeNode
	if held != nil {
		previous = held.chains[c.Chain]
	}

	if held != nil && held.chain == c.Chain {
		to = latest(to, parseTime(held.meta[MetaCoveredTo]))

		before := make([]string, len(previous))
		for i, node := range previous {
			before[i] = string(node.Stat.Name)
		}

		was, _ := isPartial(stretchStart(before))
		now, _ := isPartial(start)

		if from := parseTime(held.meta[MetaCoveredFrom]); !from.IsZero() && was == now {
			return from, to
		}
	}

	if from, ok := opened[start]; ok {
		return from, to
	}

	// a whole binlog ends with the Rotate event the server writes as it
	// opens the next
	for _, node := range previous {
		name := string(node.Stat.Name)
		if _, partial := isPartial(name); !partial && follows(name, start) {
			return time.Unix(0, node.Stat.GetMtimeNs()), to
		}
	}

	return time.Time{}, to
}

// stretchStart is the first of names, sorted binlogs of one chain, after
// the last one missing or held only in part.
func stretchStart(names []string) string {
	for i := len(names) - 1; i > 0; i-- {
		if _, partial := isPartial(names[i-1]); partial || !follows(names[i-1], names[i]) {
			return names[i]
		}
	}

	if len(names) == 0 {
		return ""
	}

	return names[0]
}
