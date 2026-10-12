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
	objects := w.Objects
	w.Objects = cache.New(cache.NewMemory(), objects)
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

	for _, name := range c.Files {
		if err := c.check(c.Spool.path(name), name); err != nil {
			return nil, err
		}
	}

	if c.Open != nil {
		if err := c.check(c.Open.Path, c.Open.Name); err != nil {
			return nil, err
		}
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
	}
	c.ID.metadata(w.Metadata)

	last := names[len(names)-1]
	w.Metadata[MetaEndBinlog], _ = isPartial(last)

	switch {
	case last == open:
		w.Metadata[MetaEndPos] = strconv.FormatInt(c.Open.Size, 10)
	case included[last]:
		if info, err := os.Stat(c.Spool.path(last)); err == nil {
			w.Metadata[MetaEndPos] = strconv.FormatInt(info.Size(), 10)
		}
	default:
		for _, node := range held.chains[c.Chain] {
			if string(node.Stat.Name) == last {
				w.Metadata[MetaEndPos] = strconv.FormatInt(node.Stat.GetSize(), 10)
			}
		}
	}

	result, err := w.Run(ctx)
	if err != nil {
		return nil, err
	}

	return result, c.Spool.Remove(c.Files...)
}

// check checks that the binlog at path opens as one the server wrote.
func (c *spoolCommit) check(path, name string) error {
	serverID, err := readBinlogFileServerID(path)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}

	if serverID != c.Server.ServerID {
		return fmt.Errorf("%w: %s was written by server_id %d, the server is %d", ErrForeignBinlog, name, serverID, c.Server.ServerID)
	}

	return nil
}
