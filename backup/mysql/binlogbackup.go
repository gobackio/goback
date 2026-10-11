package mysql

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/cache"
)

// BinlogBackup commits the server's closed binary logs to the binlog set,
// each chain in a directory of its own. Every commit holds each binlog back
// to the oldest dump of its chain inside Window, so any one commit replays
// on its own.
type BinlogBackup struct {
	// Walker stores the commit; its Set is the binlog set. Run sets its
	// Root, Subdir, Include, Carry and Metadata.
	Walker *backup.Walker
	Client Client
	// Dir is where the server keeps its binlogs; empty means the directory
	// of @@log_bin_basename, else @@datadir.
	Dir string
	// DumpSet is the set the server's dumps go to.
	DumpSet string
	Window  time.Duration
	Now     func() time.Time
}

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

// last is the newest file the commit holds of chain.
func (b *binlogCommit) last(chain int) string {
	if b == nil || len(b.chains[chain]) == 0 {
		return ""
	}

	files := b.chains[chain]

	return string(files[len(files)-1].Stat.Name)
}

// Run rotates the binary log when it holds anything new, then commits the
// closed binlogs. It commits nothing, and returns nil, when there is no
// new closed binlog. It fails, committing nothing, for binlogs of another
// server (ErrForeignBinlog), binlogs missing since the newest dump (ErrGap),
// and a held binlog that changed on disk (ErrBinlogChanged).
func (b *BinlogBackup) Run(ctx context.Context) (*backup.WalkResult, error) {
	srv, err := ReadServer(ctx, b.Client)
	if err != nil {
		return nil, err
	}

	if !srv.LogBin {
		return nil, ErrNoBinlog
	}

	w := b.Walker

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

	dir := b.dir(srv)

	writing, err := b.rotate(ctx, srv, dir)
	if err != nil {
		return nil, err
	}

	logs, err := BinaryLogs(ctx, b.Client)
	if err != nil {
		return nil, err
	}

	if len(logs) == 0 {
		return nil, errors.New("the server lists no binary logs")
	}

	active := logs[len(logs)-1].Name
	closed := logs[:len(logs)-1]

	base, _, ok := splitBinlog(active)
	if !ok {
		return nil, fmt.Errorf("the server's binlog %q is not named as a binlog", active)
	}

	id := Identity{ServerID: srv.ServerID, ServerUUID: srv.ServerUUID, Base: base}
	if held != nil && held.hasID && !held.identity.Same(id) {
		return nil, fmt.Errorf("%w: the server is %s, the set's %s", ErrForeignBinlog, id, held.identity)
	}

	commits, err := w.Index.CommitInfo(ctx, b.DumpSet, b.Now(), maxDumps)
	if err != nil {
		return nil, fmt.Errorf("listing the dumps: %w", err)
	}

	dumps := dumpCommits(commits, id)
	chain := nextChain(held, writing, b.recreated(dir, held), dumps)

	cutoffs, windowed := Window(dumps, b.Window, b.Now())
	cutoff := cutoffs[chain]

	keep := func(name string) bool { return cutoff == "" || compareBinlogs(name, cutoff) >= 0 }
	needed := func(c int) bool { _, ok := cutoffs[c]; return !windowed || ok }

	var previous []*proto.TreeNode
	if held != nil {
		previous = held.chains[chain]
	}

	heldByName := make(map[string]*proto.TreeNode, len(previous))
	for _, node := range previous {
		heldByName[string(node.Stat.Name)] = node
	}

	include := map[string]bool{}
	added := 0

	for _, log := range closed {
		if !keep(log.Name) {
			continue
		}

		isNew, err := b.check(dir, log, heldByName[log.Name], srv.ServerID)
		if err != nil {
			return nil, err
		}

		include[log.Name] = true
		if isNew {
			added++
		}
	}

	for name, node := range heldByName {
		if include[name] || !keep(name) {
			continue
		}

		if _, err := os.Stat(filepath.Join(dir, name)); errors.Is(err, os.ErrNotExist) {
			continue
		}

		if _, err := b.check(dir, Binlog{Name: name}, node, srv.ServerID); err != nil {
			return nil, err
		}

		include[name] = true
	}

	if added == 0 && held != nil && held.chain == chain {
		return nil, nil
	}

	// a partial binlog a receiver committed gives way to the whole one
	carried := func(name string) bool {
		binlog, partial := isPartial(name)
		return keep(name) && !include[name] && !(partial && include[binlog])
	}

	names := make([]string, 0, len(include)+len(previous))
	for name := range include {
		names = append(names, name)
	}

	for name := range heldByName {
		if carried(name) {
			names = append(names, name)
		}
	}

	slices.SortFunc(names, compareBinlogs)

	if err := checkContiguous(names, newestStart(dumps, chain)); err != nil {
		return nil, err
	}

	w.Root = dir
	w.Subdir = chainDir(chain)
	w.Include = func(rel string) bool { return include[rel] }
	w.Carry = func(name string) bool {
		if c, ok := parseChainDir(name); ok {
			return needed(c)
		}

		return carried(name)
	}

	w.Metadata = map[string]string{
		MetaFlavor:       "mysql",
		MetaVersion:      srv.Version,
		MetaChain:        strconv.Itoa(chain),
		MetaBinlogFormat: srv.BinlogFormat,
		MetaCapture:      CaptureFiles,
	}
	id.metadata(w.Metadata)

	if len(names) > 0 {
		last := names[len(names)-1]

		w.Metadata[MetaFirstBinlog] = names[0]
		w.Metadata[MetaLastBinlog] = lastWhole(names)
		w.Metadata[MetaEndBinlog], _ = isPartial(last)

		if node := heldByName[last]; node != nil {
			w.Metadata[MetaEndPos] = strconv.FormatInt(node.Stat.GetSize(), 10)
		} else if info, err := os.Stat(filepath.Join(dir, last)); err == nil {
			w.Metadata[MetaEndPos] = strconv.FormatInt(info.Size(), 10)
		}
	}

	return w.Run(ctx)
}

func (b *BinlogBackup) dir(srv *Server) string {
	switch {
	case b.Dir != "":
		return b.Dir
	case srv.BinlogBasename != "":
		return filepath.Dir(srv.BinlogBasename)
	}

	return srv.Datadir
}

// rotate closes the active binlog when it holds more than the events every
// binlog opens with, and returns the binlog that was active.
func (b *BinlogBackup) rotate(ctx context.Context, srv *Server, dir string) (string, error) {
	file, pos, err := Position(ctx, b.Client, srv)
	if err != nil {
		return "", err
	}

	hdr, err := readBinlogFile(filepath.Join(dir, file))
	if err != nil {
		return "", fmt.Errorf("the active binlog %s: %w; is the binlog directory right?", file, err)
	}

	if int64(pos) <= hdr.end {
		return file, nil
	}

	_, err = b.Client.Query(ctx, "FLUSH BINARY LOGS")

	return file, err
}

// recreated reports whether a binlog the set holds of its chain was created
// anew on disk after the held copy was closed, as after RESET MASTER.
func (b *BinlogBackup) recreated(dir string, held *binlogCommit) bool {
	if held == nil {
		return false
	}

	for _, node := range held.chains[held.chain] {
		path := filepath.Join(dir, string(node.Stat.Name))

		info, err := os.Stat(path)
		if err != nil || info.Size() == node.Stat.GetSize() && info.ModTime().UnixNano() == node.Stat.GetMtimeNs() {
			continue
		}

		hdr, err := readBinlogFile(path)
		if err == nil && hdr.created.UnixNano() > node.Stat.GetMtimeNs() {
			return true
		}
	}

	return false
}

// check checks the closed binlog log on disk against what the server says
// of it and, for a binlog the set holds, against the copy held; isNew is
// true for a binlog the set does not hold yet.
func (b *BinlogBackup) check(dir string, log Binlog, held *proto.TreeNode, serverID uint32) (bool, error) {
	path := filepath.Join(dir, log.Name)

	info, err := os.Stat(path)
	if err != nil {
		return false, fmt.Errorf("the closed binlog %s: %w", log.Name, err)
	}

	if held != nil {
		if info.Size() != held.Stat.GetSize() || info.ModTime().UnixNano() != held.Stat.GetMtimeNs() {
			return false, fmt.Errorf("%w: %s, held at %d bytes, is %d bytes on disk with another time", ErrBinlogChanged, log.Name, held.Stat.GetSize(), info.Size())
		}

		return false, nil
	}

	if info.Size() != log.Size {
		return false, fmt.Errorf("%s is %d bytes on disk, the server says %d", log.Name, info.Size(), log.Size)
	}

	hdr, err := readBinlogFile(path)
	if err != nil {
		return false, fmt.Errorf("%s: %w", log.Name, err)
	}

	if hdr.serverID != serverID {
		return false, fmt.Errorf("%w: %s was written by server_id %d, the server is %d", ErrForeignBinlog, log.Name, hdr.serverID, serverID)
	}

	if hdr.inUse {
		return false, fmt.Errorf("%s is marked in use although the server closed it", log.Name)
	}

	return true, nil
}

// nextChain is the chain the server's binlogs go to: the held one, the one
// after when the numbering went backwards since, which writing, the binlog
// the server wrote to, or recreated show, or a newer one a dump opened.
func nextChain(held *binlogCommit, writing string, recreated bool, dumps []*proto.Commit) int {
	chain := 1

	if held != nil {
		chain = held.chain
		if recreated {
			chain++
		} else if last := held.last(chain); last != "" {
			was, _, _ := splitBinlog(last)
			base, _, _ := splitBinlog(writing)

			// the set may hold the binlog the server writes to as partial
			_, partial := isPartial(last)
			if c := compareBinlogs(writing, last); base != was || c < 0 || c == 0 && !partial {
				chain++
			}
		}
	}

	if len(dumps) > 0 {
		chain = max(chain, chainOf(dumps[0].GetMetadata()))
	}

	return chain
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
