package mysql

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"strconv"
	"strings"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
)

// Dump commits one mysqldump of the whole server to the dump set.
type Dump struct {
	// Walker stores the commit; its Set is the dump set. Run sets its
	// Stream and Metadata.
	Walker *backup.Walker
	Client Client
	// NoPosition leaves the binary log position out even when log_bin is
	// on, which spares the dump the global read lock and the RELOAD and
	// REPLICATION CLIENT privileges.
	NoPosition bool
	// BinlogSet, when set, is the set the server's binlogs go to; a dump
	// joins the newest chain it has seen, which catches a reset the
	// numbering since the previous dump does not show.
	BinlogSet string
	// Logger is where the dump reports; nil means slog.Default.
	Logger *slog.Logger
}

// maxDumps bounds the dumps read to find the previous one with a position.
const maxDumps = 1000

// Run dumps the server into the set's next commit. A dump of another server
// than the set's fails with ErrForeignServer, and a dump mysqldump did not
// finish leaves the set without a commit.
func (d *Dump) Run(ctx context.Context) (*backup.WalkResult, error) {
	srv, err := ReadServer(ctx, d.Client)
	if err != nil {
		return nil, err
	}

	previous, positioned, err := d.previous(ctx)
	if err != nil {
		return nil, err
	}

	binlogChain, err := d.binlogChain(ctx, srv)
	if err != nil {
		return nil, err
	}

	locked, err := nonTransactional(ctx, d.Client)
	if err != nil {
		return nil, err
	}

	if len(locked) > 0 {
		d.logger().Warn("non-transactional tables: the dump locks all tables until it ends", "tables", strings.Join(locked, ", "))
	}

	version, err := d.Client.DumpVersion(ctx)
	if err != nil {
		return nil, err
	}

	dumpVersion, err := parseDumpVersion(version)
	if err != nil {
		return nil, err
	}

	position := srv.LogBin && !d.NoPosition
	args := DumpArgs(dumpVersion, len(locked) > 0, position, srv.GTIDMode)

	scan := &dumpScanner{}
	out := &lazyDump{ctx: ctx, client: d.Client, args: args}
	defer out.close()

	w := d.Walker
	w.Stream = &backup.Stream{
		Name: DumpFile,
		File: io.TeeReader(out, scan),
		Finish: func() error {
			if err := out.close(); err != nil {
				return err
			}

			meta, err := scan.metadata(srv, position)
			if err != nil {
				return err
			}

			if len(locked) > 0 {
				meta[MetaLock] = "all-tables"
			}

			id, _ := identityOf(meta, meta[MetaBinlogFile])
			if was, ok := identityOf(previous, positioned[MetaBinlogFile]); ok && !was.Same(id) {
				return fmt.Errorf("%w: the dump is of %s, the set of %s", ErrForeignServer, id, was)
			}

			if position {
				meta[MetaChain] = strconv.Itoa(max(dumpChain(positioned, meta[MetaBinlogFile]), binlogChain))
			}

			w.Metadata = meta

			return nil
		},
	}

	return w.Run(ctx)
}

func (d *Dump) logger() *slog.Logger {
	if d.Logger != nil {
		return d.Logger
	}

	return slog.Default()
}

// previous reads the metadata of the set's latest commit and of its newest
// one with a binlog position; either is nil when there is none.
func (d *Dump) previous(ctx context.Context) (map[string]string, map[string]string, error) {
	w := d.Walker

	ref, err := w.Index.LatestCommit(ctx, w.Set)
	if errors.Is(err, backup.ErrNotFound) {
		return nil, nil, nil
	}

	if err != nil {
		return nil, nil, err
	}

	var latest map[string]string

	for i := 0; i < maxDumps && ref != nil && len(ref.Hash) > 0; i++ {
		obj, err := w.Objects.Get(ctx, ref)
		if errors.Is(err, backup.ErrNotFound) && latest != nil {
			break
		}

		if err != nil {
			return nil, nil, err
		}

		commit := obj.GetCommit()
		if latest == nil {
			latest = commit.GetMetadata()
		}

		if commit.GetMetadata()[MetaBinlogFile] != "" {
			return latest, commit.GetMetadata(), nil
		}

		ref = commit.GetParent()
	}

	return latest, nil, nil
}

// binlogChain is the chain the binlog set's latest commit writes to, or 0
// without one of this server.
func (d *Dump) binlogChain(ctx context.Context, srv *Server) (int, error) {
	if d.BinlogSet == "" {
		return 0, nil
	}

	w := d.Walker

	ref, err := w.Index.LatestCommit(ctx, d.BinlogSet)
	if errors.Is(err, backup.ErrNotFound) {
		return 0, nil
	}

	if err != nil {
		return 0, err
	}

	commit, err := getCommit(ctx, w.Objects, ref)
	if err != nil {
		return 0, err
	}

	meta := commit.GetMetadata()
	if id, ok := identityOf(meta, ""); !ok || !id.Same(Identity{ServerID: srv.ServerID, ServerUUID: srv.ServerUUID}) {
		return 0, nil
	}

	return chainOf(meta), nil
}

// dumpChain is the chain a dump that starts in file belongs to: the
// previous positioned dump's, or the one after when the numbering went
// backwards since.
func dumpChain(previous map[string]string, file string) int {
	if previous == nil {
		return 1
	}

	chain := chainOf(previous)

	was, _, _ := splitBinlog(previous[MetaBinlogFile])
	base, _, _ := splitBinlog(file)

	if base != was || compareBinlogs(file, previous[MetaBinlogFile]) < 0 {
		chain++
	}

	return chain
}

// DumpArgs are the arguments mysqldump runs with: version is mysqldump's
// own, lockAll replaces the snapshot with a global read lock for the whole
// dump, and position records the binary log position.
func DumpArgs(version [3]int, lockAll, position, gtid bool) []string {
	args := []string{"--single-transaction"}
	if lockAll {
		args = []string{"--lock-all-tables"}
	}

	args = append(args, "--routines", "--triggers", "--events", "--all-databases", "--flush-privileges")

	if position {
		if versionAtLeast(version, [3]int{8, 0, 26}) {
			args = append(args, "--source-data=2")
		} else {
			args = append(args, "--master-data=2")
		}
	}

	if gtid {
		args = append(args, "--set-gtid-purged=OFF")
	}

	return args
}

var dumpVersionPattern = regexp.MustCompile(`(?:Distrib |Ver )(\d+)\.(\d+)\.(\d+)`)

// parseDumpVersion reads the MySQL version mysqldump --version names: after
// "Distrib" up to 5.7, after "Ver" from 8.0 on.
func parseDumpVersion(s string) ([3]int, error) {
	matches := dumpVersionPattern.FindAllStringSubmatch(s, -1)
	if matches == nil {
		return [3]int{}, fmt.Errorf("no version in mysqldump --version: %q", s)
	}

	m := matches[len(matches)-1]

	var v [3]int
	for i := range v {
		v[i], _ = strconv.Atoi(m[i+1])
	}

	return v, nil
}

// transactional are the engines --single-transaction keeps consistent.
var transactional = []string{"InnoDB", "ndbcluster", "NDB", "TokuDB", "ROCKSDB"}

// nonTransactional lists the user tables whose engine a snapshot does not
// keep consistent.
func nonTransactional(ctx context.Context, c Client) ([]string, error) {
	quoted := make([]string, len(transactional))
	for i, engine := range transactional {
		quoted[i] = "'" + engine + "'"
	}

	rows, err := c.Query(ctx, "SELECT CONCAT(table_schema, '.', table_name) FROM information_schema.tables "+
		"WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys') "+
		"AND engine NOT IN ("+strings.Join(quoted, ", ")+") ORDER BY 1")
	if err != nil {
		return nil, err
	}

	tables := make([]string, 0, len(rows))
	for _, row := range rows {
		tables = append(tables, row[0])
	}

	return tables, nil
}

// lazyDump starts mysqldump on the first read, so a run that fails before
// it reads the stream never starts one.
type lazyDump struct {
	ctx    context.Context
	client Client
	args   []string
	out    io.ReadCloser
	closed bool
}

func (l *lazyDump) Read(p []byte) (int, error) {
	if l.out == nil {
		out, err := l.client.Dump(l.ctx, l.args)
		if err != nil {
			return 0, err
		}

		l.out = out
	}

	return l.out.Read(p)
}

func (l *lazyDump) close() error {
	if l.out == nil || l.closed {
		return nil
	}

	l.closed = true

	return l.out.Close()
}

// keptLine bounds what dumpScanner keeps of a line; the lines it looks for
// are short, and an extended insert can run to megabytes.
const keptLine = 512

var positionLine = regexp.MustCompile(`^-- CHANGE (?:MASTER|REPLICATION SOURCE) TO (?:MASTER|SOURCE)_LOG_FILE='([^']+)', (?:MASTER|SOURCE)_LOG_POS=(\d+);`)

// dumpScanner reads the binary log position out of a dump's header and
// sees whether it ends with mysqldump's trailer.
type dumpScanner struct {
	line []byte
	last []byte

	file  string
	pos   uint64
	found bool
}

func (s *dumpScanner) Write(p []byte) (int, error) {
	n := len(p)

	for len(p) > 0 {
		i := bytes.IndexByte(p, '\n')

		chunk := p
		if i >= 0 {
			chunk = p[:i]
		}

		if room := keptLine - len(s.line); room > 0 {
			s.line = append(s.line, chunk[:min(room, len(chunk))]...)
		}

		if i < 0 {
			break
		}

		s.endLine()
		p = p[i+1:]
	}

	return n, nil
}

func (s *dumpScanner) endLine() {
	line := s.line
	s.line = s.line[:0]

	if len(bytes.TrimSpace(line)) == 0 {
		return
	}

	s.last = append(s.last[:0], line...)

	if s.found {
		return
	}

	if m := positionLine.FindSubmatch(line); m != nil {
		pos, err := strconv.ParseUint(string(m[2]), 10, 64)
		if err == nil {
			s.file, s.pos, s.found = string(m[1]), pos, true
		}
	}
}

// metadata is the commit metadata of a dump of srv once the stream ended;
// it fails for a dump without mysqldump's trailer, or without a position
// when one was asked for.
func (s *dumpScanner) metadata(srv *Server, position bool) (map[string]string, error) {
	s.endLine()

	if !bytes.HasPrefix(s.last, []byte("-- Dump completed")) {
		return nil, errors.New("the dump ends without mysqldump's trailer: it was cut short")
	}

	if position && !s.found {
		return nil, errors.New("the dump records no binary log position")
	}

	meta := map[string]string{
		MetaFlavor:  "mysql",
		MetaVersion: srv.Version,
		MetaLock:    "snapshot",
	}

	Identity{ServerID: srv.ServerID, ServerUUID: srv.ServerUUID}.metadata(meta)

	if position {
		meta[MetaBinlogFile] = s.file
		meta[MetaBinlogPos] = strconv.FormatUint(s.pos, 10)
	}

	if srv.GTIDMode {
		meta[MetaGTIDMode] = "ON"
	}

	return meta, nil
}

// dumpCommits lists the dumps of the server id with a position, newest
// first.
func dumpCommits(commits []*proto.Commit, id Identity) []*proto.Commit {
	var dumps []*proto.Commit

	for _, commit := range commits {
		meta := commit.GetMetadata()
		if _, _, ok := splitBinlog(meta[MetaBinlogFile]); !ok {
			continue
		}

		if was, ok := identityOf(meta, meta[MetaBinlogFile]); ok && was.Same(id) {
			dumps = append(dumps, commit)
		}
	}

	return dumps
}
