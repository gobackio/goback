package mysql

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/index/sql"
	"github.com/gobackio/goback/storage"
	"github.com/gobackio/goback/storage/pack"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"
)

// fakeServer is a MySQL server as its client programs show it, with its
// binlogs in a directory.
type fakeServer struct {
	t        *testing.T
	dir      string
	version  string
	serverID uint32
	uuid     string
	logBin   bool
	base     string
	// files are the binlogs the server lists, oldest first
	files []string
	next  int
	// written is how many events the active binlog holds past its opening
	written int

	nonTransactional []string
	dump             string
	dumpErr          error
	dumpArgs         []string
	loaded           []string
	replayed         [][]string
	databases        []string
	noBinlog         bool
	flushes          int
	// replicas are the rows SHOW REPLICAS returns
	replicas [][]string
}

func newFakeServer(t *testing.T) *fakeServer {
	s := &fakeServer{t: t, dir: t.TempDir(), version: "8.0.46", serverID: 1, uuid: "3e11fa47-71ca-11e1-9e33-c80aa9429562", logBin: true, base: "binlog", next: 1}
	s.open()

	return s
}

const headerSize = 4 + eventHeaderLen + 100

// open starts the next binlog.
func (s *fakeServer) open() {
	name := fmt.Sprintf("%s.%06d", s.base, s.next)
	s.next++
	s.files = append(s.files, name)
	s.written = 0
	s.writeFile(name, true, 0)
}

func (s *fakeServer) writeFile(name string, inUse bool, events int) {
	content := binlogContent(s.serverID, inUse, events)
	require.NoError(s.t, os.WriteFile(filepath.Join(s.dir, name), content, 0o640))
}

// binlogContent is a binlog of serverID: magic, a format description event
// and events of 10 bytes each.
func binlogContent(serverID uint32, inUse bool, events int) []byte {
	buf := append([]byte{}, binlogMagic...)

	event := make([]byte, eventHeaderLen)
	event[4] = formatDescriptionEvent
	binary.LittleEndian.PutUint32(event[5:], serverID)
	binary.LittleEndian.PutUint32(event[9:], headerSize-4)

	if inUse {
		binary.LittleEndian.PutUint16(event[17:], binlogInUse)
	}

	buf = append(buf, event...)
	buf = append(buf, make([]byte, headerSize-4-eventHeaderLen)...)

	return append(buf, bytes.Repeat([]byte{0xAA}, events*10)...)
}

// write logs a transaction to the active binlog.
func (s *fakeServer) write() {
	s.written++
	s.writeFile(s.files[len(s.files)-1], true, s.written)
}

// flush closes the active binlog and opens the next.
func (s *fakeServer) flush() {
	s.flushes++
	s.writeFile(s.files[len(s.files)-1], false, s.written)
	s.open()
}

// resetMaster deletes every binlog and starts over at 1.
func (s *fakeServer) resetMaster() {
	for _, name := range s.files {
		require.NoError(s.t, os.Remove(filepath.Join(s.dir, name)))
	}

	s.files, s.next = nil, 1
	s.open()
}

// purge deletes the binlogs before to.
func (s *fakeServer) purge(to string) {
	for len(s.files) > 0 && s.files[0] != to {
		require.NoError(s.t, os.Remove(filepath.Join(s.dir, s.files[0])))
		s.files = s.files[1:]
	}
}

func (s *fakeServer) active() string { return s.files[len(s.files)-1] }

func (s *fakeServer) Query(_ context.Context, sql string) ([][]string, error) {
	switch {
	case strings.HasPrefix(sql, "SELECT @@version,"):
		logBin := "0"
		if s.logBin {
			logBin = "1"
		}

		return [][]string{{s.version, "MySQL Community Server - GPL", logBin, fmt.Sprint(s.serverID), s.dir + "/", "ROW"}}, nil
	case strings.HasPrefix(sql, "SELECT @@server_uuid"):
		return [][]string{{s.uuid, filepath.Join(s.dir, s.base), "OFF"}}, nil
	case strings.HasPrefix(sql, "SELECT CONCAT(table_schema"):
		var rows [][]string
		for _, table := range s.nonTransactional {
			rows = append(rows, []string{table})
		}

		return rows, nil
	case sql == "SHOW MASTER STATUS", sql == "SHOW BINARY LOG STATUS":
		return [][]string{{s.active(), fmt.Sprint(headerSize + s.written*10), "", ""}}, nil
	case sql == "FLUSH BINARY LOGS":
		s.flush()
		return nil, nil
	case sql == "SHOW REPLICAS", sql == "SHOW SLAVE HOSTS":
		return s.replicas, nil
	case sql == "SHOW BINARY LOGS":
		var rows [][]string
		for _, name := range s.files {
			info, err := os.Stat(filepath.Join(s.dir, name))
			require.NoError(s.t, err)
			rows = append(rows, []string{name, fmt.Sprint(info.Size()), "No"})
		}

		return rows, nil
	case sql == "SHOW DATABASES":
		var rows [][]string
		for _, db := range append([]string{"information_schema", "mysql", "performance_schema", "sys"}, s.databases...) {
			rows = append(rows, []string{db})
		}

		return rows, nil
	}

	return nil, fmt.Errorf("the fake server does not know %q", sql)
}

func (s *fakeServer) DumpVersion(context.Context) (string, error) {
	return "mysqldump  Ver " + s.version + " for Linux on x86_64 (MySQL Community Server - GPL)", nil
}

func (s *fakeServer) Dump(_ context.Context, args []string) (io.ReadCloser, error) {
	s.dumpArgs = args
	return &fakeDump{Reader: strings.NewReader(s.dump), err: s.dumpErr}, nil
}

type fakeDump struct {
	io.Reader
	err error
}

func (d *fakeDump) Close() error { return d.err }

func (s *fakeServer) Load(_ context.Context, r io.Reader) error {
	content, err := io.ReadAll(r)
	s.loaded = append(s.loaded, string(content))

	return err
}

func (s *fakeServer) BinlogVersion(context.Context) (string, error) {
	if s.noBinlog {
		return "", errors.New("mysqlbinlog: executable file not found")
	}

	return "mysqlbinlog  Ver 8.0.46", nil
}

func (s *fakeServer) Replay(_ context.Context, args []string) error {
	s.replayed = append(s.replayed, args)
	return nil
}

// dumpOutput is mysqldump's output for a dump at the active binlog's
// current position.
func (s *fakeServer) dumpOutput() string {
	return fmt.Sprintf("-- MySQL dump 10.13  Distrib %s\n--\n-- CHANGE REPLICATION SOURCE TO SOURCE_LOG_FILE='%s', SOURCE_LOG_POS=%d;\n\nINSERT INTO t VALUES (1);\n-- Dump completed on 2026-10-11  9:37:42\n",
		s.version, s.active(), headerSize+s.written*10)
}

type fixture struct {
	t     *testing.T
	ctx   context.Context
	index *sql.Index
	srv   *fakeServer
	// offset moves the binlog job's clock ahead of the wall clock
	offset time.Duration
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	bucket := memblob.OpenBucket(nil)
	store, err := pack.NewPackStorage(pack.WithArchiveStorage(storage.NewBucketStore(bucket)), pack.WithArchiveIndex(pack.NewInMemoryIndex()))
	require.NoError(t, err)
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })

	index := sql.NewMemory(t.Name(), store)
	require.NoError(t, index.Open())
	t.Cleanup(func() { _ = index.Close() })
	require.NoError(t, index.SetDefaultPolicy(context.Background(), &retention.KeepAll))

	return &fixture{t: t, ctx: context.Background(), index: index, srv: newFakeServer(t)}
}

func (f *fixture) walker(set string) *backup.Walker {
	return &backup.Walker{Index: f.index, Objects: f.index, Set: set, AgentID: "a", Workers: 1}
}

// takeDump dumps the fake server at its current position.
func (f *fixture) takeDump() (*backup.WalkResult, error) {
	f.srv.dump = f.srv.dumpOutput()
	d := &Dump{Walker: f.walker("db-dump"), Client: f.srv, BinlogSet: "db-binlog"}

	return d.Run(f.ctx)
}

func (f *fixture) mustDump() *backup.WalkResult {
	f.t.Helper()

	result, err := f.takeDump()
	require.NoError(f.t, err)

	return result
}

func (f *fixture) binlog() (*backup.WalkResult, error) {
	b := &BinlogBackup{
		Walker:  f.walker("db-binlog"),
		Client:  f.srv,
		DumpSet: "db-dump",
		Window:  14 * 24 * time.Hour,
		Now:     func() time.Time { return time.Now().Add(f.offset) },
	}

	return b.Run(f.ctx)
}

func (f *fixture) mustBinlog() *backup.WalkResult {
	f.t.Helper()

	result, err := f.binlog()
	require.NoError(f.t, err)
	require.NotNil(f.t, result)

	return result
}

// chains lists the files of each chain the binlog set's latest commit
// holds.
func (f *fixture) chains() map[int][]string {
	f.t.Helper()

	ref, err := f.index.LatestCommit(f.ctx, "db-binlog")
	require.NoError(f.t, err)

	held, err := readBinlogCommit(f.ctx, f.index, nil, ref)
	require.NoError(f.t, err)

	chains := map[int][]string{}
	for n, files := range held.chains {
		for _, file := range files {
			chains[n] = append(chains[n], string(file.Stat.Name))
		}
	}

	return chains
}
