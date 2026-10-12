// Package mysql backs up MySQL servers into goback sets: logical dumps
// through mysqldump into one set, and the binary logs, received as a
// replica, into another for point-in-time recovery.
package mysql

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

// The commit metadata the MySQL sets carry.
const (
	// MetaFlavor is always mysql for now.
	MetaFlavor  = "mysql.flavor"
	MetaVersion = "mysql.version"
	// MetaServerID and MetaServerUUID name the server; MySQL 5.5 has no
	// server_uuid.
	MetaServerID   = "mysql.server_id"
	MetaServerUUID = "mysql.server_uuid"
	// MetaBinlogFile and MetaBinlogPos are the binary log position a dump
	// is consistent with.
	MetaBinlogFile = "mysql.binlog_file"
	MetaBinlogPos  = "mysql.binlog_pos"
	// MetaLock is snapshot or all-tables: how the dump was kept consistent.
	MetaLock = "mysql.lock"
	// MetaGTIDMode is ON for a dump of a server with GTIDs on.
	MetaGTIDMode = "mysql.gtid_mode"
	// MetaChain numbers the binary log chain a dump or binlog commit
	// belongs to, in decimal; the numbering starts over after RESET MASTER.
	MetaChain = "mysql.chain"
	// MetaFirstBinlog and MetaLastBinlog bound the files a binlog commit
	// holds of its chain.
	MetaFirstBinlog  = "mysql.first_binlog"
	MetaLastBinlog   = "mysql.last_binlog"
	MetaBinlogFormat = "mysql.binlog_format"
	// MetaEndBinlog and MetaEndPos are where the binlogs a binlog commit
	// holds of its chain end, its partial binlog included.
	MetaEndBinlog = "mysql.end_binlog"
	MetaEndPos    = "mysql.end_pos"
	// MetaCapture is how a binlog commit's binlogs were collected:
	// CaptureReplication, or files for binlogs copied from the server's
	// binlog directory, which restore reads alike.
	MetaCapture = "mysql.capture"
)

// CaptureReplication is the MetaCapture of binlogs received as a replica.
const CaptureReplication = "replication"

// DumpFile is the name of the one file a dump commit holds.
const DumpFile = "dump.sql"

var (
	// ErrForeignServer is returned for a dump of another server than the
	// dump set's.
	ErrForeignServer = errors.New("dump of another server")
	// ErrForeignBinlog is returned for binlogs of another server than the
	// binlog set's.
	ErrForeignBinlog = errors.New("binlog of another server")
	// ErrGap is returned when binlogs a dump needs are missing.
	ErrGap = errors.New("binlogs are missing")
	// ErrBinlogChanged is returned when the server's copy of a binlog no
	// longer matches what was received of it.
	ErrBinlogChanged = errors.New("a received binlog changed on the server")
	// ErrNoBinlog is returned when the server's binary log is off.
	ErrNoBinlog = errors.New("the server's binary log is off")
	// ErrMariaDB is returned for a MariaDB server, which is not supported yet.
	ErrMariaDB = errors.New("MariaDB is not supported yet")
)

// Querier runs SQL against a server.
type Querier interface {
	// Query runs sql and returns its rows as mysql --batch
	// --skip-column-names prints them, NULL as "NULL".
	Query(ctx context.Context, sql string) ([][]string, error)
}

// Client runs the MySQL client programs against one server. Exec runs the
// installed ones.
type Client interface {
	Querier
	// DumpVersion is the output of mysqldump --version.
	DumpVersion(ctx context.Context) (string, error)
	// Dump starts mysqldump with args. Close on the returned reader waits
	// for mysqldump, stopping it first unless its output was read to the
	// end, and reports how it ended.
	Dump(ctx context.Context, args []string) (io.ReadCloser, error)
	// Load runs the SQL r holds through mysql.
	Load(ctx context.Context, r io.Reader) error
	// BinlogVersion is the output of mysqlbinlog --version; it fails when
	// there is no mysqlbinlog.
	BinlogVersion(ctx context.Context) (string, error)
	// Replay runs mysqlbinlog with args, under TZ=UTC, into mysql.
	Replay(ctx context.Context, args []string) error
}

// Server is what goback reads about a server before it backs it up.
type Server struct {
	Version        string
	Major, Minor   int
	Patch          int
	LogBin         bool
	ServerID       uint32
	ServerUUID     string
	BinlogBasename string
	BinlogFormat   string
	GTIDMode       bool
}

var versionNumbers = regexp.MustCompile(`^(\d+)\.(\d+)\.(\d+)`)

// ReadServer reads the server's version and binary log settings.
func ReadServer(ctx context.Context, c Querier) (*Server, error) {
	row, err := queryRow(ctx, c, "SELECT @@version, @@version_comment, @@log_bin, @@server_id, @@binlog_format", 5)
	if err != nil {
		return nil, err
	}

	if strings.Contains(strings.ToLower(row[0]+" "+row[1]), "mariadb") {
		return nil, fmt.Errorf("%w: the server is %s", ErrMariaDB, row[0])
	}

	s := &Server{Version: row[0], LogBin: row[2] == "1" || strings.EqualFold(row[2], "ON"), BinlogFormat: row[4]}

	m := versionNumbers.FindStringSubmatch(s.Version)
	if m == nil {
		return nil, fmt.Errorf("the server's version %q", s.Version)
	}

	s.Major, _ = strconv.Atoi(m[1])
	s.Minor, _ = strconv.Atoi(m[2])
	s.Patch, _ = strconv.Atoi(m[3])

	id, err := strconv.ParseUint(row[3], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("the server's server_id %q: %w", row[3], err)
	}

	s.ServerID = uint32(id)

	if s.atLeast(5, 6, 0) {
		row, err := queryRow(ctx, c, "SELECT @@server_uuid, @@log_bin_basename, @@gtid_mode", 3)
		if err != nil {
			return nil, err
		}

		s.ServerUUID = row[0]
		if row[1] != "NULL" {
			s.BinlogBasename = row[1]
		}

		s.GTIDMode = strings.EqualFold(row[2], "ON")
	}

	return s, nil
}

func (s *Server) atLeast(major, minor, patch int) bool {
	return versionAtLeast([3]int{s.Major, s.Minor, s.Patch}, [3]int{major, minor, patch})
}

func versionAtLeast(have, want [3]int) bool {
	for i := range have {
		if have[i] != want[i] {
			return have[i] > want[i]
		}
	}

	return true
}

// Binlog is one binary log the server lists.
type Binlog struct {
	Name string
	Size int64
}

// BinaryLogs lists the server's binary logs, oldest first; the last one is
// the one the server writes to.
func BinaryLogs(ctx context.Context, c Querier) ([]Binlog, error) {
	rows, err := c.Query(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return nil, err
	}

	logs := make([]Binlog, 0, len(rows))
	for _, row := range rows {
		if len(row) < 2 {
			return nil, fmt.Errorf("SHOW BINARY LOGS returned %q", row)
		}

		size, err := strconv.ParseInt(row[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("SHOW BINARY LOGS: the size of %s, %q: %w", row[0], row[1], err)
		}

		logs = append(logs, Binlog{Name: row[0], Size: size})
	}

	return logs, nil
}

func queryRow(ctx context.Context, c Querier, sql string, columns int) ([]string, error) {
	rows, err := c.Query(ctx, sql)
	if err != nil {
		return nil, err
	}

	if len(rows) != 1 || len(rows[0]) != columns {
		return nil, fmt.Errorf("%s returned %q", sql, rows)
	}

	return rows[0], nil
}

// Identity names the server a commit belongs to: by server_uuid where both
// sides have one, else by server_id and the binary log's base name.
type Identity struct {
	ServerID   uint32
	ServerUUID string
	Base       string
}

// Same reports whether i and o name the same server.
func (i Identity) Same(o Identity) bool {
	if i.ServerUUID != "" && o.ServerUUID != "" {
		return i.ServerUUID == o.ServerUUID
	}

	if i.ServerID != o.ServerID {
		return false
	}

	return i.Base == "" || o.Base == "" || i.Base == o.Base
}

func (i Identity) String() string {
	if i.ServerUUID != "" {
		return i.ServerUUID
	}

	if i.Base != "" {
		return fmt.Sprintf("server_id %d logging to %s", i.ServerID, i.Base)
	}

	return fmt.Sprintf("server_id %d", i.ServerID)
}

func (i Identity) metadata(m map[string]string) {
	m[MetaServerID] = strconv.FormatUint(uint64(i.ServerID), 10)
	if i.ServerUUID != "" {
		m[MetaServerUUID] = i.ServerUUID
	}
}

// identityOf reads the server a commit's metadata names; ok is false when
// it names none.
func identityOf(meta map[string]string, file string) (Identity, bool) {
	id, err := strconv.ParseUint(meta[MetaServerID], 10, 32)
	if err != nil {
		return Identity{}, false
	}

	base, _, _ := splitBinlog(file)

	return Identity{ServerID: uint32(id), ServerUUID: meta[MetaServerUUID], Base: base}, true
}

// chainOf reads the chain a commit records, 1 when it records none.
func chainOf(meta map[string]string) int {
	n, err := strconv.Atoi(meta[MetaChain])
	if err != nil || n < 1 {
		return 1
	}

	return n
}

// chainDir is the directory of a binlog set that holds chain n.
func chainDir(n int) string {
	return "chain-" + strconv.Itoa(n)
}

func parseChainDir(name string) (int, bool) {
	rest, ok := strings.CutPrefix(name, "chain-")
	if !ok {
		return 0, false
	}

	n, err := strconv.Atoi(rest)

	return n, err == nil && n > 0
}

// partialSuffix marks a binlog the set holds only up to where a receiver
// had received it.
const partialSuffix = ".partial"

// isPartial reports whether name is a partial binlog's, and returns the
// binlog's name.
func isPartial(name string) (string, bool) {
	binlog, ok := strings.CutSuffix(name, partialSuffix)
	return binlog, ok
}

// splitBinlog splits a binlog file name, whole or partial, into its base
// and number; ok is false for names that are not a binlog's.
func splitBinlog(name string) (string, uint64, bool) {
	name, _ = isPartial(name)

	dot := strings.LastIndexByte(name, '.')
	if dot <= 0 || len(name)-dot-1 < 6 {
		return "", 0, false
	}

	n, err := strconv.ParseUint(name[dot+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}

	return name[:dot], n, true
}

// compareBinlogs orders binlogs of one base by their number, which grows
// past six digits; names of different bases or no binlog's order as
// strings.
func compareBinlogs(a, b string) int {
	baseA, na, okA := splitBinlog(a)
	baseB, nb, okB := splitBinlog(b)

	if !okA || !okB || baseA != baseB {
		return strings.Compare(a, b)
	}

	switch {
	case na < nb:
		return -1
	case na > nb:
		return 1
	}

	return 0
}

// lastWhole is the last of names that is a whole binlog's.
func lastWhole(names []string) string {
	for i := len(names) - 1; i >= 0; i-- {
		if _, partial := isPartial(names[i]); !partial {
			return names[i]
		}
	}

	return ""
}

// follows reports whether b is the binlog right after a.
func follows(a, b string) bool {
	baseA, na, okA := splitBinlog(a)
	baseB, nb, okB := splitBinlog(b)

	return okA && okB && baseA == baseB && nb == na+1
}

// binlogName names binlog n of base, as wide as like's number.
func binlogName(base string, n uint64, like string) string {
	like, _ = isPartial(like)
	width := len(like) - strings.LastIndexByte(like, '.') - 1
	return fmt.Sprintf("%s.%0*d", base, width, n)
}
