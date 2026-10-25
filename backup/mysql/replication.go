package mysql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// ErrServerIDInUse is returned when the receiver's server id is the
// server's own, or another replica connects with it, so the server ends
// one dump after the other.
var ErrServerIDInUse = errors.New("the receiver's server id is taken")

// ReplicaServerID is the server id a binlog set's receiver connects with
// unless told otherwise: a hash of the set's name in 2^31 to 2^32-1, above
// the small ids servers are usually given.
func ReplicaServerID(set string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(set))

	return 1<<31 | h.Sum32()
}

// network and address are where o says the server listens: the socket for
// the socket protocol, or for localhost when o names a socket; else TCP to
// the host, 127.0.0.1 for localhost, on the port, 3306 by default.
func (o *Options) network() (string, string) {
	local := o.Host == "" || o.Host == "localhost"

	if o.Protocol == "socket" || o.Protocol != "tcp" && local && o.Socket != "" {
		return "unix", o.Socket
	}

	host := o.Host
	if local {
		host = "127.0.0.1"
	}

	port := o.Port
	if port == 0 {
		port = 3306
	}

	return "tcp", net.JoinHostPort(host, strconv.Itoa(port))
}

// tlsConfig is the TLS o's ssl-mode asks for: none for DISABLED, PREFERRED
// or no mode, encryption alone for REQUIRED, and a verified certificate,
// for VERIFY_IDENTITY also of the host's name, against ssl-ca or the
// system's roots.
func (o *Options) tlsConfig() (*tls.Config, error) {
	switch o.SSLMode {
	case "", "DISABLED", "PREFERRED":
		return nil, nil
	case "REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY":
	default:
		return nil, fmt.Errorf("ssl-mode %q", o.SSLMode)
	}

	cfg := &tls.Config{ServerName: o.Host, MinVersion: tls.VersionTLS12}

	if o.SSLCert != "" || o.SSLKey != "" {
		cert, err := tls.LoadX509KeyPair(o.SSLCert, o.SSLKey)
		if err != nil {
			return nil, fmt.Errorf("ssl-cert and ssl-key: %w", err)
		}

		cfg.Certificates = []tls.Certificate{cert}
	}

	if o.SSLCA != "" {
		pem, err := os.ReadFile(o.SSLCA)
		if err != nil {
			return nil, fmt.Errorf("ssl-ca: %w", err)
		}

		cfg.RootCAs = x509.NewCertPool()
		if !cfg.RootCAs.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ssl-ca %s holds no certificate", o.SSLCA)
		}
	}

	switch o.SSLMode {
	case "REQUIRED":
		cfg.InsecureSkipVerify = true
	case "VERIFY_CA":
		cfg.InsecureSkipVerify = true
		cfg.VerifyConnection = func(cs tls.ConnectionState) error {
			opts := x509.VerifyOptions{Roots: cfg.RootCAs, Intermediates: x509.NewCertPool()}
			for _, cert := range cs.PeerCertificates[1:] {
				opts.Intermediates.AddCert(cert)
			}

			_, err := cs.PeerCertificates[0].Verify(opts)

			return err
		}
	}

	return cfg, nil
}

// queryTimeout bounds each read and write of a serverConn.
const queryTimeout = 30 * time.Second

// serverConn is a normal connection to the server, opened again after it
// failed.
type serverConn struct {
	opts *Options

	mu   sync.Mutex
	conn *client.Conn
	// raw is conn's network connection, which unlike conn may be closed
	// while conn is in use.
	raw net.Conn
}

func newServerConn(opts *Options) *serverConn {
	return &serverConn{opts: opts}
}

func (c *serverConn) connect(ctx context.Context) (*client.Conn, error) {
	if c.conn != nil {
		return c.conn, nil
	}

	cfg, err := c.opts.tlsConfig()
	if err != nil {
		return nil, err
	}

	network, addr := c.opts.network()
	dialer := &net.Dialer{Timeout: 10 * time.Second}

	var raw net.Conn

	dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, network, addr)
		raw = conn

		return conn, err
	}

	conn, err := client.ConnectWithDialer(ctx, network, addr, c.opts.User, c.opts.Password, "", dial, func(conn *client.Conn) error {
		conn.SetTLSConfig(cfg)
		conn.ReadTimeout, conn.WriteTimeout = queryTimeout, queryTimeout

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("connecting to %s: %w", addr, plainError(err))
	}

	c.conn, c.raw = conn, raw

	return conn, nil
}

// Query implements Querier.
func (c *serverConn) Query(ctx context.Context, sql string) ([][]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	conn, err := c.connect(ctx)
	if err != nil {
		return nil, err
	}

	raw := c.raw
	stop := context.AfterFunc(ctx, func() { _ = raw.Close() })
	result, err := conn.Execute(sql)

	if !stop() {
		if err == nil {
			result.Close()
		}

		err = ctx.Err()
	}

	if err != nil {
		var server *gomysql.MyError
		if !errors.As(err, &server) {
			_ = conn.Close()
			c.conn = nil
		}

		return nil, fmt.Errorf("%s: %w", sql, err)
	}

	defer result.Close()

	if result.Resultset == nil {
		return nil, nil
	}

	rows := make([][]string, 0, len(result.Values))
	for _, values := range result.Values {
		row := make([]string, len(values))
		for i := range values {
			if values[i].Type == gomysql.FieldValueTypeString {
				row[i] = string(values[i].AsString())
			} else {
				row[i] = values[i].String()
			}
		}

		rows = append(rows, row)
	}

	return rows, nil
}

func (c *serverConn) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

// streamEvent is one event of the dump stream, as the server sent it.
type streamEvent struct {
	header eventHeader
	data   []byte
	// next and pos are a Rotate event's binlog and position
	next string
	pos  uint64
}

// binlogStream is the dump of a server's binlogs from a position.
type binlogStream interface {
	// next returns the next event, or nil when none arrived by deadline.
	next(ctx context.Context, deadline time.Time) (*streamEvent, error)
	close()
}

// The dump connection's heartbeat; three missed ones end it.
const (
	heartbeatPeriod = 10 * time.Second
	readTimeout     = 3 * heartbeatPeriod
)

// dumpStream receives a server's binlog events through go-mysql in raw
// mode, without reconnecting on its own.
type dumpStream struct {
	syncer   *replication.BinlogSyncer
	streamer *replication.BinlogStreamer
}

// startDump registers as replica serverID and asks for the binlogs from
// name at pos.
func startDump(opts *Options, serverID uint32, name string, pos int64) (*dumpStream, error) {
	cfg, err := opts.tlsConfig()
	if err != nil {
		return nil, err
	}

	network, addr := opts.network()

	host, port := addr, 0
	if network == "tcp" {
		h, p, _ := net.SplitHostPort(addr)
		host = h
		port, _ = strconv.Atoi(p)
	}

	hostname, _ := os.Hostname()

	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID:         serverID,
		Host:             host,
		Port:             uint16(port),
		User:             opts.User,
		Password:         opts.Password,
		Localhost:        hostname,
		RawModeEnabled:   true,
		TLSConfig:        cfg,
		HeartbeatPeriod:  heartbeatPeriod,
		ReadTimeout:      readTimeout,
		DisableRetrySync: true,
		Logger:           slog.New(slog.DiscardHandler),
	})

	streamer, err := syncer.StartSync(gomysql.Position{Name: name, Pos: uint32(pos)})
	if err != nil {
		syncer.Close()
		return nil, fmt.Errorf("starting the binlog dump from %s:%d: %w", name, pos, plainError(err))
	}

	return &dumpStream{syncer: syncer, streamer: streamer}, nil
}

func (d *dumpStream) next(ctx context.Context, deadline time.Time) (*streamEvent, error) {
	waitCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	e, err := d.streamer.GetEvent(waitCtx)
	if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
		return nil, nil
	}

	if err != nil {
		return nil, plainError(err)
	}

	h, err := parseEventHeader(e.RawData)
	if err != nil {
		return nil, err
	}

	ev := &streamEvent{header: h, data: e.RawData}
	if rotate, ok := e.Event.(*replication.RotateEvent); ok {
		ev.next, ev.pos = string(rotate.NextLogName), rotate.Position
	}

	return ev, nil
}

func (d *dumpStream) close() {
	d.syncer.Close()
}

// serverErrorCode is the MySQL error number err carries, 0 for none.
func serverErrorCode(err error) uint16 {
	var server *gomysql.MyError
	if errors.As(err, &server) {
		return server.Code
	}

	if code := gomysql.ErrorCode(err.Error()); code > 0 && strings.Contains(err.Error(), "ERROR ") {
		return uint16(code)
	}

	return 0
}

// errNoBinlogFile is MySQL's error 1236: the server has no binlog at the
// position the dump asked for.
const errNoBinlogFile = 1236

// replicaHost is one row of SHOW REPLICAS.
type replicaHost struct {
	serverID uint32
	host     string
}

// replicas lists the replicas registered with the server.
func replicas(ctx context.Context, q Querier, srv *Server) ([]replicaHost, error) {
	sql := "SHOW SLAVE HOSTS"
	if srv.atLeast(8, 0, 22) {
		sql = "SHOW REPLICAS"
	}

	rows, err := q.Query(ctx, sql)
	if err != nil {
		return nil, err
	}

	hosts := make([]replicaHost, 0, len(rows))
	for _, row := range rows {
		if len(row) < 2 {
			continue
		}

		id, err := strconv.ParseUint(row[0], 10, 32)
		if err != nil {
			continue
		}

		hosts = append(hosts, replicaHost{serverID: uint32(id), host: row[1]})
	}

	return hosts, nil
}

// checkServerID refuses a server id that is the server's own, or that a
// replica on another host than hostname registered with.
func checkServerID(id uint32, srv *Server, registered []replicaHost, hostname string) error {
	if id == srv.ServerID {
		return fmt.Errorf("%w: %d is the server's own server_id; pass another", ErrServerIDInUse, id)
	}

	for _, r := range registered {
		if r.serverID == id && r.host != hostname {
			return fmt.Errorf("%w: the replica on %s registered with server id %d; pass another", ErrServerIDInUse, r.host, id)
		}
	}

	return nil
}

// The errors that say the user lacks a privilege.
const (
	errDBAccessDenied = 1044
	errAccessDenied   = 1045
	errNeedPrivilege  = 1227
)

// plainError drops the stack traces go-mysql's errors carry from their
// text, keeping MySQL's own errors as they are.
func plainError(err error) error {
	if err == nil || serverErrorCode(err) != 0 {
		return err
	}

	lines := strings.Split(err.Error(), "\n")

	var kept []string
	for i, line := range lines {
		if strings.HasPrefix(line, "\t") || i+1 < len(lines) && strings.HasPrefix(lines[i+1], "\t") {
			continue
		}

		kept = append(kept, line)
	}

	return errors.New(strings.Join(kept, ": "))
}
