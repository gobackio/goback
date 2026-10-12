package postgres

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// ErrSlotActive is returned when another connection streams through the
// receiver's slot.
var ErrSlotActive = errors.New("the replication slot is in use by another connection")

// SlotName is the replication slot a WAL set's receiver uses unless told
// otherwise: goback_ and the set's name, lowercased, with every other
// character as _, cut to 63 bytes.
func SlotName(set string) string {
	var b strings.Builder

	b.WriteString("goback_")

	for _, c := range strings.ToLower(set) {
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' {
			b.WriteRune(c)
		} else {
			b.WriteByte('_')
		}
	}

	return b.String()[:min(b.Len(), 63)]
}

func checkSlotName(slot string) error {
	if slot == "" || len(slot) > 63 || strings.TrimFunc(slot, func(c rune) bool { return c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' }) != "" {
		return fmt.Errorf("%q is not a replication slot name: lowercase letters, digits and _, at most 63", slot)
	}

	return nil
}

// connConfig is connString's connection config: a physical replication
// connection, or a normal one to the database it names, postgres by
// default. Both carry appName unless connString names one.
func connConfig(connString string, replication bool, appName string) (*pgconn.Config, error) {
	cfg, err := pgconn.ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	if replication {
		cfg.RuntimeParams["replication"] = "true"
	} else {
		delete(cfg.RuntimeParams, "replication")

		if cfg.Database == "" {
			cfg.Database = "postgres"
		}
	}

	if _, ok := cfg.RuntimeParams["application_name"]; !ok && appName != "" {
		cfg.RuntimeParams["application_name"] = appName
	}

	return cfg, nil
}

// connectReplication opens a physical replication connection.
func connectReplication(ctx context.Context, connString, appName string) (*pgconn.PgConn, error) {
	cfg, err := connConfig(connString, true, appName)
	if err != nil {
		return nil, err
	}

	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("opening a replication connection: %w", err)
	}

	return conn, nil
}

// identity is what IDENTIFY_SYSTEM says about the server.
type identity struct {
	systemID uint64
	timeline uint32
	position uint64
}

func identify(ctx context.Context, conn *pgconn.PgConn) (identity, error) {
	sys, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return identity{}, fmt.Errorf("IDENTIFY_SYSTEM: %w", err)
	}

	id, err := strconv.ParseUint(sys.SystemID, 10, 64)
	if err != nil {
		return identity{}, fmt.Errorf("IDENTIFY_SYSTEM: system id %q: %w", sys.SystemID, err)
	}

	return identity{systemID: id, timeline: uint32(sys.Timeline), position: uint64(sys.XLogPos)}, nil
}

// createSlot creates a permanent physical slot that holds WAL from now on.
func createSlot(ctx context.Context, conn *pgconn.PgConn, slot string) error {
	if _, err := conn.Exec(ctx, "CREATE_REPLICATION_SLOT "+slot+" PHYSICAL RESERVE_WAL").ReadAll(); err != nil {
		return fmt.Errorf("creating the replication slot %s: %w", slot, err)
	}

	return nil
}

func dropSlot(ctx context.Context, conn *pgconn.PgConn, slot string) error {
	if _, err := conn.Exec(ctx, "DROP_REPLICATION_SLOT "+slot).ReadAll(); err != nil {
		return fmt.Errorf("dropping the replication slot %s: %w", slot, err)
	}

	return nil
}

// DropSlot drops the receiver's replication slot, so the server no longer
// keeps WAL for it.
func DropSlot(ctx context.Context, connString, slot string) error {
	if err := checkSlotName(slot); err != nil {
		return err
	}

	conn, err := connectReplication(ctx, connString, "goback")
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	return dropSlot(ctx, conn, slot)
}

// The messages a walStream hands the receiver.
type (
	// xlogData is WAL that starts at start.
	xlogData struct {
		start uint64
		data  []byte
		// serverEnd is where the server's WAL ends.
		serverEnd uint64
	}
	keepalive struct {
		serverEnd uint64
		reply     bool
	}
	// timelineEnd is the end of the streamed timeline: the server goes on
	// with next from start.
	timelineEnd struct {
		next  uint32
		start uint64
	}
)

// walStream is a replication connection streaming WAL.
type walStream interface {
	// next returns the next message, or nil when deadline passes first.
	next(ctx context.Context, deadline time.Time) (any, error)
	// status sends a standby status update.
	status(write, flush uint64, reply bool) error
	// follow streams timeline from from, a segment's start, after a
	// timelineEnd.
	follow(ctx context.Context, timeline uint32, from uint64) error
}

// pgStream streams WAL through a slot of a live server.
type pgStream struct {
	conn  *pgconn.PgConn
	slot  string
	spool Spool
	// lacks reports whether neither the spool nor the set holds a file.
	lacks func(name string) bool
}

// start streams timeline from from. Starting where a timeline that is
// not the server's ended answers with that end instead, as a timelineEnd.
func (s *pgStream) start(ctx context.Context, timeline uint32, from uint64) (*timelineEnd, error) {
	err := pglogrepl.StartReplication(ctx, s.conn, s.slot, pglogrepl.LSN(from), pglogrepl.StartReplicationOptions{
		Timeline: int32(timeline),
		Mode:     pglogrepl.PhysicalReplication,
	})

	if next, start, ok := pglogrepl.IsErrEndTimeline(err); ok {
		return &timelineEnd{next: uint32(next), start: uint64(start)}, nil
	}

	if err != nil {
		return nil, fmt.Errorf("START_REPLICATION at %s on timeline %d: %w", FormatLSN(from), timeline, err)
	}

	return nil, nil
}

func (s *pgStream) next(ctx context.Context, deadline time.Time) (any, error) {
	rctx, cancel := context.WithDeadline(ctx, deadline)
	msg, err := s.conn.ReceiveMessage(rctx)
	cancel()

	if err != nil {
		if pgconn.Timeout(err) && ctx.Err() == nil {
			return nil, nil
		}

		return nil, err
	}

	switch msg := msg.(type) {
	case *pgproto3.CopyData:
		if len(msg.Data) == 0 {
			return nil, errors.New("an empty CopyData message")
		}

		switch msg.Data[0] {
		case pglogrepl.XLogDataByteID:
			x, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return nil, err
			}

			return xlogData{start: uint64(x.WALStart), data: x.WALData, serverEnd: uint64(x.ServerWALEnd)}, nil
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			k, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return nil, err
			}

			return keepalive{serverEnd: uint64(k.ServerWALEnd), reply: k.ReplyRequested}, nil
		}

		return nil, nil
	case *pgproto3.CopyDone:
		end, err := pglogrepl.SendStandbyCopyDone(ctx, s.conn)
		if err != nil {
			return nil, fmt.Errorf("ending the timeline: %w", err)
		}

		if end == nil || end.Timeline == 0 {
			return nil, errors.New("the server ended streaming without naming a next timeline")
		}

		return timelineEnd{next: uint32(end.Timeline), start: uint64(end.LSN)}, nil
	case *pgproto3.ErrorResponse:
		return nil, pgconn.ErrorResponseToPgError(msg)
	}

	return nil, nil
}

func (s *pgStream) follow(ctx context.Context, timeline uint32, from uint64) error {
	if err := s.fetchHistory(ctx, timeline); err != nil {
		return err
	}

	end, err := s.start(ctx, timeline, from)
	if err != nil {
		return err
	}

	if end != nil {
		return fmt.Errorf("timeline %d ends where streaming it starts, at %s", timeline, FormatLSN(from))
	}

	return nil
}

// fetchHistory puts the history file of timeline into the spool unless
// the spool or the set holds it. Timeline 1 has none.
func (s *pgStream) fetchHistory(ctx context.Context, timeline uint32) error {
	if timeline <= 1 || !s.lacks(historyName(timeline)) {
		return nil
	}

	content, err := s.readHistory(ctx, timeline)
	if err != nil {
		return err
	}

	return s.spool.put(historyName(timeline), content)
}

// serverHistory is the history of timeline, the server's.
func (s *pgStream) serverHistory(ctx context.Context, timeline uint32) (history, error) {
	if timeline <= 1 {
		return timelineHistory(timeline), nil
	}

	content, err := s.readHistory(ctx, timeline)
	if err != nil {
		return nil, err
	}

	return parseHistory(timeline, content)
}

func (s *pgStream) readHistory(ctx context.Context, timeline uint32) ([]byte, error) {
	h, err := pglogrepl.TimelineHistory(ctx, s.conn, int32(timeline))
	if err != nil {
		return nil, fmt.Errorf("TIMELINE_HISTORY %d: %w", timeline, err)
	}

	if h.FileName != historyName(timeline) {
		return nil, fmt.Errorf("TIMELINE_HISTORY %d named %q", timeline, h.FileName)
	}

	return h.Content, nil
}

func historyName(timeline uint32) string {
	return fmt.Sprintf("%08X.history", timeline)
}

// status sends a standby status update with exactly these positions,
// apply equal to flush: pglogrepl's own would turn a zero flush into the
// write position, and zero is how the receiver says nothing is flushed.
func (s *pgStream) status(write, flush uint64, reply bool) error {
	data := encodeStatus(write, flush, time.Now(), reply)

	buf, err := (&pgproto3.CopyData{Data: data}).Encode(nil)
	if err != nil {
		return err
	}

	return s.conn.Frontend().SendUnbufferedEncodedCopyData(buf)
}

// encodeStatus is a standby status update message.
func encodeStatus(write, flush uint64, now time.Time, reply bool) []byte {
	data := make([]byte, 34)
	data[0] = pglogrepl.StandbyStatusUpdateByteID
	binary.BigEndian.PutUint64(data[1:], write)
	binary.BigEndian.PutUint64(data[9:], flush)
	binary.BigEndian.PutUint64(data[17:], flush)
	binary.BigEndian.PutUint64(data[25:], uint64(now.UnixMicro()-postgresEpochMicros))

	if reply {
		data[33] = 1
	}

	return data
}

// postgresEpochMicros is 2000-01-01 in microseconds since the Unix epoch.
const postgresEpochMicros = 946684800 * 1000000

// isRemovedWAL reports whether err is the server saying it no longer has
// the WAL asked for.
func isRemovedWAL(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && strings.Contains(pgErr.Message, "has already been removed")
}

// put writes a whole file into the spool under name.
func (s Spool) put(name string, content []byte) error {
	if name == "" || name != filepath.Base(name) || strings.HasPrefix(name, ".") {
		return fmt.Errorf("%q is not a WAL file name", name)
	}

	if err := os.MkdirAll(s.receiving(), 0o700); err != nil {
		return err
	}

	tmp := filepath.Join(s.receiving(), name)
	defer os.Remove(tmp)

	if err := writeSynced(tmp, content); err != nil {
		return err
	}

	return s.adopt(tmp, name)
}

func writeSynced(path string, content []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}

	if _, err := f.Write(content); err != nil {
		_ = f.Close()
		return err
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}

	return f.Close()
}
