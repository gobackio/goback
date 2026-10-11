package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

// ErrUncappedSlot is returned when max_slot_wal_keep_size leaves the
// receiver's slot free to keep every WAL file until the disk is full.
var ErrUncappedSlot = errors.New("max_slot_wal_keep_size is -1, so the slot may keep WAL until the database's disk is full; set it, or allow an uncapped slot")

// ErrSynchronousStandby is returned when the server waits for the
// receiver to commit before its own commits return.
var ErrSynchronousStandby = errors.New("the server counts the receiver as a synchronous standby; remove it from synchronous_standby_names")

// minReceiveVersion is the oldest Postgres the receiver streams from.
const minReceiveVersion = 140000

// minInProcessBaseVersion is the oldest Postgres whose base backups are
// taken without pg_basebackup.
const minInProcessBaseVersion = 150000

// monitor is a normal connection that watches the receiver's slot.
type monitor struct {
	conn *pgx.Conn
}

func connectMonitor(ctx context.Context, connString, appName string) (*monitor, error) {
	cfg, err := pgx.ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	pc, err := connConfig(connString, false, appName)
	if err != nil {
		return nil, err
	}

	cfg.Config = *pc

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("opening the monitoring connection to %s: %w", pc.Database, err)
	}

	return &monitor{conn: conn}, nil
}

func (m *monitor) close() {
	_ = m.conn.Close(context.Background())
}

// settings is what the receiver needs to know of the server's
// configuration.
type settings struct {
	version int
	segSize uint64
	// keepSize is max_slot_wal_keep_size in bytes; -1 is unbounded.
	keepSize int64
}

func (m *monitor) settings(ctx context.Context) (settings, error) {
	var (
		s        settings
		keepSize int64
	)

	err := m.conn.QueryRow(ctx, `SELECT current_setting('server_version_num')::int,
		(SELECT setting::bigint FROM pg_settings WHERE name = 'wal_segment_size'),
		coalesce((SELECT setting::bigint FROM pg_settings WHERE name = 'max_slot_wal_keep_size'), -1)`).Scan(&s.version, &s.segSize, &keepSize)
	if err != nil {
		return s, fmt.Errorf("reading the server's settings: %w", err)
	}

	s.keepSize = -1
	if keepSize >= 0 {
		s.keepSize = keepSize << 20
	}

	return s, nil
}

// slotState is a replication slot as pg_replication_slots shows it.
type slotState struct {
	physical   bool
	active     bool
	walStatus  string
	restartLSN uint64
	// safeWALSize is how much more WAL the server may write before the
	// slot is lost; -1 when nothing bounds it.
	safeWALSize int64
}

// slot reads the slot; ok is false when it doesn't exist.
func (m *monitor) slot(ctx context.Context, name string) (slotState, bool, error) {
	var (
		s       slotState
		kind    string
		restart string
	)

	err := m.conn.QueryRow(ctx, `SELECT slot_type, active, coalesce(wal_status, ''), coalesce(restart_lsn::text, ''), coalesce(safe_wal_size, -1)
		FROM pg_replication_slots WHERE slot_name = $1`, name).Scan(&kind, &s.active, &s.walStatus, &restart, &s.safeWALSize)
	if errors.Is(err, pgx.ErrNoRows) {
		return s, false, nil
	}

	if err != nil {
		return s, false, fmt.Errorf("reading the replication slot %s: %w", name, err)
	}

	s.physical = kind == "physical"

	if restart != "" {
		if s.restartLSN, err = ParseLSN(restart); err != nil {
			return s, false, err
		}
	}

	return s, true, nil
}

func (m *monitor) currentLSN(ctx context.Context) (uint64, error) {
	var lsn string
	if err := m.conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsn); err != nil {
		return 0, err
	}

	return ParseLSN(lsn)
}

// syncState is pg_stat_replication's sync_state for the walsender with
// pid, and the flush position the server has from it. listed is false
// while it isn't listed yet; hidden is true when the role may not see it.
func (m *monitor) syncState(ctx context.Context, pid uint32) (state string, flush uint64, listed, hidden bool, err error) {
	var s, f *string

	err = m.conn.QueryRow(ctx, "SELECT sync_state, flush_lsn::text FROM pg_stat_replication WHERE pid = $1", int64(pid)).Scan(&s, &f)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", 0, false, false, nil
	}

	if err != nil {
		return "", 0, false, false, err
	}

	if s == nil {
		return "", 0, true, true, nil
	}

	if f != nil {
		if flush, err = ParseLSN(*f); err != nil {
			return "", 0, false, false, err
		}
	}

	return *s, flush, true, false, nil
}

// checkAsync fails unless the walsender with pid streams asynchronously.
// Postgres counts a standby as asynchronous until it reports a flush
// position, so the state is read once the server has flush from it. A role
// without pg_read_all_stats sees no sync_state; then an empty
// synchronous_standby_names is the proof.
func (m *monitor) checkAsync(ctx context.Context, pid uint32, flush uint64) error {
	for range 50 {
		state, reported, listed, hidden, err := m.syncState(ctx, pid)
		if err != nil {
			return fmt.Errorf("reading pg_stat_replication: %w", err)
		}

		if hidden {
			var names string
			if err := m.conn.QueryRow(ctx, "SELECT current_setting('synchronous_standby_names')").Scan(&names); err != nil {
				return err
			}

			if names == "" {
				return nil
			}

			return errors.New("synchronous_standby_names is set, and the role can't see whether it names the receiver: GRANT pg_read_all_stats to it")
		}

		if listed && reported >= flush {
			if state != "async" {
				return fmt.Errorf("%w (sync_state %s)", ErrSynchronousStandby, state)
			}

			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}

	return errors.New("pg_stat_replication doesn't show the receiver's flush position")
}

// Check connects to the cluster at connString as the receiver and the
// base backups do, and lists what it lacks for replication capture: a
// supported version, the role's REPLICATION attribute, a capped slot
// unless allowUncapped, a free slot, and pg_hba.conf entries for both
// connections. An empty list means nothing is missing.
func Check(ctx context.Context, connString, slot string, allowUncapped bool) []string {
	m, err := connectMonitor(ctx, connString, "goback")
	if err != nil {
		return []string{err.Error() + "; pg_hba.conf needs a line for a normal connection to that database"}
	}
	defer m.close()

	var problems []string

	s, err := m.settings(ctx)
	if err != nil {
		return []string{err.Error()}
	}

	switch {
	case s.version < minReceiveVersion:
		problems = append(problems, fmt.Sprintf("Postgres %s is older than 14, the oldest replication capture supports", versionString(s.version)))
	case s.version < minInProcessBaseVersion:
		problems = append(problems, fmt.Sprintf("Postgres %s takes base backups through pg_basebackup, which must be installed in the same major version", versionString(s.version)))
	}

	var (
		role        string
		replication bool
	)

	if err := m.conn.QueryRow(ctx, "SELECT rolname, rolreplication FROM pg_roles WHERE rolname = current_user").Scan(&role, &replication); err != nil {
		problems = append(problems, "reading the role: "+err.Error())
	} else if !replication {
		problems = append(problems, fmt.Sprintf("the role %s lacks REPLICATION: ALTER ROLE %s REPLICATION", role, role))
	}

	if s.keepSize < 0 && !allowUncapped {
		problems = append(problems, ErrUncappedSlot.Error())
	}

	if _, ok, err := m.slot(ctx, slot); err != nil {
		problems = append(problems, err.Error())
	} else if !ok {
		var free int

		if err := m.conn.QueryRow(ctx, "SELECT current_setting('max_replication_slots')::int - (SELECT count(*) FROM pg_replication_slots)").Scan(&free); err != nil {
			problems = append(problems, "counting replication slots: "+err.Error())
		} else if free < 1 {
			problems = append(problems, "no free replication slot: raise max_replication_slots, which takes a restart")
		}
	}

	conn, err := connectReplication(ctx, connString, "goback")
	if err != nil {
		problems = append(problems, err.Error()+"; pg_hba.conf needs a host replication line for the role")
	} else {
		conn.Close(context.Background())
	}

	return problems
}

func versionString(num int) string {
	return strconv.Itoa(num/10000) + "." + strconv.Itoa(num%10000)
}
