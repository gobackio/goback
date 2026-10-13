package job

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/postgres"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/storage/cache"
)

// PostgresBaseSettings say how PostgresBase takes a base backup.
type PostgresBaseSettings struct {
	Set string
	// DBName is the connection string pg_basebackup connects with; empty
	// leaves it to the PG* variables.
	DBName string
	// PGBasebackup, when set, is the pg_basebackup that takes the backup;
	// its major version must match the server's. When empty, the backup
	// is taken over a replication connection of its own from Postgres 15
	// on, and by the pg_basebackup on the PATH before.
	PGBasebackup string
	// InitialRetention, when set, is the retention policy the set starts
	// with if this run creates it; a set that exists keeps its own.
	InitialRetention *retention.Policy
}

// PostgresBase takes a base backup and commits it to the set. When ctx ends
// first, the backup stops and nothing is committed.
func PostgresBase(ctx context.Context, env *Env, s PostgresBaseSettings) (*backup.WalkResult, error) {
	run := &postgres.BaseBackup{
		Walker: &backup.Walker{
			Index:            env.Index,
			Objects:          env.Index,
			Sessions:         env.sessions(),
			InitialRetention: s.InitialRetention,
			Set:              s.Set,
			AgentID:          env.AgentID,
			Key:              env.Key,
			Logger:           env.Logger,
			Workers:          4,
		},
	}

	program := s.PGBasebackup
	if program == "" {
		stream, err := postgres.StartBase(ctx, s.DBName)
		if err == nil {
			defer stream.Close()
			return run.Run(ctx, stream.Tar, stream.Wait)
		}

		if !errors.Is(err, postgres.ErrNoBaseStream) {
			return nil, err
		}

		program = "pg_basebackup"
	}

	args := []string{"-D", "-", "-Ft", "-X", "fetch", "--checkpoint=fast"}
	if s.DBName != "" {
		args = append(args, "--dbname", s.DBName)
	}

	stderr, flush := env.stderr("pg_basebackup")
	defer flush()

	cmd := exec.CommandContext(ctx, program, args...)
	cmd.Stderr = stderr

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting pg_basebackup: %w", err)
	}

	waited := false
	wait := func() error {
		waited = true
		if err := cmd.Wait(); err != nil {
			return fmt.Errorf("pg_basebackup: %w", err)
		}

		return nil
	}

	result, err := run.Run(ctx, stdout, wait)
	if !waited {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}

	return result, err
}

// PostgresReceiveSettings say how PostgresReceive streams WAL.
type PostgresReceiveSettings struct {
	Set string
	// BaseSet is the set the cluster's base backups go to.
	BaseSet string
	// Spool is where WAL waits for its commit.
	Spool string
	// DBName is the connection string of the replication and monitoring
	// connections; empty leaves it to the PG* variables.
	DBName string
	// Slot is the replication slot; postgres.SlotName of the set when
	// empty.
	Slot string
	// AllowUncappedSlot streams when max_slot_wal_keep_size is -1.
	AllowUncappedSlot bool
	CommitEvery       time.Duration
	CommitBytes       int64
	// Window is how far back point-in-time recovery reaches.
	Window time.Duration
	// InitialRetention, when set, is the retention policy the set starts
	// with if this run creates it; a set that exists keeps its own.
	InitialRetention *retention.Policy
}

// PostgresReceive streams the cluster's WAL through a replication slot
// into the spool and commits it to the set, until stop is closed; then it
// makes its last commit and returns nil. committed and report, when set,
// hear of each commit and of how the receiver stands.
func PostgresReceive(ctx context.Context, env *Env, s PostgresReceiveSettings, stop <-chan struct{},
	committed func(*backup.WalkResult), report func(postgres.ReceiverStatus),
) error {
	spool := postgres.Spool{Dir: s.Spool}

	objects := backup.ObjectStore(env.Index)
	if env.TreeCache != nil {
		objects = cache.New(env.TreeCache, env.Index)
	}

	r := &postgres.Receiver{
		ConnString:        s.DBName,
		Slot:              s.Slot,
		AllowUncappedSlot: s.AllowUncappedSlot,
		Spool:             spool,
		Walker: func() *backup.Walker {
			return &backup.Walker{
				Index:            env.Index,
				Objects:          objects,
				Sessions:         env.sessions(),
				InitialRetention: s.InitialRetention,
				Set:              s.Set,
				AgentID:          env.AgentID,
				Key:              env.Key,
				Logger:           env.Logger,
				Root:             spool.Dir,
				Workers:          4,
				ReadRetries:      3,
			}
		},
		BaseSet:     s.BaseSet,
		Window:      s.Window,
		CommitEvery: s.CommitEvery,
		CommitBytes: s.CommitBytes,
		Logger:      env.logger(),
		Committed:   committed,
		Report:      report,
	}

	return r.Run(ctx, stop)
}
