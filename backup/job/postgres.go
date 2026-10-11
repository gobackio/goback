package job

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/postgres"
	"github.com/gobackio/goback/storage/cache"
)

// PostgresBaseSettings say how PostgresBase takes a base backup.
type PostgresBaseSettings struct {
	Set string
	// DBName is the connection string pg_basebackup connects with; empty
	// leaves it to the PG* variables.
	DBName string
	// PGBasebackup is the pg_basebackup to run; its major version must
	// match the server's.
	PGBasebackup string
}

// PostgresBase takes a base backup with pg_basebackup and commits it to the
// set. When ctx ends first, pg_basebackup is killed and nothing is
// committed.
func PostgresBase(ctx context.Context, env *Env, s PostgresBaseSettings) (*backup.WalkResult, error) {
	run := &postgres.BaseBackup{
		Walker: &backup.Walker{
			Index:    env.Index,
			Objects:  env.Index,
			Sessions: env.sessions(),
			Set:      s.Set,
			AgentID:  env.AgentID,
			Key:      env.Key,
			Logger:   env.Logger,
			Workers:  4,
		},
	}

	args := []string{"-D", "-", "-Ft", "-X", "fetch", "--checkpoint=fast"}
	if s.DBName != "" {
		args = append(args, "--dbname", s.DBName)
	}

	stderr, flush := env.stderr("pg_basebackup")
	defer flush()

	cmd := exec.CommandContext(ctx, s.PGBasebackup, args...)
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

// PostgresWALSettings say how PostgresWAL commits the spool.
type PostgresWALSettings struct {
	Set string
	// Spool is the directory archive_command copies WAL files into.
	Spool string
	// BaseSet is the set the cluster's base backups go to.
	BaseSet string
	// Window is how far back point-in-time recovery reaches.
	Window time.Duration
}

// PostgresWAL commits the spooled WAL files to the set, then deletes them
// from the spool. It commits nothing, and returns nil, when the spool is
// empty.
func PostgresWAL(ctx context.Context, env *Env, s PostgresWALSettings) (*backup.WalkResult, error) {
	spool := postgres.Spool{Dir: s.Spool}

	objects := backup.ObjectStore(env.Index)
	if env.TreeCache != nil {
		objects = cache.New(env.TreeCache, env.Index)
	}

	run := &postgres.WALBackup{
		Walker: &backup.Walker{
			Index:       env.Index,
			Objects:     objects,
			Sessions:    env.sessions(),
			Set:         s.Set,
			AgentID:     env.AgentID,
			Key:         env.Key,
			Logger:      env.Logger,
			Root:        spool.Dir,
			Workers:     4,
			ReadRetries: 3,
		},
		Spool:   spool,
		BaseSet: s.BaseSet,
		Window:  s.Window,
		Now:     time.Now,
	}

	return run.Run(ctx)
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
				Index:       env.Index,
				Objects:     objects,
				Sessions:    env.sessions(),
				Set:         s.Set,
				AgentID:     env.AgentID,
				Key:         env.Key,
				Logger:      env.Logger,
				Root:        spool.Dir,
				Workers:     4,
				ReadRetries: 3,
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
