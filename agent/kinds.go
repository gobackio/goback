package agent

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/job"
	"github.com/gobackio/goback/backup/mysql"
	"github.com/gobackio/goback/backup/postgres"

	"github.com/dustin/go-humanize"
	"go.yaml.in/yaml/v3"
)

// kind is what a backup protects. It checks its settings and says which
// tasks the backup runs; Tasks is the one place a backup becomes tasks.
type kind interface {
	validate(b *Backup) []error
	tasks(b *Backup) []*Task
}

// kindKeys are the keys that name a kind in a backup, as kinds lists
// them.
var kindKeys = []string{"path", "postgres", "mysql"}

func (b *Backup) kinds() []kind {
	var kinds []kind

	if b.Path != nil {
		kinds = append(kinds, b.Path)
	}

	if b.Postgres != nil {
		kinds = append(kinds, b.Postgres)
	}

	if b.MySQL != nil {
		kinds = append(kinds, b.MySQL)
	}

	return kinds
}

// Tasks lists what the config's backups run, in config order.
func (c *Config) Tasks() []*Task {
	var tasks []*Task

	for i := range c.Backups {
		b := &c.Backups[i]

		k := b.kind()
		if k == nil {
			continue
		}

		spec, _ := yaml.Marshal(b)

		for _, t := range k.tasks(b) {
			t.Backup = b.Name
			t.Spec = string(spec)
			tasks = append(tasks, t)
		}
	}

	return tasks
}

// defaultDatabasePlan is when a database's full backups run unless the
// backup says otherwise: daily at 03:00.
const defaultDatabasePlan = "0 3 * * *"

// fullPlan is when b's full backups run, or nil when b doesn't say and
// the kind has no default.
func (b *Backup) fullPlan(fallback string) *Plan {
	switch {
	case b.Every > 0:
		return Every(b.Every)
	case b.Cron != "":
		p, err := Cron(b.Cron, time.Local)
		if err != nil {
			return nil
		}

		return p
	case fallback != "":
		p, _ := Cron(fallback, time.Local)
		return p
	}

	return nil
}

// fullTask is the task that runs b's full backups to set.
func (b *Backup) fullTask(kind, set, fallback string, run RunFunc) *Task {
	return &Task{
		Name:    set,
		Set:     set,
		Kind:    kind,
		Plan:    b.fullPlan(fallback),
		Jitter:  b.Jitter,
		Timeout: b.Timeout,
		Run:     run,
	}
}

// PathBackup backs up a directory, with commit new's settings.
type PathBackup struct {
	Dir                string            `yaml:"dir"`
	Include            []string          `yaml:"include"`
	Exclude            []string          `yaml:"exclude"`
	Workers            int               `yaml:"workers"`
	ForceHash          int               `yaml:"force-hash"`
	CheckpointInterval time.Duration     `yaml:"checkpoint-interval"`
	ScanWorkers        int               `yaml:"scan-workers"`
	ReadRetries        int               `yaml:"read-retries"`
	LostRetries        int               `yaml:"lost-retries"`
	ProgressInterval   time.Duration     `yaml:"progress-interval"`
	Meta               map[string]string `yaml:"meta"`
	PreHook            string            `yaml:"pre-hook"`
	PostHook           string            `yaml:"post-hook"`
	HookTimeout        time.Duration     `yaml:"hook-timeout"`
	HookOptional       bool              `yaml:"hook-optional"`
}

// UnmarshalYAML fills in commit new's defaults before decoding. It takes
// the decoder's own unmarshal, which keeps unknown keys an error.
func (p *PathBackup) UnmarshalYAML(unmarshal func(any) error) error {
	*p = PathBackup{
		Workers:            runtime.NumCPU(),
		CheckpointInterval: 30 * time.Minute,
		ScanWorkers:        backup.DefaultScanWorkers,
		ReadRetries:        3,
		LostRetries:        1,
		ProgressInterval:   backup.DefaultProgressInterval,
		HookTimeout:        5 * time.Minute,
	}

	type plain PathBackup

	return unmarshal((*plain)(p))
}

func (p *PathBackup) validate(b *Backup) []error {
	var errs []error

	if p.Dir == "" {
		errs = append(errs, errors.New("path.dir is required"))
	}

	if b.Every == 0 && b.Cron == "" {
		errs = append(errs, errors.New("a path backup needs every or cron"))
	}

	return errs
}

func (p *PathBackup) tasks(b *Backup) []*Task {
	settings := job.PathSettings{
		Set:                b.Name,
		Dir:                p.Dir,
		Include:            p.Include,
		Exclude:            p.Exclude,
		Workers:            p.Workers,
		ForceHash:          p.ForceHash,
		CheckpointInterval: p.CheckpointInterval,
		ScanWorkers:        p.ScanWorkers,
		ReadRetries:        p.ReadRetries,
		LostRetries:        p.LostRetries,
		ProgressInterval:   p.ProgressInterval,
		Meta:               p.Meta,
		PreHook:            p.PreHook,
		PostHook:           p.PostHook,
		HookTimeout:        p.HookTimeout,
		HookOptional:       p.HookOptional,
	}

	return []*Task{b.fullTask("path", b.Name, "", func(ctx context.Context, env *job.Env) (*backup.WalkResult, error) {
		return job.Path(ctx, env, settings)
	})}
}

// PostgresBackup backs up a Postgres cluster: base backups and, with
// PITR, its WAL streamed through a replication slot, with goback
// postgres receive's settings.
type PostgresBackup struct {
	DBName       string `yaml:"dbname"`
	PGBasebackup string `yaml:"pg-basebackup"`
	// Spool is where WAL waits for its commit; <state-dir>/spool/<set>
	// when empty.
	Spool             string        `yaml:"spool"`
	Slot              string        `yaml:"slot"`
	CommitEvery       time.Duration `yaml:"commit-every"`
	CommitBytes       string        `yaml:"commit-bytes"`
	AllowUncappedSlot bool          `yaml:"allow-uncapped-slot"`
	Window            time.Duration `yaml:"window"`
	PITR              bool          `yaml:"pitr"`
}

// UnmarshalYAML fills in the postgres commands' defaults before decoding.
func (p *PostgresBackup) UnmarshalYAML(unmarshal func(any) error) error {
	*p = PostgresBackup{CommitEvery: postgres.DefaultCommitEvery, CommitBytes: "256MiB", Window: 14 * 24 * time.Hour, PITR: true}

	type plain PostgresBackup

	return unmarshal((*plain)(p))
}

func (p *PostgresBackup) validate(*Backup) []error {
	var errs []error

	if holdsPassword(p.DBName) {
		errs = append(errs, errors.New("postgres.dbname holds a password; put it in .pgpass or PGPASSFILE"))
	}

	if p.Window <= 0 {
		errs = append(errs, errors.New("postgres.window must be positive"))
	}

	if p.CommitEvery < postgres.MinCommitEvery {
		errs = append(errs, fmt.Errorf("postgres.commit-every must be at least %s", postgres.MinCommitEvery))
	}

	if _, err := humanize.ParseBytes(p.CommitBytes); err != nil {
		errs = append(errs, fmt.Errorf("postgres.commit-bytes: %w", err))
	}

	return errs
}

// slot is the replication slot the backup's receiver streams through.
func (p *PostgresBackup) slot(b *Backup) string {
	if p.Slot != "" {
		return p.Slot
	}

	return postgres.SlotName(b.Name + "-wal")
}

func (p *PostgresBackup) tasks(b *Backup) []*Task {
	base, wal := b.Name+"-base", b.Name+"-wal"

	baseSettings := job.PostgresBaseSettings{Set: base, DBName: p.DBName, PGBasebackup: p.PGBasebackup}
	tasks := []*Task{b.fullTask("postgres-base", base, defaultDatabasePlan, func(ctx context.Context, env *job.Env) (*backup.WalkResult, error) {
		return job.PostgresBase(ctx, env, baseSettings)
	})}

	if !p.PITR {
		return tasks
	}

	commitBytes, _ := humanize.ParseBytes(p.CommitBytes)
	settings := job.PostgresReceiveSettings{
		Set:               wal,
		BaseSet:           base,
		Spool:             p.Spool,
		DBName:            p.DBName,
		Slot:              p.slot(b),
		AllowUncappedSlot: p.AllowUncappedSlot,
		CommitEvery:       p.CommitEvery,
		CommitBytes:       int64(commitBytes),
		Window:            p.Window,
	}

	// the receiver starts with the agent, not after the first base backup,
	// so its slot keeps the WAL from that backup's start on
	return append(tasks, &Task{
		Name:       wal,
		Set:        wal,
		Kind:       "postgres-receive",
		Log:        true,
		StaleAfter: 3 * p.CommitEvery,
		Serve: func(ctx context.Context, env *job.Env, c *Control) error {
			s := settings
			if s.Spool == "" {
				if c.StateDir() == "" {
					return errors.New("postgres.spool is required without a state dir")
				}

				s.Spool = filepath.Join(c.StateDir(), "spool", wal)
			}

			return job.PostgresReceive(ctx, env, s, c.Stop(), c.Committed, func(st postgres.ReceiverStatus) { c.Report(st, st.Healthy) })
		},
	})
}

// CheckDatabase connects to the backup's database and lists what it lacks
// for the backup as configured; nil for a backup of no database.
func (b *Backup) CheckDatabase(ctx context.Context) []string {
	if m := b.MySQL; m != nil {
		if !m.PITR {
			return mysql.CheckDump(ctx, m.DefaultsExtraFile, !m.NoBinlogPosition)
		}

		return mysql.Check(ctx, m.DefaultsExtraFile, m.serverID(b))
	}

	if b.Postgres == nil {
		return nil
	}

	p := b.Postgres

	if !p.PITR {
		return postgres.Check(ctx, p.DBName, "", false)
	}

	return postgres.Check(ctx, p.DBName, p.slot(b), p.AllowUncappedSlot)
}

// holdsPassword reports whether a libpq connection string, as key=value
// pairs or a URL, carries a password.
func holdsPassword(conninfo string) bool {
	if u, err := url.Parse(conninfo); err == nil && u.User != nil {
		if _, ok := u.User.Password(); ok {
			return true
		}
	}

	lower := strings.ToLower(conninfo)

	return strings.Contains(lower, "password=")
}

// MySQLBackup backs up a MySQL server: dumps and, with PITR, its binlogs
// received as a replica, with goback mysql receive's settings.
type MySQLBackup struct {
	DefaultsExtraFile string `yaml:"defaults-extra-file"`
	Mysql             string `yaml:"mysql"`
	Mysqldump         string `yaml:"mysqldump"`
	// Spool is where binlogs wait for their commit; <state-dir>/spool/<set>
	// when empty.
	Spool       string        `yaml:"spool"`
	ServerID    uint32        `yaml:"server-id"`
	CommitEvery time.Duration `yaml:"commit-every"`
	CommitBytes string        `yaml:"commit-bytes"`
	Window      time.Duration `yaml:"window"`
	PITR        bool          `yaml:"pitr"`
	// NoBinlogPosition, with pitr off, has dumps record no binlog position,
	// which spares them the global read lock.
	NoBinlogPosition bool `yaml:"no-binlog-position"`
}

// UnmarshalYAML fills in the mysql commands' defaults before decoding.
func (m *MySQLBackup) UnmarshalYAML(unmarshal func(any) error) error {
	*m = MySQLBackup{Mysql: "mysql", Mysqldump: "mysqldump", CommitEvery: mysql.DefaultCommitEvery, CommitBytes: "256MiB", Window: 14 * 24 * time.Hour, PITR: true}

	type plain MySQLBackup

	return unmarshal((*plain)(m))
}

func (m *MySQLBackup) validate(*Backup) []error {
	var errs []error

	if m.Window <= 0 {
		errs = append(errs, errors.New("mysql.window must be positive"))
	}

	if m.PITR && m.NoBinlogPosition {
		errs = append(errs, errors.New("mysql.no-binlog-position needs pitr: false; binlogs are replayed from a dump's position"))
	}

	if m.CommitEvery < mysql.MinCommitEvery {
		errs = append(errs, fmt.Errorf("mysql.commit-every must be at least %s", mysql.MinCommitEvery))
	}

	if _, err := humanize.ParseBytes(m.CommitBytes); err != nil {
		errs = append(errs, fmt.Errorf("mysql.commit-bytes: %w", err))
	}

	return errs
}

// serverID is the replica server id the backup's receiver connects with.
func (m *MySQLBackup) serverID(b *Backup) uint32 {
	if m.ServerID != 0 {
		return m.ServerID
	}

	return mysql.ReplicaServerID(b.Name + "-binlog")
}

// noBinlogWait is how long a receiver waits before it looks again at a
// server whose binary log is off.
const noBinlogWait = time.Minute

func (m *MySQLBackup) tasks(b *Backup) []*Task {
	dump, binlog := b.Name+"-dump", b.Name+"-binlog"

	client := job.MySQLClient{DefaultsExtraFile: m.DefaultsExtraFile, Mysql: m.Mysql, Mysqldump: m.Mysqldump}

	dumpSettings := job.MySQLDumpSettings{Set: dump, Client: client, NoBinlogPosition: m.NoBinlogPosition}
	if m.PITR {
		dumpSettings.BinlogSet = binlog
	}

	tasks := []*Task{b.fullTask("mysql-dump", dump, defaultDatabasePlan, func(ctx context.Context, env *job.Env) (*backup.WalkResult, error) {
		return job.MySQLDump(ctx, env, dumpSettings)
	})}

	if !m.PITR {
		return tasks
	}

	commitBytes, _ := humanize.ParseBytes(m.CommitBytes)
	settings := job.MySQLReceiveSettings{
		Set:               binlog,
		DumpSet:           dump,
		Spool:             m.Spool,
		DefaultsExtraFile: m.DefaultsExtraFile,
		ServerID:          m.serverID(b),
		CommitEvery:       m.CommitEvery,
		CommitBytes:       int64(commitBytes),
		Window:            m.Window,
	}

	// the receiver starts at a dump's binlog position, so it waits for one
	return append(tasks, &Task{
		Name:       binlog,
		Set:        binlog,
		Kind:       "mysql-receive",
		Log:        true,
		After:      dump,
		StaleAfter: 3 * m.CommitEvery,
		Serve: func(ctx context.Context, env *job.Env, c *Control) error {
			s := settings
			if s.Spool == "" {
				if c.StateDir() == "" {
					return errors.New("mysql.spool is required without a state dir")
				}

				s.Spool = filepath.Join(c.StateDir(), "spool", binlog)
			}

			for {
				err := job.MySQLReceive(ctx, env, s, c.Stop(), c.Committed, func(st mysql.ReceiverStatus) { c.Report(st, st.Healthy) })
				if !errors.Is(err, mysql.ErrNoBinlog) {
					return err
				}

				// log capture runs only while log_bin is on
				c.Report(map[string]any{"log_bin": false}, true)

				select {
				case <-c.Stop():
					return nil
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(noBinlogWait):
				}
			}
		},
	})
}
