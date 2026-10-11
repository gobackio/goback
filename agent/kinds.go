package agent

import (
	"context"
	"errors"
	"net/url"
	"runtime"
	"strings"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/job"
	"github.com/gobackio/goback/backup/mysql"

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

// logEvery is how often file-based log capture commits.
const logEvery = time.Minute

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

// logTask is the task that captures a database's log to set every
// minute, once base holds a complete commit.
func logTask(kind, set, base string, run RunFunc) *Task {
	return &Task{
		Name:  set,
		Set:   set,
		Kind:  kind,
		Plan:  Every(logEvery),
		Log:   true,
		After: base,
		Run:   run,
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
// PITR, the WAL archive_command copies into Spool.
type PostgresBackup struct {
	DBName       string        `yaml:"dbname"`
	PGBasebackup string        `yaml:"pg-basebackup"`
	Spool        string        `yaml:"spool"`
	Window       time.Duration `yaml:"window"`
	PITR         bool          `yaml:"pitr"`
}

// UnmarshalYAML fills in the postgres commands' defaults before decoding.
func (p *PostgresBackup) UnmarshalYAML(unmarshal func(any) error) error {
	*p = PostgresBackup{PGBasebackup: "pg_basebackup", Window: 14 * 24 * time.Hour, PITR: true}

	type plain PostgresBackup

	return unmarshal((*plain)(p))
}

func (p *PostgresBackup) validate(*Backup) []error {
	var errs []error

	if holdsPassword(p.DBName) {
		errs = append(errs, errors.New("postgres.dbname holds a password; put it in .pgpass or PGPASSFILE"))
	}

	if p.PITR && p.Spool == "" {
		errs = append(errs, errors.New("postgres.spool is required for point-in-time recovery: the directory archive_command copies WAL into; or set pitr: false"))
	}

	if p.Window <= 0 {
		errs = append(errs, errors.New("postgres.window must be positive"))
	}

	return errs
}

func (p *PostgresBackup) tasks(b *Backup) []*Task {
	base, wal := b.Name+"-base", b.Name+"-wal"

	baseSettings := job.PostgresBaseSettings{Set: base, DBName: p.DBName, PGBasebackup: p.PGBasebackup}
	tasks := []*Task{b.fullTask("postgres-base", base, defaultDatabasePlan, func(ctx context.Context, env *job.Env) (*backup.WalkResult, error) {
		return job.PostgresBase(ctx, env, baseSettings)
	})}

	if p.PITR {
		walSettings := job.PostgresWALSettings{Set: wal, Spool: p.Spool, BaseSet: base, Window: p.Window}
		tasks = append(tasks, logTask("postgres-wal", wal, base, func(ctx context.Context, env *job.Env) (*backup.WalkResult, error) {
			return job.PostgresWAL(ctx, env, walSettings)
		}))
	}

	return tasks
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

// MySQLBackup backs up a MySQL server: dumps and, with PITR and log_bin
// on, its binlogs.
type MySQLBackup struct {
	DefaultsExtraFile string        `yaml:"defaults-extra-file"`
	Mysql             string        `yaml:"mysql"`
	Mysqldump         string        `yaml:"mysqldump"`
	BinlogDir         string        `yaml:"binlog-dir"`
	Window            time.Duration `yaml:"window"`
	PITR              bool          `yaml:"pitr"`
}

// UnmarshalYAML fills in the mysql commands' defaults before decoding.
func (m *MySQLBackup) UnmarshalYAML(unmarshal func(any) error) error {
	*m = MySQLBackup{Mysql: "mysql", Mysqldump: "mysqldump", Window: 14 * 24 * time.Hour, PITR: true}

	type plain MySQLBackup

	return unmarshal((*plain)(m))
}

func (m *MySQLBackup) validate(*Backup) []error {
	if m.Window <= 0 {
		return []error{errors.New("mysql.window must be positive")}
	}

	return nil
}

func (m *MySQLBackup) tasks(b *Backup) []*Task {
	dump, binlog := b.Name+"-dump", b.Name+"-binlog"

	client := job.MySQLClient{DefaultsExtraFile: m.DefaultsExtraFile, Mysql: m.Mysql, Mysqldump: m.Mysqldump}

	dumpSettings := job.MySQLDumpSettings{Set: dump, Client: client}
	if m.PITR {
		dumpSettings.BinlogSet = binlog
	}

	tasks := []*Task{b.fullTask("mysql-dump", dump, defaultDatabasePlan, func(ctx context.Context, env *job.Env) (*backup.WalkResult, error) {
		return job.MySQLDump(ctx, env, dumpSettings)
	})}

	if m.PITR {
		binlogSettings := job.MySQLBinlogSettings{Set: binlog, Client: client, BinlogDir: m.BinlogDir, DumpSet: dump, Window: m.Window}
		tasks = append(tasks, logTask("mysql-binlog", binlog, dump, func(ctx context.Context, env *job.Env) (*backup.WalkResult, error) {
			result, err := job.MySQLBinlog(ctx, env, binlogSettings)
			if errors.Is(err, mysql.ErrNoBinlog) {
				// log capture runs only while log_bin is on
				return nil, nil
			}

			return result, err
		}))
	}

	return tasks
}
