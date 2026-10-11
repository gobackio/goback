package job

import (
	"context"
	"io"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/mysql"
	"github.com/gobackio/goback/storage/cache"
)

// MySQLClient names the client programs that reach a MySQL server.
type MySQLClient struct {
	// DefaultsExtraFile is the option file the programs read before their
	// own, for the connection and its password; empty reads ~/.my.cnf
	// alone.
	DefaultsExtraFile string
	Mysql             string
	Mysqldump         string
}

func (c MySQLClient) exec(stderr io.Writer) *mysql.Exec {
	return &mysql.Exec{
		Mysql:        c.Mysql,
		Mysqldump:    c.Mysqldump,
		DefaultsFile: c.DefaultsExtraFile,
		Stderr:       stderr,
	}
}

// MySQLDumpSettings say how MySQLDump dumps a server.
type MySQLDumpSettings struct {
	Set    string
	Client MySQLClient
	// BinlogSet is the set the server's binlogs go to, whose chain the dump
	// joins; empty when there is none.
	BinlogSet string
	// NoBinlogPosition records no binary log position even with log_bin
	// on, which spares the dump the global read lock.
	NoBinlogPosition bool
}

// MySQLDump dumps the server with mysqldump and commits the dump to the set.
func MySQLDump(ctx context.Context, env *Env, s MySQLDumpSettings) (*backup.WalkResult, error) {
	stderr, flush := env.stderr("mysqldump")
	defer flush()

	run := &mysql.Dump{
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
		Client:     s.Client.exec(stderr),
		BinlogSet:  s.BinlogSet,
		NoPosition: s.NoBinlogPosition,
		Logger:     env.Logger,
	}

	return run.Run(ctx)
}

// MySQLBinlogSettings say how MySQLBinlog commits a server's binlogs.
type MySQLBinlogSettings struct {
	Set    string
	Client MySQLClient
	// BinlogDir is where the server writes its binlogs; empty asks the
	// server.
	BinlogDir string
	// DumpSet is the set the server's dumps go to.
	DumpSet string
	// Window is how far back point-in-time recovery reaches.
	Window time.Duration
}

// MySQLBinlog rotates the binary log when it holds anything new and commits
// the closed binlogs to the set. It commits nothing, and returns nil, when
// there is no new closed binlog.
func MySQLBinlog(ctx context.Context, env *Env, s MySQLBinlogSettings) (*backup.WalkResult, error) {
	stderr, flush := env.stderr("mysql")
	defer flush()

	objects := backup.ObjectStore(env.Index)
	if env.TreeCache != nil {
		objects = cache.New(env.TreeCache, env.Index)
	}

	run := &mysql.BinlogBackup{
		Walker: &backup.Walker{
			Index:       env.Index,
			Objects:     objects,
			Sessions:    env.sessions(),
			Set:         s.Set,
			AgentID:     env.AgentID,
			Key:         env.Key,
			Logger:      env.Logger,
			Workers:     4,
			ReadRetries: 3,
		},
		Client:  s.Client.exec(stderr),
		Dir:     s.BinlogDir,
		DumpSet: s.DumpSet,
		Window:  s.Window,
		Now:     time.Now,
	}

	return run.Run(ctx)
}

// MySQLReceiveSettings say how MySQLReceive receives a server's binlogs.
type MySQLReceiveSettings struct {
	Set string
	// DumpSet is the set the server's dumps go to.
	DumpSet string
	// Spool is where binlogs wait for their commit.
	Spool string
	// DefaultsExtraFile is the option file read before ~/.my.cnf for the
	// connection and its password.
	DefaultsExtraFile string
	// ServerID is the replica server id; mysql.ReplicaServerID of the set
	// when zero.
	ServerID    uint32
	CommitEvery time.Duration
	CommitBytes int64
	// Window is how far back point-in-time recovery reaches.
	Window time.Duration
}

// MySQLReceive receives the server's binlogs as a replica into the spool
// and commits them to the set, until stop is closed; then it makes its
// last commit and returns nil. committed and report, when set, hear of
// each commit and of how the receiver stands.
func MySQLReceive(ctx context.Context, env *Env, s MySQLReceiveSettings, stop <-chan struct{},
	committed func(*backup.WalkResult), report func(mysql.ReceiverStatus),
) error {
	spool := mysql.Spool{Dir: s.Spool}

	objects := backup.ObjectStore(env.Index)
	if env.TreeCache != nil {
		objects = cache.New(env.TreeCache, env.Index)
	}

	r := &mysql.Receiver{
		DefaultsExtraFile: s.DefaultsExtraFile,
		ServerID:          s.ServerID,
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
		DumpSet:     s.DumpSet,
		Window:      s.Window,
		CommitEvery: s.CommitEvery,
		CommitBytes: s.CommitBytes,
		Logger:      env.logger(),
		Committed:   committed,
		Report:      report,
	}

	return r.Run(ctx, stop)
}
