package mysql

import (
	"log"

	"github.com/gobackio/goback"
	"github.com/gobackio/goback/backup/job"
	"github.com/gobackio/goback/backup/mysql"
	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/common/views"

	"github.com/urfave/cli"
)

var dumpCmd = cli.Command{
	Name:  "dump",
	Usage: "dump the server with mysqldump and commit it to the set named by --set",
	Description: "The client programs connect as MySQL's option files say: ~/.my.cnf, or --defaults-extra-file.\n" +
		"With log_bin on, the dump records the binary log position it is consistent with.",
	Flags: withClient(
		cli.StringFlag{
			Name:  "binlog-set",
			Usage: "set the server's binlogs go to, whose chain the dump joins",
		},
		cli.BoolFlag{
			Name:  "no-binlog-position",
			Usage: "record no binary log position even with log_bin on: no global read lock, no RELOAD or REPLICATION CLIENT",
		},
	),
	Action: dumpAction,
}

func dumpAction(c *cli.Context) error {
	if c.GlobalString("set") == "" {
		return cli.NewExitError("--set is required", 2)
	}

	repo := common.OpenRepo(c)
	env := common.Env(c, repo, goback.Caches{})

	defer func() {
		_ = env.Close()
		common.CloseRepo(repo)
	}()

	result, err := job.MySQLDump(common.Context(c), env, job.MySQLDumpSettings{
		Set:              c.GlobalString("set"),
		Client:           jobClient(c),
		BinlogSet:        c.String("binlog-set"),
		NoBinlogPosition: c.Bool("no-binlog-position"),
	})
	if err != nil {
		return err
	}

	meta := result.Commit.Metadata

	common.Result(struct {
		views.WalkView
		BinlogFile string `json:"binlog_file,omitempty"`
		BinlogPos  string `json:"binlog_pos,omitempty"`
		Lock       string `json:"lock"`
	}{common.View.Walk(result), meta[mysql.MetaBinlogFile], meta[mysql.MetaBinlogPos], meta[mysql.MetaLock]}, func() {
		if meta[mysql.MetaBinlogFile] == "" {
			log.Printf("Commit %x: dump of %d bytes, no binlog position", result.Ref.Hash, result.Bytes)
			return
		}

		log.Printf("Commit %x: dump of %d bytes, binlogs from %s:%s", result.Ref.Hash, result.Bytes, meta[mysql.MetaBinlogFile], meta[mysql.MetaBinlogPos])
	})

	return nil
}
