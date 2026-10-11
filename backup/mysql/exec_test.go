package mysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExecPassesTheOptionFileFirst(t *testing.T) {
	e := &Exec{Mysql: "mysql", Mysqldump: "mysqldump", Mysqlbinlog: "mysqlbinlog", DefaultsFile: "/etc/goback/my.cnf"}
	ctx := context.Background()

	require.Equal(t, []string{"mysql", "--defaults-extra-file=/etc/goback/my.cnf", "--batch", "--skip-column-names", "-e", "SHOW BINARY LOGS"},
		e.queryCommand(ctx, "SHOW BINARY LOGS").Args)

	binlog, load := e.replayCommands(ctx, []string{"--start-position=4", "/tmp/b.000001"})
	require.Equal(t, []string{"mysqlbinlog", "--defaults-extra-file=/etc/goback/my.cnf", "--start-position=4", "/tmp/b.000001"}, binlog.Args)
	require.Contains(t, binlog.Env, "TZ=UTC")
	require.Equal(t, []string{"mysql", "--defaults-extra-file=/etc/goback/my.cnf"}, load.Args)

	require.Equal(t, []string{"mysql"}, (&Exec{Mysql: "mysql"}).loadCommand(ctx).Args, "without an option file, the client's own defaults")
}

func TestExecPrintsTheCommandsForADirectory(t *testing.T) {
	e := &Exec{Mysql: "mysql", Mysqlbinlog: "mysqlbinlog", DefaultsFile: "/root/my extra.cnf"}

	require.Equal(t, `mysql '--defaults-extra-file=/root/my extra.cnf' < /restore/dump.sql`, e.LoadCommand("/restore/dump.sql"))
	require.Equal(t, `TZ=UTC mysqlbinlog '--defaults-extra-file=/root/my extra.cnf' --start-position=4 '--stop-datetime=2026-10-02 14:30:00' /restore/b.000001 | mysql '--defaults-extra-file=/root/my extra.cnf'`,
		e.ReplayCommand([]string{"--start-position=4", "--stop-datetime=2026-10-02 14:30:00", "/restore/b.000001"}))
}

func TestParseBatchUnescapesFields(t *testing.T) {
	require.Equal(t, [][]string{{"a\tb", "c\\d"}, {"NULL", "e\nf"}}, parseBatch([]byte("a\\tb\tc\\\\d\nNULL\te\\nf\n")))
	require.Empty(t, parseBatch(nil))
}
