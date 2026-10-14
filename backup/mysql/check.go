package mysql

import (
	"context"
	"fmt"
	"os"
)

// Check connects as the option files say, defaultsExtraFile first, and
// lists what the server lacks for receiving its binlogs as replica
// serverID; nil when it lacks nothing.
func Check(ctx context.Context, defaultsExtraFile string, serverID uint32) []string {
	opts, err := ReadOptions(defaultsExtraFile)
	if err != nil {
		return []string{"reading the option files: " + err.Error()}
	}

	conn := newServerConn(opts)
	defer conn.Close()

	srv, err := ReadServer(ctx, conn)
	if err != nil {
		return []string{err.Error()}
	}

	var problems []string

	if !srv.LogBin {
		problems = append(problems, "log_bin is off: point-in-time recovery needs log_bin in my.cnf and a restart, or pitr: false for dumps only")
	} else if srv.ServerID == 0 {
		problems = append(problems, "server_id is 0: set a server_id in my.cnf for binary logging")
	}

	if _, err := BinaryLogs(ctx, conn); err != nil && srv.LogBin {
		problems = append(problems, fmt.Sprintf("%v; GRANT REPLICATION CLIENT ON *.* to the user", err))
	}

	registered, err := replicas(ctx, conn, srv)
	if err != nil {
		return append(problems, fmt.Sprintf("listing the replicas: %v; GRANT REPLICATION SLAVE ON *.* to the user", err))
	}

	hostname, _ := os.Hostname()
	if err := checkServerID(serverID, srv, registered, hostname); err != nil {
		problems = append(problems, err.Error())
	}

	return problems
}

// CheckDump connects as the option files say, defaultsExtraFile first, and
// lists what the server lacks for dumps, which record their binlog position
// when position is set and log_bin is on; nil when it lacks nothing.
func CheckDump(ctx context.Context, defaultsExtraFile string, position bool) []string {
	opts, err := ReadOptions(defaultsExtraFile)
	if err != nil {
		return []string{"reading the option files: " + err.Error()}
	}

	conn := newServerConn(opts)
	defer conn.Close()

	srv, err := ReadServer(ctx, conn)
	if err != nil {
		return []string{err.Error()}
	}

	if !position || !srv.LogBin {
		return nil
	}

	if _, err := BinaryLogs(ctx, conn); err != nil {
		return []string{fmt.Sprintf("%v; GRANT REPLICATION CLIENT ON *.* to the user, or set no-binlog-position", err)}
	}

	return nil
}
