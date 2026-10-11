package agent

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gobackio/goback/agent"
	"github.com/gobackio/goback/backup/postgres"
	"github.com/gobackio/goback/proto"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
)

// psql runs sql in the container as postgres.
func psql(t *testing.T, c testcontainers.Container, sql string) string {
	t.Helper()

	code, r, err := c.Exec(context.Background(), []string{"psql", "-v", "ON_ERROR_STOP=1", "-tA", "-c", sql}, tcexec.WithUser("postgres"))
	require.NoError(t, err)

	var stdout, stderr bytes.Buffer
	_, err = stdcopy.StdCopy(&stdout, &stderr, r)
	require.NoError(t, err)
	require.Zero(t, code, "%s: %s", sql, stderr.String())

	return strings.TrimSpace(stdout.String())
}

func TestTheAgentBacksUpPostgresWithPointInTimeRecoveryFromAnEmptyKind(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}

	ctx := context.Background()

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:17-alpine",
			Env:          map[string]string{"POSTGRES_PASSWORD": "goback"},
			Cmd:          []string{"postgres", "-c", "fsync=off"},
			ExposedPorts: []string{"5432/tcp"},
			WaitingFor:   wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	})
	if c != nil {
		t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	}

	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatalf("no Docker: %v", err)
		}

		t.Skipf("no Docker: %v", err)
	}

	psql(t, c, "CREATE ROLE goback LOGIN PASSWORD 'goback'")
	psql(t, c, "CREATE TABLE rows (n int)")

	host, err := c.Host(ctx)
	require.NoError(t, err)

	port, err := c.MappedPort(ctx, "5432/tcp")
	require.NoError(t, err)

	t.Setenv("PGPASSWORD", "goback")

	home := t.TempDir()
	s := agentAt(t, home, fmt.Sprintf("  - {name: db, postgres: {dbname: \"host=%s port=%s user=goback sslmode=disable\", commit-every: 10s}}\n", host, port.Port()))

	problems := s.cfg.Backups[0].CheckDatabase(ctx)
	require.Len(t, problems, 3, "%q", problems)
	require.Contains(t, problems[0], "lacks REPLICATION")
	require.Contains(t, problems[1], "max_slot_wal_keep_size")
	require.Contains(t, problems[2], "pg_hba.conf")

	psql(t, c, "ALTER ROLE goback REPLICATION")
	psql(t, c, "ALTER SYSTEM SET max_slot_wal_keep_size = '1GB'")

	code, _, err := c.Exec(ctx, []string{"sh", "-c", `echo "host replication goback all scram-sha-256" >> "$PGDATA/pg_hba.conf"`}, tcexec.WithUser("postgres"))
	require.NoError(t, err)
	require.Zero(t, code)

	psql(t, c, "SELECT pg_reload_conf()")
	require.Empty(t, s.cfg.Backups[0].CheckDatabase(ctx))

	runUntil(t, s, func(status *agent.Status) bool {
		return jobNamed(status, "db-base").LastResult == agent.ResultOK && jobNamed(status, "db-wal").LastCommit != ""
	})

	psql(t, c, "INSERT INTO rows SELECT generate_series(1, 1000)")

	current, err := postgres.ParseLSN(psql(t, c, "SELECT pg_current_wal_lsn()"))
	require.NoError(t, err)

	status := runUntil(t, s, func(status *agent.Status) bool {
		details, _ := jobNamed(status, "db-wal").Details.(map[string]any)
		committed, _ := details["committed"].(string)
		lsn, err := postgres.ParseLSN(committed)

		return err == nil && lsn >= current
	})

	wal := jobNamed(status, "db-wal")
	require.Equal(t, "postgres-receive", wal.Kind)
	require.EqualValues(t, 30, wal.StaleAfter)

	files, err := os.ReadDir(filepath.Join(s.stateDir, "spool", "db-wal"))
	require.NoError(t, err, "the spool is under the state dir")
	require.NotEmpty(t, files)

	env, err := s.open(ctx)
	require.NoError(t, err)
	defer closeEnv(env)

	r := &postgres.Restore{
		Objects:        env.Index,
		Key:            env.Key,
		Latest:         func(ctx context.Context, set string) (*proto.Ref, error) { return env.Index.LatestCommit(ctx, set) },
		BaseSet:        "db-base",
		WALSet:         "db-wal",
		RestoreCommand: func(*proto.Ref) string { return "true" },
	}

	restored, err := r.Run(ctx, filepath.Join(t.TempDir(), "data"))
	require.NoError(t, err)
	require.NotNil(t, restored.WAL)

	walCommit, err := env.Index.Get(ctx, restored.WAL)
	require.NoError(t, err)

	end, err := postgres.ParseLSN(walCommit.GetCommit().GetMetadata()[postgres.MetaEndLSN])
	require.NoError(t, err)
	require.GreaterOrEqual(t, end, current, "the WAL set holds the rows written after the base")

	require.Equal(t, "goback_db_wal", psql(t, c, "SELECT slot_name FROM pg_replication_slots"))
}
