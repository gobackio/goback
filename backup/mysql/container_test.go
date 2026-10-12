package mysql

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// mysqlContainer is a Client that runs the client programs inside a
// container.
type mysqlContainer struct {
	t   *testing.T
	ctx context.Context
	testcontainers.Container

	// replayer runs mysqlbinlog against the server at replayHost
	replayer   *mysqlContainer
	replayHost string
}

func skipWithoutDocker(t *testing.T, err error) {
	t.Helper()

	if os.Getenv("CI") != "" {
		t.Fatalf("no Docker: %v", err)
	}

	t.Skipf("no Docker: %v", err)
}

func startMySQL(t *testing.T, net *testcontainers.DockerNetwork, image, alias string, args ...string) *mysqlContainer {
	t.Helper()

	return start(t, testcontainers.ContainerRequest{
		Image:          image,
		Env:            map[string]string{"MYSQL_ROOT_PASSWORD": "goback"},
		Cmd:            args,
		Networks:       []string{net.Name},
		NetworkAliases: map[string][]string{net.Name: {alias}},
		WaitingFor: wait.ForExec([]string{"mysql", "-uroot", "-pgoback", "-h127.0.0.1", "-e", "SELECT 1"}).
			WithStartupTimeout(3 * time.Minute).WithPollInterval(time.Second),
	})
}

func startReplayer(t *testing.T, net *testcontainers.DockerNetwork, image string) *mysqlContainer {
	t.Helper()

	return start(t, testcontainers.ContainerRequest{
		Image:      image,
		Entrypoint: []string{"sleep", "infinity"},
		Networks:   []string{net.Name},
	})
}

func start(t *testing.T, req testcontainers.ContainerRequest) *mysqlContainer {
	t.Helper()

	ctx := context.Background()

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if c != nil {
		t.Cleanup(func() {
			if t.Failed() {
				if logs, err := c.Logs(ctx); err == nil {
					out, _ := io.ReadAll(logs)
					t.Logf("%s logs:\n%s", req.Image, out)
				}
			}

			_ = c.Terminate(ctx)
		})
	}

	if err != nil {
		skipWithoutDocker(t, err)
	}

	return &mysqlContainer{t: t, ctx: ctx, Container: c}
}

func (c *mysqlContainer) exec(ctx context.Context, cmd ...string) ([]byte, error) {
	code, r, err := c.Exec(ctx, cmd)
	if err != nil {
		return nil, err
	}

	var stdout, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, r); err != nil {
		return nil, err
	}

	if code != 0 {
		return stdout.Bytes(), fmt.Errorf("%v: exit %d: %s", cmd, code, stderr.String())
	}

	return stdout.Bytes(), nil
}

// sql runs statements as root and returns their output.
func (c *mysqlContainer) sql(statements string) string {
	c.t.Helper()

	out, err := c.exec(c.ctx, "mysql", "-uroot", "-pgoback", "--batch", "--skip-column-names", "-e", statements)
	require.NoError(c.t, err)

	return strings.TrimSpace(strings.ReplaceAll(string(out), "\t", "|"))
}

// insert writes 100 rows of phase, one transaction each.
func (c *mysqlContainer) insert(phase string) {
	c.t.Helper()

	var b strings.Builder
	for n := 1; n <= 100; n++ {
		fmt.Fprintf(&b, "INSERT INTO app.rows VALUES ('%s', %d);", phase, n)
	}

	c.sql(b.String())
}

func (c *mysqlContainer) rows() string {
	c.t.Helper()

	return c.sql("SELECT phase, count(*) FROM app.rows GROUP BY phase ORDER BY phase")
}

func (c *mysqlContainer) Query(ctx context.Context, sql string) ([][]string, error) {
	out, err := c.exec(ctx, "mysql", "-uroot", "-pgoback", "--batch", "--skip-column-names", "-e", sql)
	if err != nil {
		return nil, err
	}

	return parseBatch(out), nil
}

func (c *mysqlContainer) DumpVersion(ctx context.Context) (string, error) {
	out, err := c.exec(ctx, "mysqldump", "--version")
	return string(out), err
}

func (c *mysqlContainer) Dump(ctx context.Context, args []string) (io.ReadCloser, error) {
	out, err := c.exec(ctx, append([]string{"mysqldump", "-uroot", "-pgoback"}, args...)...)
	return &fakeDump{Reader: bytes.NewReader(out), err: err}, nil
}

func (c *mysqlContainer) Load(ctx context.Context, r io.Reader) error {
	content, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	if err := c.CopyToContainer(ctx, content, "/tmp/dump.sql", 0o644); err != nil {
		return err
	}

	_, err = c.exec(ctx, "sh", "-c", "mysql -uroot -pgoback < /tmp/dump.sql")

	return err
}

func (c *mysqlContainer) BinlogVersion(ctx context.Context) (string, error) {
	out, err := c.replayer.exec(ctx, "mysqlbinlog", "--version")
	return string(out), err
}

func (c *mysqlContainer) Replay(ctx context.Context, args []string) error {
	words := []string{"TZ=UTC", "mysqlbinlog"}

	for _, arg := range args {
		if content, err := os.ReadFile(arg); err == nil {
			remote := "/tmp/" + filepath.Base(arg)
			if err := c.replayer.CopyToContainer(ctx, content, remote, 0o644); err != nil {
				return err
			}

			arg = remote
		}

		words = append(words, shellQuote(arg))
	}

	words = append(words, "|", "mysql", "-uroot", "-pgoback")
	if c.replayHost != "" {
		words = append(words, "-h", c.replayHost)
	}

	_, err := c.replayer.exec(ctx, "bash", "-o", "pipefail", "-c", strings.Join(words, " "))

	return err
}
