package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// runJSON invokes the binary with --json and returns stdout and stderr
// apart, with the exit code.
func (c *tool) runJSON(args ...string) (string, string, int) {
	c.t.Helper()

	all := append([]string{"--json", "--storage", "store", "--index", "index", "--set", "world", "--agent-id", "node-1"}, args...)
	cmd := exec.Command(c.bin, all...)
	cmd.Dir = c.home

	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr

	err := cmd.Run()

	code := 0
	if exit, ok := err.(*exec.ExitError); ok {
		code = exit.ExitCode()
	} else {
		require.NoError(c.t, err)
	}

	return stdout.String(), stderr.String(), code
}

func TestJSONModeKeepsResultsOnStdoutAndLogsAsLinesOnStderr(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}

	home := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(home, "store"), 0o755))
	c := &tool{t: t, bin: buildCLI(t), home: home}

	src := filepath.Join(home, "world")
	write(t, filepath.Join(src, "server.properties"), []byte("motd=hello\n"))

	out, logs, code := c.runJSON("commit", "new", src)
	require.Zero(t, code, logs)

	var walk struct {
		Commit string `json:"commit"`
		Files  int    `json:"files"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &walk), out)
	require.NotEmpty(t, walk.Commit)
	require.Equal(t, 1, walk.Files)

	lines := bufio.NewScanner(bytes.NewBufferString(logs))
	for lines.Scan() {
		var line map[string]any
		require.NoError(t, json.Unmarshal(lines.Bytes(), &line), "every log line is JSON: %s", lines.Text())
	}

	out, _, code = c.runJSON("commit", "list")
	require.Zero(t, code)

	var commits []struct {
		Ref string `json:"ref"`
		Set string `json:"set"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &commits), out)
	require.Len(t, commits, 1)
	require.Equal(t, walk.Commit, commits[0].Ref, "a listed commit carries the ref a run reported")
	require.Equal(t, "world", commits[0].Set)

	write(t, filepath.Join(src, "server.properties"), []byte("motd=again\n"))
	_, logs, code = c.runJSON("commit", "new", src)
	require.Zero(t, code, logs)

	_, logs, code = c.runJSON("commit", "delete", walk.Commit)
	require.Zero(t, code, logs)

	out, logs, code = c.runJSON("commit", "list", "--deleted")
	require.Zero(t, code, logs)

	var trashed []struct {
		Ref     string    `json:"ref"`
		Deleted time.Time `json:"deleted"`
		Expires time.Time `json:"expires"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &trashed), out)
	require.Len(t, trashed, 1)
	require.Equal(t, walk.Commit, trashed[0].Ref)
	require.True(t, trashed[0].Expires.After(trashed[0].Deleted), "a deleted commit shows when it expires")

	out, _, code = c.runJSON("pin", "remove", "not-hex")
	require.Equal(t, 1, code)

	var failure struct {
		Error string `json:"error"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &failure), out)
	require.NotEmpty(t, failure.Error)

	out, _, code = c.runJSON("postgres", "receive")
	require.Equal(t, 2, code, "a usage error keeps its own exit code")
	require.NoError(t, json.Unmarshal([]byte(out), &failure), out)
}

func TestJSONErrorNamesTheCodeOfAStoreServerRefusingTheKey(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}

	home := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(home, "store"), 0o755))
	bin := buildCLI(t)

	server := exec.Command(bin, "--storage", "store", "--index", "index", "server",
		"--address", "127.0.0.1:0", "--secret", "right", "--plaintext-behind-proxy")
	server.Dir = home

	logs, err := server.StderrPipe()
	require.NoError(t, err)
	require.NoError(t, server.Start())
	t.Cleanup(func() { _ = server.Process.Kill(); _ = server.Wait() })

	var addr string

	lines := bufio.NewScanner(logs)
	for addr == "" && lines.Scan() {
		if _, after, ok := strings.Cut(lines.Text(), "listening address="); ok {
			addr = after
		}
	}

	require.NotEmpty(t, addr, "the server never listened")

	go func() {
		for lines.Scan() {
		}
	}()

	list := exec.Command(bin, "--json", "--storage", "goback+insecure://wrong@"+addr, "--set", "world", "--agent-id", "node-1", "commit", "list")
	list.Dir = home

	out, err := list.Output()
	require.Error(t, err)

	var failure struct {
		Error string `json:"error"`
		Code  string `json:"code"`
	}
	require.NoError(t, json.Unmarshal(out, &failure), string(out))
	require.NotEmpty(t, failure.Error)
	require.Equal(t, "Unauthenticated", failure.Code)
}
