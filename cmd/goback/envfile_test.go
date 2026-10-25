package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTheCLIReadsAnEnvFileOnlyWhenOneIsNamed(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}

	home := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(home, "store"), 0o755))
	write(t, filepath.Join(home, "world", "a.txt"), []byte("a"))
	write(t, filepath.Join(home, ".env"), []byte("GOBACK_STORAGE=store\nGOBACK_INDEX=index\n"))
	bin := buildCLI(t)

	var base []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "GOBACK_") {
			base = append(base, kv)
		}
	}

	run := func(env []string, args ...string) error {
		cmd := exec.Command(bin, append(args, "--set", "world", "commit", "new", "world")...)
		cmd.Dir = home
		cmd.Env = append(append([]string(nil), base...), env...)
		out, err := cmd.CombinedOutput()
		t.Logf("%s", out)

		return err
	}

	require.Error(t, run(nil), "a .env in the working directory is not read")
	require.NoError(t, run(nil, "--env-file", ".env"))
	require.NoError(t, run([]string{"GOBACK_ENV_FILE=.env"}))
	require.Error(t, run(nil, "--env-file", "missing.env"))
}
