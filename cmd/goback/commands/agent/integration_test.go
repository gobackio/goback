package agent

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gobackio/goback"
	"github.com/gobackio/goback/agent"
	"github.com/gobackio/goback/auth"
	"github.com/gobackio/goback/backup/job"

	"github.com/stretchr/testify/require"
)

// agentAt is the setup of a config naming a directory store under home.
func agentAt(t *testing.T, home, backups string) *setup {
	t.Helper()

	for _, dir := range []string{"store", "index"} {
		require.NoError(t, os.MkdirAll(filepath.Join(home, dir), 0o700))
	}

	path := writeConfig(t, home, `
store:
  storage: `+filepath.ToSlash(filepath.Join(home, "store"))+`
  index: `+filepath.ToSlash(filepath.Join(home, "index"))+`
  cache-dir: `+filepath.ToSlash(filepath.Join(home, "cache"))+`
  agent-id: node-1
backups:
`+backups)

	s, err := load(path, cliDefaults(nil))
	require.NoError(t, err)

	return s
}

// runUntil runs the agent until done reports true of its status, then stops
// it and returns the status it leaves.
func runUntil(t *testing.T, s *setup, done func(*agent.Status) bool) *agent.Status {
	t.Helper()

	unlock, err := agent.Lock(s.stateDir)
	require.NoError(t, err)
	defer unlock()

	a := s.agent(slog.New(slog.DiscardHandler))

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error)

	go func() { stopped <- a.Run(ctx, s.cfg.Tasks()) }()

	require.Eventually(t, func() bool {
		status, err := agent.ReadStatus(s.stateDir)
		return err == nil && done(status)
	}, time.Minute, 50*time.Millisecond)

	cancel()
	require.NoError(t, <-stopped)

	status, err := agent.ReadStatus(s.stateDir)
	require.NoError(t, err)

	return status
}

func jobNamed(status *agent.Status, name string) agent.JobStatus {
	for _, j := range status.Jobs {
		if j.Name == name {
			return j
		}
	}

	return agent.JobStatus{}
}

// commits counts the complete commits of set, with the newest one's scan
// start.
func commits(t *testing.T, s *setup, set string) (int, time.Time) {
	t.Helper()

	repo, err := goback.Open(context.Background(), s.settings)
	require.NoError(t, err)
	defer repo.Close()

	infos, err := repo.Index.CommitInfo(context.Background(), set, time.Now().Add(time.Hour), 100)
	require.NoError(t, err)

	if len(infos) == 0 {
		return 0, time.Time{}
	}

	return len(infos), time.Unix(0, infos[0].GetScanStartNs())
}

func TestTheAgentPlansFromTheStoreAcrossRestarts(t *testing.T) {
	home := t.TempDir()

	for _, dir := range []string{"one", "two"} {
		require.NoError(t, os.MkdirAll(filepath.Join(home, dir), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(home, dir, "data.txt"), []byte(dir), 0o644))
	}

	backups := func(firstEvery string) string {
		return "  - {name: one, every: " + firstEvery + ", path: {dir: " + filepath.ToSlash(filepath.Join(home, "one")) + "}}\n" +
			"  - {name: two, every: 1h, path: {dir: " + filepath.ToSlash(filepath.Join(home, "two")) + "}}\n"
	}

	s := agentAt(t, home, backups("1h"))

	repo, err := goback.Open(context.Background(), s.settings)
	require.NoError(t, err)

	env, err := repo.Env(context.Background(), s.store.Caches(s.cfg.Backups))
	require.NoError(t, err)

	ctx := auth.WithPrincipal(context.Background(), &auth.Principal{AgentID: "node-1"})
	_, err = job.Path(ctx, env, job.PathSettings{Set: "one", Dir: filepath.Join(home, "one"), Workers: 1, ProgressInterval: -1})
	require.NoError(t, err)
	require.NoError(t, env.Close())
	require.NoError(t, repo.Close())

	_, byHand := commits(t, s, "one")

	status := runUntil(t, s, func(status *agent.Status) bool {
		return jobNamed(status, "two").LastResult == agent.ResultOK
	})

	n, _ := commits(t, s, "one")
	require.Equal(t, 1, n)

	n, _ = commits(t, s, "two")
	require.Equal(t, 1, n)

	require.True(t, byHand.Add(time.Hour).Equal(jobNamed(status, "one").NextDue), "%v vs %v", byHand, jobNamed(status, "one").NextDue)

	require.NoError(t, os.WriteFile(filepath.Join(home, "one", "data.txt"), []byte("one, changed"), 0o644))
	s = agentAt(t, home, backups("1s"))

	status = runUntil(t, s, func(status *agent.Status) bool {
		return jobNamed(status, "one").LastResult == agent.ResultOK
	})

	n, _ = commits(t, s, "one")
	require.GreaterOrEqual(t, n, 2)

	n, _ = commits(t, s, "two")
	require.Equal(t, 1, n)

	require.Equal(t, agent.AgentStopped, status.State)
	require.Contains(t, []string{agent.ResultOK, agent.ResultCancelled}, jobNamed(status, "one").LastResult)
}
