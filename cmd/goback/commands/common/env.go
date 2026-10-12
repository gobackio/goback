package common

import (
	"log/slog"
	"os"

	"github.com/gobackio/goback"
	"github.com/gobackio/goback/backup/job"

	"github.com/urfave/cli"
)

// Env is the job environment of repo with its key, or exits. It opens no
// cache, and the programs runs start write to stderr as they are.
func Env(c *cli.Context, repo *goback.Repo) *job.Env {
	return &job.Env{
		Store:   repo.Store,
		Index:   repo.Index,
		Key:     StoreKey(c, repo),
		AgentID: AgentID(c),
		Logger:  slog.Default(),
		Stderr:  os.Stderr,
	}
}
