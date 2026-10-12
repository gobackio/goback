package common

import (
	"os"

	"github.com/gobackio/goback"
	"github.com/gobackio/goback/backup/job"

	"github.com/urfave/cli"
)

// Env is the job environment of repo with the caches c names, or exits.
// The programs runs start write to stderr as they are. Close it before
// the repo.
func Env(c *cli.Context, repo *goback.Repo, caches goback.Caches) *job.Env {
	env, err := repo.Env(Context(c), caches)
	if err != nil {
		Fatal(err)
	}

	env.Stderr = os.Stderr

	return env
}
