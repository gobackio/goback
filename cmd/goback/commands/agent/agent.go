// Package agent holds the goback agent command, which runs a machine's
// backups on their schedules from one config file.
package agent

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/gobackio/goback"
	"github.com/gobackio/goback/agent"
	"github.com/gobackio/goback/auth"
	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/telemetry"

	"github.com/urfave/cli"
)

var configFlag = cli.StringFlag{
	Name:   "config",
	Usage:  "the agent's YAML config file",
	EnvVar: "GOBACK_CONFIG",
}

// Command is the agent command.
var Command = cli.Command{
	Name:  "agent",
	Usage: "run a machine's backups on their schedules from one config file",
	Description: "The config lists the store once and each backup with its kind and schedule. Store settings come from\n" +
		"the file; a global flag or its variable overrides one.",
	Subcommands: []cli.Command{
		{
			Name:   "run",
			Usage:  "run the backups until SIGTERM; SIGHUP reloads the backups",
			Flags:  []cli.Flag{configFlag},
			Action: runAction,
		},
		{
			Name:   "check",
			Usage:  "validate the config, open the store, print each job's last run and next due time, and list what each database lacks",
			Flags:  []cli.Flag{configFlag},
			Action: checkAction,
		},
		{
			Name:  "status",
			Usage: "print the status file a running agent writes, without touching the store",
			Flags: []cli.Flag{
				configFlag,
				cli.BoolFlag{
					Name:  "check",
					Usage: "exit 1 when the agent is hung or a job is stale, 2 when there is no status to read",
				},
			},
			Action: statusAction,
		},
	},
}

func loadFlags(c *cli.Context) (*setup, error) {
	s, err := load(c.String("config"), c)
	if err != nil {
		return nil, cli.NewExitError(err.Error(), 2)
	}

	return s, nil
}

func runAction(c *cli.Context) error {
	s, err := loadFlags(c)
	if err != nil {
		return err
	}

	unlock, err := agent.Lock(s.stateDir)
	if err != nil {
		return cli.NewExitError(err.Error(), 1)
	}
	defer unlock()

	logger := slog.Default()

	stopTelemetry, err := telemetry.Setup(context.Background(), "goback-agent")
	if err != nil {
		logger.Warn("setting up telemetry failed", "error", err)
	} else {
		defer func() { _ = stopTelemetry(context.Background()) }()
	}

	for _, o := range s.overrides {
		logger.Info("a flag or variable overrides the config", "key", o.Key, "from", o.Source)
	}

	a := s.agent(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signals := make(chan os.Signal, 4)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(signals)

	go func() {
		stopping := false

		for sig := range signals {
			switch {
			case sig == syscall.SIGHUP:
				s.reloadInto(a, logger)
			case !stopping:
				stopping = true
				cancel()
			default:
				a.Abort()
			}
		}
	}()

	if err := a.Run(ctx, s.cfg.Tasks()); err != nil {
		return cli.NewExitError(err.Error(), 1)
	}

	return nil
}

// agent is the agent that runs s's backups.
func (s *setup) agent(logger *slog.Logger) *agent.Agent {
	return &agent.Agent{
		ID:          s.settings.AgentID,
		Settings:    s.settings,
		Caches:      s.store.Caches(),
		Concurrency: s.cfg.Schedule.Concurrency,
		Grace:       s.cfg.Schedule.ShutdownGrace,
		StateDir:    s.stateDir,
		Logger:      logger,
	}
}

// reloadInto reads the config again and hands a its backups, logging an
// invalid file and the sections that need a restart.
func (s *setup) reloadInto(a *agent.Agent, logger *slog.Logger) {
	tasks, restart, err := s.reload()
	if err != nil {
		logger.Error("the reloaded config is invalid; keeping the old one", "error", err)
		return
	}

	for _, section := range restart {
		logger.Warn("the reloaded config changes a section that takes a restart; keeping the old values", "section", section)
	}

	a.Reload(tasks)
}

func checkAction(c *cli.Context) error {
	s, err := loadFlags(c)
	if err != nil {
		return err
	}

	ctx := auth.WithPrincipal(context.Background(), &auth.Principal{AgentID: s.settings.AgentID})

	repo, err := goback.Open(ctx, s.settings)
	if err != nil {
		return cli.NewExitError(err.Error(), 2)
	}
	defer repo.Close()

	if _, err := repo.Key(ctx); err != nil {
		return cli.NewExitError(err.Error(), 2)
	}

	checked, err := s.cfg.Check(ctx, repo.Index, s.settings.AgentID, time.Now())

	common.Result(struct {
		Overrides []override `json:"overrides"`
		agent.Checked
	}{s.overrides, checked}, func() {
		for _, o := range s.overrides {
			fmt.Printf("%s from %s\n", o.Key, o.Source)
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "JOB\tKIND\tPLAN\tLAST RUN\tNEXT DUE")

		for _, j := range checked.Jobs {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", j.Name, j.Kind, j.Plan, when(j.LastRun, "never"), when(j.NextDue, ""))
		}

		_ = w.Flush()
	})

	if err != nil {
		return cli.NewExitError(err.Error(), 2)
	}

	return nil
}

func when(t time.Time, zero string) string {
	if t.IsZero() {
		return zero
	}

	return t.Local().Format(time.DateTime)
}

func statusAction(c *cli.Context) error {
	s, err := loadFlags(c)
	if err != nil {
		return err
	}

	status, err := agent.ReadStatus(s.stateDir)
	if err != nil {
		return cli.NewExitError(err.Error(), 2)
	}

	if c.Bool("check") {
		problems := status.Problems(time.Now())

		common.Result(struct {
			State    string   `json:"state"`
			Healthy  bool     `json:"healthy"`
			Problems []string `json:"problems"`
		}{status.State, len(problems) == 0, problems}, func() {
			for _, p := range problems {
				fmt.Println(p)
			}

			if len(problems) == 0 {
				fmt.Println(status.State)
			}
		})

		if len(problems) > 0 {
			return cli.NewExitError("unhealthy", 1)
		}

		return nil
	}

	common.Result(status, func() {
		fmt.Printf("pid %d, %s since %s, updated %s\n", status.PID, status.State, when(status.StartedAt, ""), when(status.UpdatedAt, ""))

		w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "JOB\tKIND\tSTATE\tLAST START\tRESULT\tLAST SUCCESS\tFAILURES\tNEXT DUE")

		for _, j := range status.Jobs {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\n", j.Name, j.Kind, j.State, when(j.LastStart, "-"), j.LastResult,
				when(j.LastSuccess, "-"), j.Failures, when(j.NextDue, "-"))
		}

		_ = w.Flush()

		for _, j := range status.Jobs {
			if j.LastError != "" {
				fmt.Printf("%s: %s\n", j.Name, j.LastError)
			}
		}
	})

	return nil
}
