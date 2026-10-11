package common

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gobackio/goback/auth"

	"github.com/urfave/cli"
)

// Context returns the command's context: cancelled by the first SIGINT or
// SIGTERM, with a second signal ending the process as usual, and carrying
// the agent as the principal.
func Context(c *cli.Context) context.Context {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	go func() {
		<-ctx.Done()
		stop()
	}()

	return auth.WithPrincipal(ctx, &auth.Principal{AgentID: AgentID(c)})
}

// AgentID is the global --agent-id flag, or the hostname.
func AgentID(c *cli.Context) string {
	if agent := c.GlobalString("agent-id"); agent != "" {
		return agent
	}

	host, _ := os.Hostname()

	return host
}

// Graceful returns a context for a command that runs until SIGTERM: stop
// closes on the first SIGINT or SIGTERM, and ctx ends on the second or once
// grace has passed since the first. ctx carries the agent as the principal.
func Graceful(c *cli.Context, grace time.Duration) (context.Context, <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	stop := make(chan struct{})

	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-signals
		close(stop)

		select {
		case <-signals:
		case <-time.After(grace):
		}

		cancel()
		signal.Stop(signals)
	}()

	return auth.WithPrincipal(ctx, &auth.Principal{AgentID: AgentID(c)}), stop
}
