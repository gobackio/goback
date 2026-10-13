package postgres

import (
	"context"
	"errors"
	"net"

	"github.com/jackc/pgx/v5/pgconn"
)

// ErrServerShutDown is returned by Receiver.Run when the server shut down
// and the WAL it streamed up to its shutdown is committed. Run again once
// the server is back, streaming goes on where it ended.
var ErrServerShutDown = errors.New("the server shut down")

const (
	adminShutdown = "57P01"
	cannotConnect = "57P03"
)

// watchShutdown closes the channel it returns once the server starts
// shutting down, which it tells by an idle connection it holds: the server
// ends every backend with admin_shutdown, and then refuses connections. A
// backend ended alone, by pg_terminate_backend, is followed by a new
// connection. stop ends the watch.
func watchShutdown(ctx context.Context, connString string) (down <-chan struct{}, stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	shut := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)

		cfg, err := connConfig(connString, false, "goback")
		if err != nil {
			return
		}

		ended := false

		for {
			conn, err := pgconn.ConnectConfig(ctx, cfg)
			if err != nil {
				if ended && ctx.Err() == nil && refusedForShutdown(err) {
					close(shut)
				}

				return
			}

			err = conn.WaitForNotification(ctx)
			_ = conn.Close(context.Background())

			if !hasCode(err, adminShutdown) {
				return
			}

			ended = true
		}
	}()

	return shut, func() {
		cancel()
		<-done
	}
}

// refusedForShutdown reports whether a connection failed because the
// server is shutting down or already gone.
func refusedForShutdown(err error) bool {
	var op *net.OpError

	return hasCode(err, cannotConnect) || errors.As(err, &op)
}

func hasCode(err error, code string) bool {
	var pgErr *pgconn.PgError

	return errors.As(err, &pgErr) && pgErr.Code == code
}
