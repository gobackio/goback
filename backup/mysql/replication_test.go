package mysql

import (
	"context"
	"net"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
	"github.com/stretchr/testify/require"
)

// stalling answers every query once released.
type stalling struct {
	server.EmptyHandler
	release chan struct{}
}

func (h stalling) HandleQuery(string) (*gomysql.Result, error) {
	<-h.release
	return nil, nil
}

func TestAQueryEndsWithItsContextWhileTheServerStalls(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })

	h := stalling{release: make(chan struct{})}

	srv := server.NewDefaultServer()

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}

			go func() {
				c, err := srv.NewConn(conn, "goback", "secret", h)
				if err != nil {
					return
				}

				for c.HandleCommand() == nil {
				}
			}()
		}
	}()

	port := l.Addr().(*net.TCPAddr).Port
	conn := newServerConn(&Options{Host: "127.0.0.1", Port: port, User: "goback", Password: "secret", Protocol: "tcp"})
	t.Cleanup(conn.Close)
	t.Cleanup(func() { close(h.release) })

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	ended := make(chan error, 1)
	go func() {
		_, err := conn.Query(ctx, "SHOW BINARY LOGS")
		ended <- err
	}()

	select {
	case err := <-ended:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(10 * time.Second):
		t.Fatal("the query outlived its context")
	}
}
