package mysql

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
	"github.com/stretchr/testify/require"
)

// recording is a connection that keeps what the client sent.
type recording struct {
	net.Conn
	mu   *sync.Mutex
	sent *bytes.Buffer
}

func (r recording) Read(p []byte) (int, error) {
	n, err := r.Conn.Read(p)

	r.mu.Lock()
	r.sent.Write(p[:n])
	r.mu.Unlock()

	return n, err
}

// serve runs srv on a loopback port for user goback with password secret,
// and returns the port and what clients sent it.
func serve(t *testing.T, srv *server.Server) (int, func() []byte) {
	t.Helper()

	var (
		mu     sync.Mutex
		sent   bytes.Buffer
		served []net.Conn
		conns  sync.WaitGroup
	)

	t.Cleanup(conns.Wait)
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()

		for _, c := range served {
			_ = c.Close()
		}
	})

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })

	conns.Add(1)

	go func() {
		defer conns.Done()

		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}

			mu.Lock()
			served = append(served, conn)
			mu.Unlock()

			conns.Go(func() {
				c, err := srv.NewConn(recording{Conn: conn, mu: &mu, sent: &sent}, "goback", "secret", server.EmptyHandler{})
				if err != nil {
					return
				}

				for c.HandleCommand() == nil {
				}
			})
		}
	}()

	return l.Addr().(*net.TCPAddr).Port, func() []byte {
		mu.Lock()
		defer mu.Unlock()

		return bytes.Clone(sent.Bytes())
	}
}

func connect(t *testing.T, opts *Options) error {
	t.Helper()

	conn := newServerConn(opts)
	t.Cleanup(conn.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := conn.connect(ctx)

	return err
}

// tlsRecord starts the TLS handshake a client sends.
var tlsRecord = []byte{0x16, 0x03}

func TestWithoutAnSSLModeTheReceiverEncryptsWhereTheServerOffersTLS(t *testing.T) {
	port, sent := serve(t, server.NewDefaultServer())

	require.NoError(t, connect(t, &Options{Host: "127.0.0.1", Port: port, User: "goback", Password: "secret", Protocol: "tcp"}))
	require.True(t, bytes.Contains(sent(), tlsRecord))
}

func TestWithoutAnSSLModeTheReceiverConnectsInTheClearToAServerWithoutTLS(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	port, sent := serve(t, server.NewServer("8.0.11", gomysql.DEFAULT_COLLATION_ID, gomysql.AUTH_NATIVE_PASSWORD, key, nil))

	opts := &Options{Host: "127.0.0.1", Port: port, User: "goback", Password: "secret", Protocol: "tcp"}
	require.NoError(t, connect(t, opts))
	require.False(t, bytes.Contains(sent(), tlsRecord))

	opts.SSLMode = "REQUIRED"
	require.Error(t, connect(t, opts))
}

func TestTheReceiverNeverSendsThePasswordInTheClear(t *testing.T) {
	port, sent := serve(t, server.NewServer("8.0.11", gomysql.DEFAULT_COLLATION_ID, gomysql.AUTH_CLEAR_PASSWORD, nil, nil))

	for _, mode := range []string{"", "DISABLED"} {
		err := connect(t, &Options{Host: "127.0.0.1", Port: port, User: "goback", Password: "secret", Protocol: "tcp", SSLMode: mode})
		require.ErrorIs(t, err, ErrClearPassword, mode)
	}

	require.False(t, bytes.Contains(sent(), []byte("secret")))
}

func TestAnSSLCAWithoutAModeVerifiesTheServer(t *testing.T) {
	port, _ := serve(t, server.NewDefaultServer())

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "another CA"}, NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	ca := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))

	opts := &Options{Host: "127.0.0.1", Port: port, User: "goback", Password: "secret", Protocol: "tcp", SSLCA: ca}
	require.Error(t, connect(t, opts))

	opts.SSLMode = "PREFERRED"
	require.NoError(t, connect(t, opts))
}
