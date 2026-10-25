package postgres

import (
	archive "archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// ErrNoBaseStream is returned by StartBase for a server before Postgres
// 15, whose base backup stream it doesn't read; pg_basebackup takes those.
var ErrNoBaseStream = errors.New("base backups of Postgres before 15 are taken with pg_basebackup")

// BaseStream is a base backup a server streams over a replication
// connection.
type BaseStream struct {
	// Tar is the backup as pg_basebackup -D - -Ft -X fetch writes it: the
	// data directory with the WAL it needs, then backup_manifest.
	Tar io.Reader
	// Wait reports how the backup ended, once Tar is read to its end.
	Wait func() error

	tar     *io.PipeReader
	cancel  context.CancelFunc
	running *sync.WaitGroup
	conn    interface{ Close(context.Context) error }
}

// Close ends the connection, and with it a backup not read to its end.
func (b *BaseStream) Close() {
	b.tar.CloseWithError(errors.New("the base backup stream was closed"))
	b.cancel()
	b.running.Wait()
	_ = b.conn.Close(context.Background())
}

// StartBase connects to the cluster at connString, as the receiver does,
// and starts a base backup with a fast checkpoint and the WAL it needs.
// Only the data directory's tablespace is taken; a cluster with others is
// refused.
func StartBase(ctx context.Context, connString string) (*BaseStream, error) {
	conn, err := connectReplication(ctx, connString, "goback")
	if err != nil {
		return nil, err
	}

	if major := serverMajor(conn.ParameterStatus("server_version")); major < minInProcessBaseVersion/10000 {
		_ = conn.Close(context.Background())
		return nil, fmt.Errorf("%w: the server is %d", ErrNoBaseStream, major)
	}

	conn.Frontend().Send(&pgproto3.Query{String: "BASE_BACKUP (LABEL 'goback', CHECKPOINT 'fast', WAL, WAIT false, MANIFEST 'yes')"})
	if err := conn.Frontend().Flush(); err != nil {
		_ = conn.Close(context.Background())
		return nil, fmt.Errorf("sending BASE_BACKUP: %w", err)
	}

	return streamBase(ctx, conn), nil
}

// streamBase reads the response to BASE_BACKUP from conn into a stream.
func streamBase(ctx context.Context, conn interface {
	messages
	Close(context.Context) error
}) *BaseStream {
	ctx, cancel := context.WithCancel(ctx)

	archiveR, archiveW := io.Pipe()
	tarR, tarW := io.Pipe()

	manifests := make(chan []byte, 1)
	ended := make(chan error, 1)

	running := new(sync.WaitGroup)
	running.Add(2)

	go func() {
		defer running.Done()

		manifest, err := readBase(ctx, conn, archiveW)
		archiveW.CloseWithError(err)
		manifests <- manifest
		ended <- err
	}()

	go func() {
		defer running.Done()

		err := retar(archiveR, tarW, manifests)
		archiveR.CloseWithError(err)
		tarW.CloseWithError(err)
	}()

	var result error

	waited := false

	return &BaseStream{
		Tar: tarR,
		Wait: func() error {
			if !waited {
				result, waited = <-ended, true
			}

			return result
		},
		tar:     tarR,
		cancel:  cancel,
		running: running,
		conn:    conn,
	}
}

// serverMajor reads the major version out of server_version: 17 of
// "17.2 (Debian 17.2-1)".
func serverMajor(version string) int {
	end := strings.IndexFunc(version, func(c rune) bool { return c < '0' || c > '9' })
	if end < 0 {
		end = len(version)
	}

	n, _ := strconv.Atoi(version[:end])

	return n
}

// messages is where BASE_BACKUP's response comes from.
type messages interface {
	ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error)
}

// readBase reads the response to BASE_BACKUP from Postgres 15 on: the
// start position, the tablespaces, then one COPY stream of typed messages
// (n a new archive, d its data, p progress, m the manifest, whose data
// follows), then the end position. It writes the archive to archive and
// returns the manifest.
func readBase(ctx context.Context, src messages, archive io.Writer) ([]byte, error) {
	var (
		manifest   bytes.Buffer
		columns    int
		archives   int
		inManifest bool
		copied     bool
	)

	for {
		msg, err := src.ReceiveMessage(ctx)
		if err != nil {
			return nil, err
		}

		switch msg := msg.(type) {
		case *pgproto3.RowDescription:
			columns = len(msg.Fields)
		case *pgproto3.DataRow:
			if columns == 3 && len(msg.Values) == 3 && msg.Values[0] != nil {
				return nil, fmt.Errorf("the cluster has a tablespace outside the data directory, at %s, which base backups don't take", msg.Values[1])
			}
		case *pgproto3.CopyData:
			if len(msg.Data) == 0 {
				return nil, errors.New("an empty message in the base backup")
			}

			switch msg.Data[0] {
			case 'n':
				if archives++; archives > 1 {
					return nil, errors.New("the base backup holds more than one archive")
				}
			case 'd':
				if inManifest {
					manifest.Write(msg.Data[1:])
				} else if _, err := archive.Write(msg.Data[1:]); err != nil {
					return nil, err
				}
			case 'm':
				inManifest = true
			case 'p':
			default:
				return nil, fmt.Errorf("an unknown message %q in the base backup", msg.Data[0])
			}
		case *pgproto3.CopyDone:
			copied = true
		case *pgproto3.ErrorResponse:
			return nil, fmt.Errorf("BASE_BACKUP: %w", pgconn.ErrorResponseToPgError(msg))
		case *pgproto3.ReadyForQuery:
			if !copied {
				return nil, errors.New("the base backup ended before its data")
			}

			return manifest.Bytes(), nil
		}
	}
}

// retar copies the tar archive in to out, then adds the manifest the
// archive's stream ends with as backup_manifest, as pg_basebackup does.
// The server's archive may lack the end-of-archive marker.
func retar(in io.Reader, out io.Writer, manifests <-chan []byte) error {
	tr := archive.NewReader(in)
	tw := archive.NewWriter(out)

	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return fmt.Errorf("reading the base backup: %w", err)
		}

		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}

		if _, err := io.Copy(tw, tr); err != nil {
			return fmt.Errorf("reading the base backup: %w", err)
		}
	}

	if _, err := io.Copy(io.Discard, in); err != nil {
		return err
	}

	manifest := <-manifests
	if len(manifest) > 0 {
		hdr := &archive.Header{Name: "backup_manifest", Mode: 0o600, Size: int64(len(manifest)), ModTime: time.Now(), Format: archive.FormatPAX}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}

		if _, err := tw.Write(manifest); err != nil {
			return err
		}
	}

	return tw.Close()
}
