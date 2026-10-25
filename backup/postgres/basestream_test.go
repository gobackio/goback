package postgres

import (
	archive "archive/tar"
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

type script []pgproto3.BackendMessage

func (s *script) ReceiveMessage(context.Context) (pgproto3.BackendMessage, error) {
	if len(*s) == 0 {
		return nil, io.ErrUnexpectedEOF
	}

	msg := (*s)[0]
	*s = (*s)[1:]

	return msg, nil
}

func fields(names ...string) *pgproto3.RowDescription {
	d := &pgproto3.RowDescription{}
	for _, name := range names {
		d.Fields = append(d.Fields, pgproto3.FieldDescription{Name: []byte(name)})
	}

	return d
}

func typed(kind byte, data string) *pgproto3.CopyData {
	return &pgproto3.CopyData{Data: append([]byte{kind}, data...)}
}

// serverTar is an archive as the server streams it: no end-of-archive
// marker.
func serverTar(t *testing.T) []byte {
	t.Helper()

	var buf bytes.Buffer

	tw := archive.NewWriter(&buf)
	for name, content := range map[string]string{"backup_label": "START WAL LOCATION: 0/7000028", "PG_VERSION": "17\n"} {
		require.NoError(t, tw.WriteHeader(&archive.Header{Name: name, Mode: 0o600, Size: int64(len(content))}))
		_, err := tw.Write([]byte(content))
		require.NoError(t, err)
	}

	require.NoError(t, tw.Flush())

	return buf.Bytes()
}

// baseResponse is how Postgres 15 and on answer BASE_BACKUP, the archive
// cut into data messages of chunk bytes.
func baseResponse(archiveData []byte, chunk int, tablespaces ...*pgproto3.DataRow) script {
	s := script{
		fields("recptr", "tli"),
		&pgproto3.DataRow{Values: [][]byte{[]byte("0/7000028"), []byte("1")}},
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT")},
		fields("spcoid", "spclocation", "size"),
	}

	for _, ts := range tablespaces {
		s = append(s, ts)
	}

	s = append(s,
		&pgproto3.DataRow{Values: [][]byte{nil, nil, nil}},
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT")},
		&pgproto3.CopyOutResponse{},
		typed('n', "base.tar\x00\x00"),
	)

	for len(archiveData) > 0 {
		n := min(chunk, len(archiveData))
		s = append(s, typed('d', string(archiveData[:n])), typed('p', "\x00\x00\x00\x00\x00\x00\x01\x00"))
		archiveData = archiveData[n:]
	}

	return append(s,
		typed('m', ""),
		typed('d', `{ "PostgreSQL-Backup-Manifest-Version": 1,`),
		typed('d', ` "Files": [] }`),
		&pgproto3.CopyDone{},
		fields("recptr", "tli"),
		&pgproto3.DataRow{Values: [][]byte{[]byte("0/7000120"), []byte("1")}},
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT")},
		&pgproto3.CommandComplete{CommandTag: []byte("BASE_BACKUP")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	)
}

func TestTheBaseBackupStreamSplitsIntoArchiveAndManifest(t *testing.T) {
	data := serverTar(t)
	s := baseResponse(data, 100)

	var got bytes.Buffer

	manifest, err := readBase(context.Background(), &s, &got)
	require.NoError(t, err)
	require.Equal(t, data, got.Bytes())
	require.Equal(t, `{ "PostgreSQL-Backup-Manifest-Version": 1, "Files": [] }`, string(manifest))
}

func TestTheBaseBackupBecomesOneTarWithItsManifest(t *testing.T) {
	manifests := make(chan []byte, 1)
	manifests <- []byte("manifest")

	var out bytes.Buffer
	require.NoError(t, retar(bytes.NewReader(serverTar(t)), &out, manifests))

	contents := map[string]string{}

	tr := archive.NewReader(&out)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}

		require.NoError(t, err)

		content, err := io.ReadAll(tr)
		require.NoError(t, err)

		contents[hdr.Name] = string(content)
	}

	require.Equal(t, map[string]string{"backup_label": "START WAL LOCATION: 0/7000028", "PG_VERSION": "17\n", "backup_manifest": "manifest"}, contents)
}

func TestABaseBackupWithAnotherTablespaceIsRefused(t *testing.T) {
	s := baseResponse(serverTar(t), 100, &pgproto3.DataRow{Values: [][]byte{[]byte("16385"), []byte("/mnt/ts"), nil}})

	_, err := readBase(context.Background(), &s, io.Discard)
	require.ErrorContains(t, err, "/mnt/ts")
}

func TestABaseBackupErrorEndsTheStream(t *testing.T) {
	s := script{
		fields("recptr", "tli"),
		&pgproto3.ErrorResponse{Severity: "ERROR", Code: "55000", Message: "WAL level not sufficient"},
	}

	_, err := readBase(context.Background(), &s, io.Discard)
	require.ErrorContains(t, err, "WAL level not sufficient")
}

// endlessBase streams one tar entry that never ends, and tells whether it
// was closed while a message was being received.
type endlessBase struct {
	header    script
	receiving atomic.Bool
	overlap   atomic.Bool
}

func (e *endlessBase) ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error) {
	e.receiving.Store(true)
	defer e.receiving.Store(false)

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if len(e.header) > 0 {
		return e.header.ReceiveMessage(ctx)
	}

	return typed('d', string(make([]byte, 4096))), nil
}

func (e *endlessBase) Close(context.Context) error {
	e.overlap.Store(e.receiving.Load())
	return nil
}

func TestClosingABaseStreamReadPartlyEndsItsReaders(t *testing.T) {
	var hdr bytes.Buffer
	require.NoError(t, archive.NewWriter(&hdr).WriteHeader(&archive.Header{Name: "base/1/1", Mode: 0o600, Size: 1 << 40}))

	src := &endlessBase{header: baseResponse(nil, 1)[:8]}
	src.header = append(src.header, typed('d', hdr.String()))

	stream := streamBase(context.Background(), src)

	_, err := io.ReadFull(stream.Tar, make([]byte, 100_000))
	require.NoError(t, err)

	stream.Close()
	require.False(t, src.overlap.Load())

	ended := make(chan error, 1)
	go func() { ended <- stream.Wait() }()

	select {
	case err := <-ended:
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the base backup's reader still runs")
	}
}

func TestServerVersionsReadAsTheirMajor(t *testing.T) {
	require.Equal(t, 17, serverMajor("17.2 (Debian 17.2-1.pgdg120+1)"))
	require.Equal(t, 14, serverMajor("14.13"))
	require.Equal(t, 18, serverMajor("18beta1"))
}
