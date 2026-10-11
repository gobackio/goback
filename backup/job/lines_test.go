package job

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

type record struct {
	Msg     string `json:"msg"`
	Program string `json:"program"`
}

func capture(t *testing.T) (*slog.Logger, func() []record) {
	var buf bytes.Buffer

	return slog.New(slog.NewJSONHandler(&buf, nil)), func() []record {
		var records []record

		dec := json.NewDecoder(&buf)
		for dec.More() {
			var r record
			require.NoError(t, dec.Decode(&r))
			records = append(records, r)
		}

		return records
	}
}

func TestLinesLogsEachLineOnceComplete(t *testing.T) {
	logger, records := capture(t)
	l := &lines{logger: logger, program: "pg_basebackup"}

	_, _ = l.Write([]byte("first\nsec"))
	_, _ = l.Write([]byte("ond\r\n\n  \nthi"))
	require.Equal(t, []record{{"first", "pg_basebackup"}, {"second", "pg_basebackup"}}, records())

	_, _ = l.Write([]byte("rd"))
	l.flush()
	require.Equal(t, []record{{"third", "pg_basebackup"}}, records())
}

func TestLinesCutsALineThatNeverEnds(t *testing.T) {
	logger, records := capture(t)
	l := &lines{logger: logger, program: "hook"}

	_, _ = l.Write(bytes.Repeat([]byte("x"), maxLine+1))
	l.flush()

	got := records()
	require.Len(t, got, 2)
	require.Len(t, got[0].Msg, maxLine)
	require.Equal(t, "x", got[1].Msg)
}

func TestPathLogsHookStderrThroughTheEnvLogger(t *testing.T) {
	fail := "echo quiesce failed >&2; exit 3"
	if runtime.GOOS == "windows" {
		fail = "echo quiesce failed 1>&2 & exit /b 3"
	}

	logger, records := capture(t)

	_, err := Path(context.Background(), &Env{Logger: logger}, PathSettings{Dir: t.TempDir(), PreHook: fail})
	require.Error(t, err)

	require.Contains(t, records(), record{"quiesce failed", "hook"})
}
