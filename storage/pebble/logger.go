package pebble

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// slogLogger is a pebble.Logger that writes through the default slog
// logger, pebble's informational lines at debug level.
type slogLogger struct{}

func (slogLogger) Infof(f string, v ...any)  { log(slog.LevelDebug, f, v) }
func (slogLogger) Errorf(f string, v ...any) { log(slog.LevelError, f, v) }

func (slogLogger) Fatalf(f string, v ...any) {
	log(slog.LevelError, f, v)
	pebble.DefaultLogger.Fatalf(f, v...)
}

func log(level slog.Level, format string, v []any) {
	ctx := context.Background()
	handler := slog.Default().Handler()

	if !handler.Enabled(ctx, level) {
		return
	}

	// the source is pebble's caller of the logger, not this adapter
	var pcs [1]uintptr
	runtime.Callers(4, pcs[:])

	record := slog.NewRecord(time.Now(), level, strings.TrimSpace(fmt.Sprintf(format, v...)), pcs[0])
	record.AddAttrs(slog.String("component", "pebble"))
	_ = handler.Handle(ctx, record)
}
