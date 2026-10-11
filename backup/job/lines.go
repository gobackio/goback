package job

import (
	"bytes"
	"log/slog"
	"sync"
	"unicode"
)

// maxLine is the longest line lines holds back; a longer one is logged in
// pieces.
const maxLine = 64 << 10

// lines logs each line written to it as a record of its own.
type lines struct {
	mu      sync.Mutex
	logger  *slog.Logger
	program string
	pending []byte
}

func (l *lines) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.pending = append(l.pending, p...)

	rest := l.pending
	for {
		line, after, found := bytes.Cut(rest, []byte{'\n'})
		if !found {
			break
		}

		l.log(line)
		rest = after
	}

	for len(rest) >= maxLine {
		l.log(rest[:maxLine])
		rest = rest[maxLine:]
	}

	l.pending = append(l.pending[:0], rest...)

	return len(p), nil
}

func (l *lines) flush() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.log(l.pending)
	l.pending = l.pending[:0]
}

func (l *lines) log(line []byte) {
	line = bytes.TrimRightFunc(line, unicode.IsSpace)
	if len(line) == 0 {
		return
	}

	l.logger.Info(string(line), "program", l.program)
}
