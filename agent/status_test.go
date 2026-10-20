package agent

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTheStatusFileIsReplacedWhileSomeoneReadsIt(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeStatus(dir, &Status{State: AgentRunning}))

	reader, err := os.Open(filepath.Join(dir, StatusFile))
	require.NoError(t, err)

	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = reader.Close()
	}()

	require.NoError(t, writeStatus(dir, &Status{State: AgentStopped}))

	got, err := ReadStatus(dir)
	require.NoError(t, err)
	require.Equal(t, AgentStopped, got.State)
}
