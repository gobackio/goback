package agent

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestASecondAgentOnTheStateDirIsRefused(t *testing.T) {
	dir := t.TempDir()

	unlock, err := Lock(dir)
	require.NoError(t, err)

	_, err = Lock(dir)
	require.ErrorIs(t, err, ErrLocked)

	unlock()

	unlock, err = Lock(dir)
	require.NoError(t, err)
	unlock()
}
