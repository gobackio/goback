package statcache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestASecondOpenOfACacheFailsAsLocked(t *testing.T) {
	dir := t.TempDir()

	first, err := Open(dir)
	require.NoError(t, err)
	defer first.Close()

	_, err = Open(dir)
	require.ErrorIs(t, err, ErrLocked)
}
