package common

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAPassphraseComesFromItsFileWhenNotGiven(t *testing.T) {
	path := filepath.Join(t.TempDir(), "passphrase")
	require.NoError(t, os.WriteFile(path, []byte("  from file\n"), 0o600))

	p, err := Passphrase("", path)
	require.NoError(t, err)
	require.Equal(t, "from file", p)

	p, err = Passphrase("given", path)
	require.NoError(t, err)
	require.Equal(t, "given", p)

	_, err = Passphrase("", filepath.Join(t.TempDir(), "missing"))
	require.Error(t, err)
}
