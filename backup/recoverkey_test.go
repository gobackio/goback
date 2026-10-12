package backup_test

import (
	"context"
	"testing"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/storekey"

	"github.com/stretchr/testify/require"
)

type keeps []backup.EscrowedKey

func (k keeps) EscrowedKeys(context.Context) ([]backup.EscrowedKey, error) { return k, nil }

func TestAPassphraseOpensTheCopyItEscrowed(t *testing.T) {
	ctx := context.Background()

	key, err := storekey.Generate("s1")
	require.NoError(t, err)

	var kept keeps
	for _, passphrase := range []string{"old passphrase", "new passphrase"} {
		data, err := key.Escrow(passphrase)
		require.NoError(t, err)
		kept = append(kept, backup.EscrowedKey{KeyID: key.IDString(), Escrowed: data})
	}

	opened, err := backup.RecoverKey(ctx, kept, "new passphrase")
	require.NoError(t, err)
	require.Equal(t, key.ID(), opened.ID())

	_, err = backup.RecoverKey(ctx, kept, "wrong passphrase")
	require.Error(t, err)

	_, err = backup.RecoverKey(ctx, keeps(nil), "new passphrase")
	require.Error(t, err)
}
