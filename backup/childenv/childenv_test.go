package childenv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnvironLeavesOutGobacksSecrets(t *testing.T) {
	t.Setenv("GOBACK_PASSPHRASE", "correct-horse-1")
	t.Setenv("GOBACK_STORAGE", "goback://api-key-2@backup:7000")
	t.Setenv("GOBACK_INDEX", "postgres://u:db-password-3@db/goback")
	t.Setenv("GOBACK_POSTGRES_DBNAME", "host=db password=db-password-4")
	t.Setenv("GOBACK_ADMIN_KEY", "admin-key-5")
	t.Setenv("GOBACK_CACHE_DIR", "/cache")

	env := Environ()
	require.Contains(t, env, "GOBACK_CACHE_DIR=/cache")
	for _, kv := range env {
		for _, secret := range []string{"correct-horse-1", "api-key-2", "db-password-3", "db-password-4", "admin-key-5"} {
			require.NotContains(t, kv, secret)
		}
	}
}

func TestEnvironKeepsLocationsWithoutACredential(t *testing.T) {
	t.Setenv("GOBACK_STORAGE", "gs://bucket/a@b")
	t.Setenv("GOBACK_INDEX", "/var/lib/goback")
	t.Setenv("GOBACK_POSTGRES_DBNAME", "host=db user=goback")

	env := Environ()
	require.Contains(t, env, "GOBACK_STORAGE=gs://bucket/a@b")
	require.Contains(t, env, "GOBACK_INDEX=/var/lib/goback")
	require.Contains(t, env, "GOBACK_POSTGRES_DBNAME=host=db user=goback")
}
