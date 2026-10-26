package pack

import (
	"testing"

	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"
)

// LegacyFile stores content as a file of parts of size bytes, each blob
// sealed with key the way a store held it before Reseal, for the tests
// outside the package.
func LegacyFile(t *testing.T, store *PackStorage, key *storekey.Key, content []byte, size int) *proto.Ref {
	t.Helper()

	return newLegacy(t, store, key).file(content, size)
}
