package storekey

import (
	"bytes"
	"io/fs"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tink-crypto/tink-go/v2/keyset"
	"github.com/tink-crypto/tink-go/v2/prf"
)

func testKey(t *testing.T) *Key {
	t.Helper()

	key, err := Generate("s1")
	require.NoError(t, err)

	return key
}

func saveData(size int) []byte {
	return bytes.Repeat([]byte("game save data "), size/15+1)[:size]
}

func TestASealedBlobOpensAndIsStoredOnce(t *testing.T) {
	key := testKey(t)
	data := saveData(2000)

	sealed := key.SealBlob(data)
	require.NotContains(t, string(sealed), "game save")
	require.Equal(t, sealed, key.SealBlob(data), "the same blob seals the same way")

	opened, err := key.OpenBlob(sealed)
	require.NoError(t, err)
	require.Equal(t, data, opened)
}

func TestABlobOpensOnlyAsABlobUnderItsKey(t *testing.T) {
	key := testKey(t)
	sealed := key.SealBlob(saveData(3000))

	_, err := testKey(t).OpenBlob(sealed)
	require.ErrorIs(t, err, ErrWrongKey)

	_, err = key.OpenInline(sealed)
	require.ErrorIs(t, err, ErrWrongKey)

	_, err = key.OpenBlob(key.SealInline(saveData(3000)))
	require.ErrorIs(t, err, ErrWrongKey)

	ident, err := keyset.NewHandle(prf.HMACSHA256PRFKeyTemplate())
	require.NoError(t, err)
	twin, err := New("s1", key.Policy, key.seal, ident)
	require.NoError(t, err)
	_, err = twin.OpenBlob(sealed)
	require.ErrorIs(t, err, ErrWrongKey, "a blob is bound to the id of the key that sealed it")
}

func TestInlineContentRoundTrips(t *testing.T) {
	key := testKey(t)

	ciphertext := key.SealInline([]byte("hello"))
	require.NotContains(t, string(ciphertext), "hello")

	opened, err := key.OpenInline(ciphertext)
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), opened)
}

func TestFieldTokensBindTheParent(t *testing.T) {
	key := testKey(t)
	parent := key.SealField(nil, FieldName, []byte("sub"))

	token := key.SealField(parent, FieldName, []byte("save.dat"))
	require.Equal(t, token, key.SealField(parent, FieldName, []byte("save.dat")), "tokens are deterministic")
	require.NotEqual(t, token, key.SealField(nil, FieldName, []byte("save.dat")), "the same name elsewhere gets another token")
	require.NotEqual(t, token, key.SealField(parent, FieldUser, []byte("save.dat")), "fields do not collide")
	require.Empty(t, key.SealField(parent, FieldUser, nil), "empty fields stay empty")

	opened, err := key.OpenField(parent, FieldName, token)
	require.NoError(t, err)
	require.Equal(t, []byte("save.dat"), opened)

	_, err = key.OpenField(nil, FieldName, token)
	require.ErrorIs(t, err, ErrWrongKey)

	opened, err = key.OpenField(parent, FieldName, nil)
	require.NoError(t, err)
	require.Empty(t, opened)
}

func TestAKeyFileLoadsAsTheSameKey(t *testing.T) {
	key := testKey(t)
	key.Policy.Mode = ModeNone

	path := filepath.Join(t.TempDir(), "store.key")
	require.NoError(t, key.Save(path))
	require.ErrorIs(t, testKey(t).Save(path), fs.ErrExist, "a key is never written over another")

	loaded, err := Load(path)
	require.NoError(t, err)
	require.Equal(t, key.ID(), loaded.ID())
	require.Equal(t, "s1", loaded.Name)
	require.Equal(t, key.Policy, loaded.Policy)

	opened, err := loaded.OpenBlob(key.SealBlob(saveData(100)))
	require.NoError(t, err)
	require.Equal(t, saveData(100), opened)
}

func TestAnEscrowedKeyComesBackWhole(t *testing.T) {
	key := testKey(t)
	key.Policy.PresenceScope = "set"

	escrowed, err := key.Escrow("correct horse")
	require.NoError(t, err)

	recovered, err := Recover(escrowed, "correct horse")
	require.NoError(t, err)
	require.Equal(t, key.ID(), recovered.ID())
	require.Equal(t, key.Name, recovered.Name)
	require.Equal(t, key.Policy, recovered.Policy, "the policy is kept")

	_, err = Recover(escrowed, "wrong horse")
	require.Error(t, err)
}
