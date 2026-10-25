package backup

import (
	"bytes"
	"testing"

	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

func TestASealedBlobIsCompressedAndNamedByItsKey(t *testing.T) {
	key, err := storekey.Generate("s1")
	require.NoError(t, err)
	data := bytes.Repeat([]byte("game save data "), 2000)

	sealed := SealBlob(key, data)
	require.Equal(t, key.ID(), sealed.KeyId)
	require.True(t, BlobRef(key, data).Equal(sealed.Ref()))
	require.True(t, sealed.Ref().Equal(SealBlob(key, data).Ref()), "a chunk seals to one ref under one key")
	require.Less(t, len(sealed.Data), len(data), "compressible data is compressed before sealing")

	opened, err := OpenBlob(key, sealed)
	require.NoError(t, err)
	require.Equal(t, data, opened)
}

func TestASealedBlobDoesNotOpenUnderAnotherKey(t *testing.T) {
	key, err := storekey.Generate("s1")
	require.NoError(t, err)
	sealed := SealBlob(key, []byte("one"))

	other, err := storekey.Generate("s1")
	require.NoError(t, err)
	_, err = OpenBlob(other, sealed)
	require.ErrorIs(t, err, storekey.ErrWrongKey)
	require.False(t, BlobRef(other, []byte("one")).Equal(sealed.Ref()), "refs are private to their key")
}

func TestASealedObjectReferencesNothing(t *testing.T) {
	for _, kind := range []proto.ObjectType{proto.ObjectType_COMMIT, proto.ObjectType_TREE, proto.ObjectType_FILE, proto.ObjectType_PIN} {
		sealed := proto.NewObject(&proto.Sealed{Type: kind, Data: []byte("junk"), Encryption: proto.Encryption_SEALED, KeyId: []byte{0xba, 0xd0}})
		require.Empty(t, References(sealed), kind.String())
	}
}
