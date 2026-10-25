package proto

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCanonicalGoldenVectorsEncryption(t *testing.T) {
	tree := refOf("tree")

	cases := []struct {
		name    string
		object  *Object
		payload string
	}{
		{
			name:    "sealed inline file",
			object:  NewObject(&File{Inline: []byte("hi"), InlineEncryption: Encryption_SEALED}),
			payload: "220268693003",
		},
		{
			name:    "commit with policy",
			object:  NewObject(&Commit{Timestamp: 1700000000, Tree: tree, BackupSet: "world", AgentId: "agent-1", ScanStartNs: 1700000000000000000, Partial: true, PolicyVersion: 1}),
			payload: "0880e2cfaa0612220a20dc9c5edb8b2d479e697b4b0b8ab874f32b325138598ce9e7b759eb82921106221a05776f726c642a076167656e742d31308080a8b1e39fe7cb1738014801",
		},
		{
			name: "tree with token names",
			object: NewObject(&Tree{Nodes: []*TreeNode{
				{Stat: &FileInfo{Name: []byte{0x00, 0xff}, Mode: 0644, Size: 5, MtimeNs: 1, User: []byte{0x01}}, Ref: tree},
			}}),
			payload: "0a340a0e0a0200ff10a4031a01013005400112220a20dc9c5edb8b2d479e697b4b0b8ab874f32b325138598ce9e7b759eb8292110622",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := tc.object.Canonical()
			require.NoError(t, err)
			require.Equal(t, tc.payload, hex.EncodeToString(payload))

			decoded, err := NewObjectFromPayload(payload, tc.object.Type())
			require.NoError(t, err)

			again, err := decoded.Canonical()
			require.NoError(t, err)
			require.Equal(t, payload, again)
		})
	}
}

func sealedFixture() *Sealed {
	return &Sealed{
		Ref:         refOf("blob key"),
		Type:        ObjectType_BLOB,
		Data:        []byte("ciphertext"),
		Compression: Compression_NONE,
		Encryption:  Encryption_SEALED,
		KeyId:       []byte("keyid123"),
	}
}

func TestSealedObject(t *testing.T) {
	sealed := sealedFixture()
	obj := NewObject(sealed)

	require.Equal(t, ObjectType_BLOB, obj.Type())
	require.True(t, obj.Ref().Equal(sealed.Ref), "a sealed object's ref is the one the client computed")

	payload, err := obj.Canonical()
	require.NoError(t, err)
	require.Equal(t, sealed.Data, payload)

	// a sealed object must claim an encryption and carry a valid ref
	plain := sealedFixture()
	plain.Encryption = Encryption_PLAINTEXT
	require.Error(t, NewObject(plain).Validate())

	bad := sealedFixture()
	bad.Ref = &Ref{Hash: []byte("short")}
	require.Error(t, NewObject(bad).Validate())
}

func TestSealedStoredRoundTrip(t *testing.T) {
	obj := NewObject(sealedFixture())

	hdr, stored, err := HeaderFor(obj)
	require.NoError(t, err)
	require.Equal(t, obj.GetSealed().Data, stored, "sealed bytes are stored as they arrive")
	require.Equal(t, Encryption_SEALED, hdr.Encryption)
	require.Equal(t, []byte("keyid123"), hdr.KeyId)
	require.Equal(t, StoredHash(stored), hdr.StoredHash)
	require.True(t, hdr.Ref.Equal(obj.Ref()))

	require.NoError(t, VerifyStored(hdr, stored))

	corrupt := append([]byte(nil), stored...)
	corrupt[0] ^= 1
	require.Error(t, VerifyStored(hdr, corrupt), "the server checks the stored hash")

	back, err := ObjectFromStored(hdr, stored)
	require.NoError(t, err)
	require.NotNil(t, back.GetSealed())
	require.True(t, back.Ref().Equal(obj.Ref()))
	require.Equal(t, obj.GetSealed().Data, back.GetSealed().Data)
	require.Equal(t, Encryption_SEALED, back.GetSealed().Encryption)
	require.Equal(t, []byte("keyid123"), back.GetSealed().KeyId)
	require.Equal(t, []byte("keyid123"), back.KeyId)

	_, err = ObjectFromStored(hdr, corrupt)
	require.Error(t, err)
}

func TestPlaintextStoredRoundTrip(t *testing.T) {
	obj := NewObject(&File{Inline: []byte("hi"), InlineEncryption: Encryption_SEALED})
	obj.KeyId = []byte("keyid123")

	hdr, stored, err := HeaderFor(obj)
	require.NoError(t, err)
	require.Equal(t, Encryption_PLAINTEXT, hdr.Encryption)
	require.Equal(t, []byte("keyid123"), hdr.KeyId, "the key id of a structure object rides in the header")
	require.NoError(t, VerifyStored(hdr, stored))

	back, err := ObjectFromStored(hdr, stored)
	require.NoError(t, err)
	require.NotNil(t, back.GetFile())
	require.Equal(t, Encryption_SEALED, back.GetFile().InlineEncryption)
	require.Equal(t, []byte("keyid123"), back.KeyId)
	require.True(t, back.Ref().Equal(obj.Ref()))
}

func TestOnlyABlobIsSealed(t *testing.T) {
	for _, kind := range []ObjectType{ObjectType_COMMIT, ObjectType_TREE, ObjectType_FILE, ObjectType_PIN, ObjectType_POLICY} {
		sealed := sealedFixture()
		sealed.Type = kind
		require.ErrorIs(t, NewObject(sealed).Validate(), ErrInvalidObject, kind.String())
	}
}
