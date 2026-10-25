package proto

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
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

func sealedFixture() *Stored {
	return &Stored{
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

	payload, err := obj.Canonical()
	require.NoError(t, err)
	require.Equal(t, sealed.Data, payload)

	// a sealed object must claim an encryption and a key
	plain := sealedFixture()
	plain.Encryption = Encryption_PLAINTEXT
	require.Error(t, NewObject(plain).Validate())

	keyless := sealedFixture()
	keyless.KeyId = nil
	require.Error(t, NewObject(keyless).Validate())
}

func TestASealedRefHashesTheKeyIDTheCodecAndTheSealedBytes(t *testing.T) {
	sealed := sealedFixture()
	ref := NewObject(sealed).Ref()

	payload := append([]byte{8}, "keyid123"...)
	payload = append(payload, byte(Compression_NONE))
	payload = append(payload, "ciphertext"...)

	sum := sha256.Sum256(append([]byte("sealed blob "+strconv.Itoa(len(payload))+"\x00"), payload...))
	require.Equal(t, sum[:], ref.Hash)
	require.NoError(t, NewObject(sealed).Verify(ref))

	for name, change := range map[string]func(*Stored){
		"key id":      func(s *Stored) { s.KeyId = []byte("keyid124") },
		"compression": func(s *Stored) { s.Compression = Compression_ZSTD },
		"data":        func(s *Stored) { s.Data = []byte("ciphertexu") },
	} {
		other := sealedFixture()
		change(other)
		require.False(t, NewObject(other).Ref().Equal(ref), name)
		require.ErrorIs(t, NewObject(other).Verify(ref), ErrRefMismatch, name)
	}

	require.False(t, NewObject(&Blob{Data: sealed.Data}).Ref().Equal(ref), "no sealed ref is a plaintext one")
}

func TestSealedStoredRoundTrip(t *testing.T) {
	obj := NewObject(sealedFixture())

	hdr, stored, err := HeaderFor(obj)
	require.NoError(t, err)
	require.Equal(t, obj.Sealed().Data, stored, "sealed bytes are stored as they arrive")
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
	require.NotNil(t, back.Sealed())
	require.True(t, back.Ref().Equal(obj.Ref()))
	require.Equal(t, obj.Sealed().Data, back.Sealed().Data)
	require.Equal(t, Encryption_SEALED, back.Sealed().Encryption)
	require.Equal(t, []byte("keyid123"), back.Sealed().KeyId)
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
