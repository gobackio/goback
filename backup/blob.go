package backup

import (
	"bytes"
	"fmt"

	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"
)

// BlobRef is the ref a chunk is stored under when it is sealed with key.
func BlobRef(key *storekey.Key, chunk []byte) *proto.Ref {
	return SealBlob(key, chunk).SealedRef()
}

// SealBlob compresses a chunk and seals it under key.
func SealBlob(key *storekey.Key, chunk []byte) *proto.Stored {
	stored, compression := proto.Encode(chunk)

	return &proto.Stored{
		Type:        proto.ObjectType_BLOB,
		Data:        key.SealBlob(stored),
		Compression: compression,
		Encryption:  proto.Encryption_SEALED,
		KeyId:       key.ID(),
	}
}

// OpenBlob reverses SealBlob, refusing a blob sealed under another key.
// The caller checks the sealed blob against the ref it was read for.
func OpenBlob(key *storekey.Key, sealed *proto.Stored) ([]byte, error) {
	if sealed.Encryption != proto.Encryption_SEALED || sealed.Type != proto.ObjectType_BLOB {
		return nil, fmt.Errorf("sealed %s object under %s is not a sealed blob", sealed.Type, sealed.Encryption)
	}

	if !bytes.Equal(sealed.KeyId, key.ID()) {
		return nil, fmt.Errorf("%w: blob %x is sealed under key %x", storekey.ErrWrongKey, sealed.SealedRef().GetHash(), sealed.KeyId)
	}

	stored, err := key.OpenBlob(sealed.Data)
	if err != nil {
		return nil, err
	}

	chunk, err := proto.Decode(stored, sealed.Compression)
	if err != nil {
		return nil, fmt.Errorf("blob %x: %w", sealed.SealedRef().GetHash(), err)
	}

	return chunk, nil
}
