package pack

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/gobackio/goback/proto"

	"github.com/tink-crypto/tink-go/v2/aead"
	"github.com/tink-crypto/tink-go/v2/insecurecleartextkeyset"
	"github.com/tink-crypto/tink-go/v2/keyderivation"
	"github.com/tink-crypto/tink-go/v2/keyset"
	"github.com/tink-crypto/tink-go/v2/prf"
	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
	"github.com/tink-crypto/tink-go/v2/tink"
)

// ErrAtRestKeyMissing is returned when a record is sealed at rest and the
// store was opened without a key.
var ErrAtRestKeyMissing = errors.New("record is sealed at rest and the store has no key")

// ErrAtRestKeyMismatch is returned when a record is sealed under a key
// the store's keyset does not hold.
var ErrAtRestKeyMismatch = errors.New("record is sealed at rest under another key")

// AtRestKey seals every payload on its way into an archive and opens it on
// the way out; headers stay readable, so an index can be rebuilt without
// the key. It is the server's own layer: an agent's sealed blobs are
// sealed once more, and what an agent sent in plaintext is plaintext
// again for every reader with the key.
//
// Each archive is sealed under its own key, derived from this one and the
// archive's name, so a location can carry the key to one archive without
// giving away the store's. Its primary key seals and every key in it
// opens, so rotating is Rotate, and a key can leave the keyset once a
// rewrite has re-sealed the last record naming it (Scrub counts them).
type AtRestKey struct {
	handle  *keyset.Handle
	deriver keyderivation.KeysetDeriver
	id      []byte
	holds   map[string]bool
}

func atRestTemplate() (*tinkpb.KeyTemplate, error) {
	return keyderivation.CreatePRFBasedKeyTemplate(prf.HKDFSHA256PRFKeyTemplate(), aead.XChaCha20Poly1305KeyTemplate())
}

// NewAtRestKey wraps a keyset of AEAD key derivers, as GenerateAtRestKey
// makes them.
func NewAtRestKey(h *keyset.Handle) (*AtRestKey, error) {
	deriver, err := keyderivation.New(h)
	if err != nil {
		return nil, fmt.Errorf("at-rest key: %w", err)
	}

	info := h.KeysetInfo()

	return &AtRestKey{handle: h, deriver: deriver, id: keyID(info.GetPrimaryKeyId()), holds: holding(h)}, nil
}

func holding(h *keyset.Handle) map[string]bool {
	info := h.KeysetInfo()
	holds := make(map[string]bool, len(info.GetKeyInfo()))
	for _, key := range info.GetKeyInfo() {
		holds[string(keyID(key.GetKeyId()))] = true
	}

	return holds
}

func keyID(id uint32) []byte {
	return binary.BigEndian.AppendUint32(nil, id)
}

// GenerateAtRestKey makes a keyset of one fresh key.
func GenerateAtRestKey() (*AtRestKey, error) {
	template, err := atRestTemplate()
	if err != nil {
		return nil, err
	}

	h, err := keyset.NewHandle(template)
	if err != nil {
		return nil, err
	}

	return NewAtRestKey(h)
}

// LoadAtRestKey reads a keyset written by Save.
func LoadAtRestKey(path string) (*AtRestKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	h, err := insecurecleartextkeyset.Read(keyset.NewJSONReader(bytes.NewReader(data)))
	if err != nil {
		return nil, fmt.Errorf("at-rest key %s: %w", path, err)
	}

	return NewAtRestKey(h)
}

// Save writes the keyset, readable by its owner only. It replaces a file
// at path whole or not at all.
func (k *AtRestKey) Save(path string) error {
	var buf bytes.Buffer

	err := insecurecleartextkeyset.Write(k.handle, keyset.NewJSONWriter(&buf))
	if err != nil {
		return err
	}

	temp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())

	_, err = temp.Write(buf.Bytes())
	if err == nil {
		err = temp.Sync()
	}

	if closeErr := temp.Close(); err == nil {
		err = closeErr
	}

	if err != nil {
		return err
	}

	return os.Rename(temp.Name(), path)
}

// Wrap returns the keyset sealed under kek, a key-encryption key such as
// one held in a KMS, bound to ad; UnwrapAtRestKey reverses it.
func (k *AtRestKey) Wrap(kek tink.AEAD, ad []byte) ([]byte, error) {
	var buf bytes.Buffer

	err := k.handle.WriteWithAssociatedData(keyset.NewBinaryWriter(&buf), kek, ad)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// UnwrapAtRestKey opens a keyset Wrap sealed under kek with ad.
func UnwrapAtRestKey(wrapped []byte, kek tink.AEAD, ad []byte) (*AtRestKey, error) {
	h, err := keyset.ReadWithAssociatedData(keyset.NewBinaryReader(bytes.NewReader(wrapped)), kek, ad)
	if err != nil {
		return nil, fmt.Errorf("unwrapping the at-rest key: %w", err)
	}

	return NewAtRestKey(h)
}

// Rotate returns the keyset with a fresh primary key; the keys it held
// still open what they sealed.
func (k *AtRestKey) Rotate() (*AtRestKey, error) {
	template, err := atRestTemplate()
	if err != nil {
		return nil, err
	}

	manager := keyset.NewManagerFromHandle(k.handle)

	id, err := manager.Add(template)
	if err != nil {
		return nil, err
	}

	err = manager.SetPrimary(id)
	if err != nil {
		return nil, err
	}

	h, err := manager.Handle()
	if err != nil {
		return nil, err
	}

	return NewAtRestKey(h)
}

// ID marks the records sealed under the primary key.
func (k *AtRestKey) ID() []byte {
	return k.id
}

// archive derives the key the named archive is sealed under, from the
// last element of its name.
func (k *AtRestKey) archive(name string) (*archiveKey, error) {
	h, err := k.deriver.DeriveKeyset([]byte(path.Base(name)))
	if err != nil {
		return nil, fmt.Errorf("deriving the at-rest key of archive %s: %w", name, err)
	}

	return newArchiveKey(h)
}

// archiveKey seals and opens the records of one archive.
type archiveKey struct {
	aead   tink.AEAD
	id     []byte
	holds  map[string]bool
	shared []byte
}

func newArchiveKey(h *keyset.Handle) (*archiveKey, error) {
	a, err := aead.New(h)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer

	err = insecurecleartextkeyset.Write(h, keyset.NewBinaryWriter(&buf))
	if err != nil {
		return nil, err
	}

	return &archiveKey{aead: a, id: keyID(h.KeysetInfo().GetPrimaryKeyId()), holds: holding(h), shared: buf.Bytes()}, nil
}

// parseArchiveKey reads the key a location carries.
func parseArchiveKey(shared []byte) (*archiveKey, error) {
	h, err := insecurecleartextkeyset.Read(keyset.NewBinaryReader(bytes.NewReader(shared)))
	if err != nil {
		return nil, fmt.Errorf("the location's at-rest key: %w", err)
	}

	return newArchiveKey(h)
}

func atRestAAD(hdr *proto.ObjectHeader) []byte {
	return append(append([]byte(nil), hdr.Ref.GetHash()...), byte(hdr.Type))
}

// seal returns a payload sealed under the primary key, bound to the
// record's ref and type.
func (k *archiveKey) seal(hdr *proto.ObjectHeader, payload []byte) ([]byte, error) {
	return k.aead.Encrypt(payload, atRestAAD(hdr))
}

// openAtRest returns a stored payload as the agent sent it: opened when the
// header says it is sealed, as is otherwise.
func openAtRest(key *archiveKey, hdr *proto.ObjectHeader, stored []byte) ([]byte, error) {
	if len(hdr.AtRestKeyId) == 0 {
		return stored, nil
	}

	if key == nil {
		return nil, fmt.Errorf("%w: record %x", ErrAtRestKeyMissing, hdr.Ref.GetHash())
	}

	if !key.holds[string(hdr.AtRestKeyId)] {
		return nil, fmt.Errorf("%w: record %x under key %x", ErrAtRestKeyMismatch, hdr.Ref.GetHash(), hdr.AtRestKeyId)
	}

	payload, err := key.aead.Decrypt(stored, atRestAAD(hdr))
	if err != nil {
		return nil, fmt.Errorf("opening record %x sealed at rest: %w", hdr.Ref.GetHash(), err)
	}

	return payload, nil
}
