// Package storekey implements the client-held store key: its key file,
// deterministic sealing of blobs, name tokens and inline content, and
// passphrase escrow. The server never sees
// the key. It carries none of the object format, so a WebAssembly build
// can take it without the rest of goback.
package storekey

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"

	"filippo.io/age"
	"filippo.io/age/armor"
	"github.com/tink-crypto/tink-go/v2/daead"
	"github.com/tink-crypto/tink-go/v2/insecurecleartextkeyset"
	"github.com/tink-crypto/tink-go/v2/keyset"
	"github.com/tink-crypto/tink-go/v2/prf"
	"github.com/tink-crypto/tink-go/v2/tink"
)

// IDSize is the width of a key id.
const IDSize = 8

var (
	// ErrWrongKey is returned when a token or sealed blob does not open
	// under the given key.
	ErrWrongKey = errors.New("wrong store key or corrupt ciphertext")
	// ErrNoKey is returned when an encrypted object is met without a key.
	ErrNoKey = errors.New("object is encrypted and no store key is loaded")
)

// Field names the FileInfo fields that are sealed.
type Field string

const (
	// FieldName is the entry name.
	FieldName Field = "name"
	// FieldUser is the owning user.
	FieldUser Field = "user"
	// FieldGroup is the owning group.
	FieldGroup Field = "group"
	// FieldTarget is a symlink's target.
	FieldTarget Field = "link_target"
	// FieldStream is an alternate stream's name, sealed within its entry.
	FieldStream Field = "stream"
	// FieldXattr is an extended attribute's name, sealed within its entry.
	FieldXattr Field = "xattr"
	// FieldHardLink is the path of the file a hard link shares its content
	// with.
	FieldHardLink Field = "hard_link"
)

// Mode is the store policy's encryption requirement for new writes. An
// agent holding a store key seals whatever the mode.
type Mode string

const (
	// ModeSealed refuses agents without a store key.
	ModeSealed Mode = "sealed"
	// ModeNone also admits agents without a store key, which write in the
	// clear.
	ModeNone Mode = "none"
)

// Policy is the store's write policy, as the agent holds it.
type Policy struct {
	Version       uint32 `json:"version"`
	Mode          Mode   `json:"mode"`
	PresenceScope string `json:"presence_scope"`
}

// DefaultPolicy seals everything and scopes presence to the store.
func DefaultPolicy() Policy {
	return Policy{Version: 1, Mode: ModeSealed, PresenceScope: "store"}
}

// Key is a store key, named after its store, with the policy the agent
// writes under. It is a pair of Tink keysets: deterministic AEAD seals,
// a PRF derives the key id and the digests PRF-named blobs carry.
type Key struct {
	Name   string
	Policy Policy

	seal, ident *keyset.Handle
	sealer      tink.DeterministicAEAD
	prf         *prf.Set
	id          []byte
}

const keyKind = "goback store key"

type keyFile struct {
	Kind   string          `json:"kind"`
	Name   string          `json:"name"`
	KeyID  string          `json:"key_id"`
	Policy Policy          `json:"policy"`
	Seal   json.RawMessage `json:"seal"`
	Ident  json.RawMessage `json:"ref"`
}

// Generate makes a fresh key named after its store.
func Generate(name string) (*Key, error) {
	seal, err := keyset.NewHandle(daead.AESSIVKeyTemplate())
	if err != nil {
		return nil, err
	}

	ident, err := keyset.NewHandle(prf.HMACSHA256PRFKeyTemplate())
	if err != nil {
		return nil, err
	}

	return New(name, DefaultPolicy(), seal, ident)
}

// New makes a key from its keysets: deterministic AEAD to seal with and a
// PRF to derive the key id and digests from.
func New(name string, policy Policy, seal, ident *keyset.Handle) (*Key, error) {
	sealer, err := daead.New(seal)
	if err != nil {
		return nil, fmt.Errorf("store key %s: %w", name, err)
	}

	set, err := prf.NewPRFSet(ident)
	if err != nil {
		return nil, fmt.Errorf("store key %s: %w", name, err)
	}

	id, err := set.ComputePrimaryPRF([]byte("goback store key id"), IDSize)
	if err != nil {
		return nil, err
	}

	return &Key{Name: name, Policy: policy, seal: seal, ident: ident, sealer: sealer, prf: set, id: id}, nil
}

// Load reads a key file written by Save.
func Load(path string) (*Key, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	key, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("store key %s: %w", path, err)
	}

	return key, nil
}

// Parse reads a key file's contents.
func Parse(data []byte) (*Key, error) {
	var f keyFile

	err := json.Unmarshal(data, &f)
	if err != nil {
		return nil, err
	}

	if f.Kind != keyKind {
		return nil, fmt.Errorf("not a store key file (kind %q)", f.Kind)
	}

	seal, err := ReadKeyset(f.Seal)
	if err != nil {
		return nil, err
	}

	ident, err := ReadKeyset(f.Ident)
	if err != nil {
		return nil, err
	}

	key, err := New(f.Name, f.Policy, seal, ident)
	if err != nil {
		return nil, err
	}

	if f.KeyID != "" && f.KeyID != key.IDString() {
		return nil, fmt.Errorf("key id %s does not match the key", f.KeyID)
	}

	return key, nil
}

// Save writes the key file, readable by its owner only. It refuses a path
// that exists, with an error matching fs.ErrExist, so no key is ever
// written over another.
func (k *Key) Save(path string) error {
	data, err := k.Marshal()
	if err != nil {
		return err
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("%s exists, refusing to overwrite a key: %w", path, err)
	}

	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}

	if err != nil {
		_ = os.Remove(path)
	}

	return err
}

// Marshal returns the key file's contents.
func (k *Key) Marshal() ([]byte, error) {
	seal, err := WriteKeyset(k.seal)
	if err != nil {
		return nil, err
	}

	ident, err := WriteKeyset(k.ident)
	if err != nil {
		return nil, err
	}

	data, err := json.MarshalIndent(keyFile{
		Kind: keyKind, Name: k.Name, KeyID: k.IDString(), Policy: k.Policy, Seal: seal, Ident: ident,
	}, "", "  ")
	if err != nil {
		return nil, err
	}

	return append(data, '\n'), nil
}

// ReadKeyset reads a keyset as a key file holds it.
func ReadKeyset(data json.RawMessage) (*keyset.Handle, error) {
	if len(data) == 0 {
		return nil, errors.New("the file holds no keyset")
	}

	return insecurecleartextkeyset.Read(keyset.NewJSONReader(bytes.NewReader(data)))
}

// WriteKeyset writes a keyset as a key file holds it.
func WriteKeyset(h *keyset.Handle) (json.RawMessage, error) {
	var buf bytes.Buffer

	err := insecurecleartextkeyset.Write(h, keyset.NewJSONWriter(&buf))
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// ID identifies the key in object headers.
func (k *Key) ID() []byte { return k.id }

// IDString is the hex form of ID.
func (k *Key) IDString() string { return hex.EncodeToString(k.id) }

// SealBlob seals a blob's stored bytes, bound to the key's id. Sealing is
// deterministic, so a blob stored twice is stored once.
func (k *Key) SealBlob(stored []byte) []byte {
	return k.encrypt(stored, k.blobAD())
}

// OpenBlob reverses SealBlob.
func (k *Key) OpenBlob(sealed []byte) ([]byte, error) {
	stored, err := k.sealer.DecryptDeterministically(sealed, k.blobAD())
	if err != nil {
		return nil, ErrWrongKey
	}

	return stored, nil
}

func (k *Key) blobAD() []byte {
	return append([]byte("blob"), k.id...)
}

// Digest is the PRF of a blob's plaintext, which a PRF-named blob's ref is
// made from: equal under one key, unrelated under another.
func (k *Key) Digest(plaintext []byte) []byte {
	out, err := k.prf.ComputePrimaryPRF(plaintext, 32)
	if err != nil {
		panic(fmt.Sprintf("computing a digest under store key %s: %v", k.IDString(), err))
	}

	return out
}

// OpenPRFBlob opens a PRF-named blob: stored bytes sealed bound to "blob"
// and the ref made from their plaintext's Digest, the form a store held
// sealed blobs in until it was resealed.
func (k *Key) OpenPRFBlob(sealed, ref []byte) ([]byte, error) {
	stored, err := k.sealer.DecryptDeterministically(sealed, append([]byte("blob"), ref...))
	if err != nil {
		return nil, ErrWrongKey
	}

	return stored, nil
}

// SealInline seals a small file's whole content.
func (k *Key) SealInline(plaintext []byte) []byte {
	return k.encrypt(plaintext, []byte("inline"))
}

// OpenInline reverses SealInline.
func (k *Key) OpenInline(ciphertext []byte) ([]byte, error) {
	plaintext, err := k.sealer.DecryptDeterministically(ciphertext, []byte("inline"))
	if err != nil {
		return nil, ErrWrongKey
	}

	return plaintext, nil
}

// SealField makes the deterministic token of a name-like field within its
// parent directory. Equal names in one directory give equal tokens; the
// same name elsewhere gives a different one. An empty value stays empty.
func (k *Key) SealField(parent []byte, field Field, plaintext []byte) []byte {
	if len(plaintext) == 0 {
		return nil
	}

	return k.encrypt(plaintext, fieldAD(parent, field))
}

// OpenField reverses SealField.
func (k *Key) OpenField(parent []byte, field Field, token []byte) ([]byte, error) {
	if len(token) == 0 {
		return nil, nil
	}

	plaintext, err := k.sealer.DecryptDeterministically(token, fieldAD(parent, field))
	if err != nil {
		return nil, ErrWrongKey
	}

	return plaintext, nil
}

func fieldAD(parent []byte, field Field) []byte {
	ad := binary.BigEndian.AppendUint32([]byte("field"), uint32(len(parent)))
	ad = append(ad, parent...)

	return append(ad, field...)
}

// encrypt seals under a loaded keyset, which only fails on a keyset Tink
// itself refused to load.
func (k *Key) encrypt(plaintext, ad []byte) []byte {
	out, err := k.sealer.EncryptDeterministically(plaintext, ad)
	if err != nil {
		panic(fmt.Sprintf("sealing under store key %s: %v", k.IDString(), err))
	}

	return out
}

// MinPassphrase is the fewest characters Escrow takes: anyone holding an
// agent key of the store can fetch the escrowed copy and guess at it
// offline.
const MinPassphrase = 12

// Escrow wraps the key file under a passphrase of at least MinPassphrase
// characters with age, so the key's owner can keep it where the server
// cannot open it. The result is an armored age file; `age -d` opens it
// too.
func (k *Key) Escrow(passphrase string) ([]byte, error) {
	if len([]rune(passphrase)) < MinPassphrase {
		return nil, fmt.Errorf("the passphrase needs at least %d characters", MinPassphrase)
	}

	data, err := k.Marshal()
	if err != nil {
		return nil, err
	}

	recipient, err := age.NewScryptRecipient(passphrase)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	armored := armor.NewWriter(&buf)

	w, err := age.Encrypt(armored, recipient)
	if err != nil {
		return nil, err
	}

	if _, err := w.Write(data); err != nil {
		return nil, err
	}

	if err := w.Close(); err != nil {
		return nil, err
	}

	if err := armored.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Recover opens an escrowed key with its passphrase.
func Recover(escrowed []byte, passphrase string) (*Key, error) {
	identity, err := age.NewScryptIdentity(passphrase)
	if err != nil {
		return nil, err
	}

	var in io.Reader = bytes.NewReader(escrowed)
	if strings.HasPrefix(strings.TrimSpace(string(escrowed)), armor.Header) {
		in = armor.NewReader(bytes.NewReader(bytes.TrimSpace(escrowed)))
	}

	r, err := age.Decrypt(in, identity)
	if err != nil {
		return nil, fmt.Errorf("opening the escrowed key: %w", err)
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("opening the escrowed key: %w", err)
	}

	return Parse(data)
}
