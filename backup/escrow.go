package backup

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/gobackio/goback/backup/storekey"

	"filippo.io/age/armor"
)

// MaxEscrow is far above what an escrowed key file takes up.
const MaxEscrow = 64 << 10

var (
	// ErrOtherKeyEscrowed is returned when a store is given the escrowed
	// copy of a store key other than the one it already keeps.
	ErrOtherKeyEscrowed = errors.New("the store already keeps the escrowed copy of another store key")
	// ErrKeyEscrowed is returned when a first escrowed copy is put where
	// the store already keeps another copy of that key.
	ErrKeyEscrowed = errors.New("the store already keeps an escrowed copy of the store key")
	// ErrInvalidEscrow is returned for an escrowed key that is not an
	// armored age file of a sensible size, or has no valid key id.
	ErrInvalidEscrow = errors.New("not an escrowed store key")
)

// EscrowedKey is a store key escrowed under a passphrase, as goback key
// escrow prints it, with the id of the key inside.
type EscrowedKey struct {
	KeyID    string
	Escrowed []byte
}

// Validate reports ErrInvalidEscrow unless the key has a key id and is an
// armored age file of at most MaxEscrow bytes.
func (k EscrowedKey) Validate() error {
	if _, err := hex.DecodeString(k.KeyID); err != nil || k.KeyID == "" {
		return fmt.Errorf("%w: key id %q", ErrInvalidEscrow, k.KeyID)
	}

	if len(k.Escrowed) > MaxEscrow || !bytes.HasPrefix(bytes.TrimSpace(k.Escrowed), []byte(armor.Header)) {
		return fmt.Errorf("%w: an armored age file of at most %d bytes is expected", ErrInvalidEscrow, MaxEscrow)
	}

	return nil
}

// KeyEscrow keeps a store's escrowed store key beside its data, so the
// key survives anything the data survives. The store cannot open it.
type KeyEscrow interface {
	// PutEscrowedKey keeps one escrowed copy, once: nothing kept is ever
	// replaced. A store keeps copies of one store key only.
	PutEscrowedKey(ctx context.Context, key EscrowedKey) error
	EscrowLister
}

// FirstEscrow keeps the first escrowed copy of a store key and no other.
type FirstEscrow interface {
	// PutFirstEscrowedKey keeps key only while the store keeps no copy of
	// that key, and is ErrKeyEscrowed otherwise; putting the copy kept
	// again changes nothing. Of copies put at once, one is first.
	PutFirstEscrowedKey(ctx context.Context, key EscrowedKey) error
}

// EscrowLister reads the escrowed copies of a store's key.
type EscrowLister interface {
	// EscrowedKeys returns every copy kept.
	EscrowedKeys(ctx context.Context) ([]EscrowedKey, error)
}

// RecoverKey opens the store key with the first escrowed copy the
// passphrase opens.
func RecoverKey(ctx context.Context, escrow EscrowLister, passphrase string) (*storekey.Key, error) {
	kept, err := escrow.EscrowedKeys(ctx)
	if err != nil {
		return nil, err
	}

	if len(kept) == 0 {
		return nil, errors.New("the store keeps no escrowed key; upload one with goback key escrow --upload")
	}

	for _, k := range kept {
		if key, err := storekey.Recover(k.Escrowed, passphrase); err == nil {
			return key, nil
		}
	}

	return nil, fmt.Errorf("the passphrase opens none of the %d escrowed copies the store keeps", len(kept))
}
