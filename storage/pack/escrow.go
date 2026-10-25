package pack

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/gobackio/goback/backup"
)

const (
	escrowDir = "keys"
	escrowExt = ".age"
)

var (
	_ backup.KeyEscrow   = (*PackStorage)(nil)
	_ backup.FirstEscrow = (*PackStorage)(nil)
)

// PutEscrowedKey implements backup.KeyEscrow. Each copy is a file of its
// own under keys/<key id>/, named by its content.
func (ps *PackStorage) PutEscrowedKey(ctx context.Context, key backup.EscrowedKey) error {
	return ps.putEscrowedKey(ctx, key, false)
}

// PutFirstEscrowedKey implements backup.FirstEscrow.
func (ps *PackStorage) PutFirstEscrowedKey(ctx context.Context, key backup.EscrowedKey) error {
	return ps.putEscrowedKey(ctx, key, true)
}

func (ps *PackStorage) putEscrowedKey(ctx context.Context, key backup.EscrowedKey, first bool) error {
	err := key.Validate()
	if err != nil {
		return err
	}

	ps.escrowMtx.Lock()
	defer ps.escrowMtx.Unlock()

	kept, err := ps.EscrowedKeys(ctx)
	if err != nil {
		return err
	}

	for _, k := range kept {
		if k.KeyID != key.KeyID {
			return fmt.Errorf("%w: it keeps key %s, not %s", backup.ErrOtherKeyEscrowed, k.KeyID, key.KeyID)
		}

		if bytes.Equal(k.Escrowed, key.Escrowed) {
			return nil
		}
	}

	if first && len(kept) > 0 {
		return fmt.Errorf("%w: %d copies of key %s", backup.ErrKeyEscrowed, len(kept), key.KeyID)
	}

	sum := sha256.Sum256(key.Escrowed)

	f, err := ps.storage.Create(path.Join(escrowDir, key.KeyID, hex.EncodeToString(sum[:8])+escrowExt))
	if err != nil {
		return err
	}

	_, err = f.Write(key.Escrowed)
	if err != nil {
		_ = f.Close()
		return err
	}

	return f.Close()
}

// EscrowedKeys implements backup.KeyEscrow.
func (ps *PackStorage) EscrowedKeys(context.Context) ([]backup.EscrowedKey, error) {
	names, err := ps.storage.List(escrowExt)
	if err != nil {
		return nil, err
	}

	var kept []backup.EscrowedKey

	for _, name := range names {
		name = path.Clean(strings.ReplaceAll(name, "\\", "/"))
		if !strings.HasPrefix(name, escrowDir+"/") {
			continue
		}

		escrowed, err := ps.readFile(name)
		if err != nil {
			return nil, fmt.Errorf("reading escrowed key %s: %w", name, err)
		}

		kept = append(kept, backup.EscrowedKey{KeyID: path.Base(path.Dir(name)), Escrowed: escrowed})
	}

	return kept, nil
}

func (ps *PackStorage) readFile(name string) ([]byte, error) {
	f, err := ps.storage.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return io.ReadAll(io.LimitReader(f, backup.MaxEscrow+1))
}
