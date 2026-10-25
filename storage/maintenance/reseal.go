package maintenance

import (
	"context"
	"errors"
	"fmt"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/storage/pack"
	"github.com/gobackio/goback/storage/wrapped"
)

// Resealer is a store that rewrites its PRF-named blobs under the refs of
// their sealed bytes.
type Resealer interface {
	Reseal(ctx context.Context, keys pack.KeyOpener) (*pack.ResealReport, error)
}

// Reseal reseals store as pack.PackStorage.Reseal does, opening keys with
// keys. It fails with an error matching errors.ErrUnsupported when the
// store cannot be resealed. Rebuild the index from the store afterwards,
// into an empty database.
func Reseal(ctx context.Context, store backup.ObjectStore, keys pack.KeyOpener) (*pack.ResealReport, error) {
	resealer, ok := wrapped.Innermost(store).(Resealer)
	if !ok {
		return nil, fmt.Errorf("store %T cannot be resealed: %w", wrapped.Innermost(store), errors.ErrUnsupported)
	}

	return resealer.Reseal(ctx, keys)
}
