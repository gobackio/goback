//go:build !linux && !darwin

package backup

import (
	"errors"
	"os"
)

// extendedIn reads nothing here: a walk records no extended attributes or
// flags on this platform.
func extendedIn(*os.Root, string) (Extended, error) {
	return Extended{}, errors.ErrUnsupported
}
