//go:build !linux && !darwin

package backup

import "errors"

// extendedIn reads nothing here: a walk records no extended attributes or
// flags on this platform.
func extendedIn(localRoot, string) (Extended, error) {
	return Extended{}, errors.ErrUnsupported
}
