//go:build !linux && !windows

package watch

import "fmt"

// Open opens a Source following root. On this system there is none.
func Open(root string) (Source, error) {
	return nil, fmt.Errorf("%w on this system", ErrUnavailable)
}
