//go:build !windows

package backup

import (
	"os"
	"testing"
)

func setSparse(*testing.T, *os.File) {}
