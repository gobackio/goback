package backup

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWindowsHolds(t *testing.T) {
	for _, name := range []string{"a.txt", "cone.d", "comma,", "COM10", "x.con", "100%", ".hidden", "notes.", "Projects "} {
		require.True(t, WindowsHolds(name), name)
	}

	for _, name := range []string{"1700000000.M1P2.host:2,S", "aux.c", "CON", "nul.txt", "COM1", "lpt9.log", "com¹", "conin$", "con .txt", "...", ". .", "a?b", "a\x01b", `a"b`} {
		require.False(t, WindowsHolds(name), name)
	}
}

func TestAnEscapedNameIsHeldAndReversed(t *testing.T) {
	for _, name := range []string{"1700000000.M1P2.host:2,S", "aux.c", "CON", "a.", "a. ", "...", "50%:off", "a?b*c", "x" + EscapedSuffix} {
		escaped := EscapeName(name)
		require.True(t, WindowsHolds(escaped), escaped)

		back, ok := UnescapeName(escaped)
		require.True(t, ok, escaped)
		require.Equal(t, name, back)
	}
}

func TestOnlyANameEscapeNameGivesIsUnescaped(t *testing.T) {
	for _, name := range []string{"report" + EscapedSuffix, "a%3ab" + EscapedSuffix, "a%3" + EscapedSuffix, "a%zzb" + EscapedSuffix, "plain.txt", "100%25" + EscapedSuffix} {
		_, ok := UnescapeName(name)
		require.False(t, ok, name)
	}
}
