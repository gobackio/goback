package backup

import (
	"fmt"
	"strconv"
	"strings"
)

// EscapedSuffix ends the name EscapeName gives an entry Windows cannot hold.
const EscapedSuffix = ".goback-escaped"

// WindowsHolds reports whether Windows can hold an entry named name: one
// without the characters it refuses, <>:"|?* and the control characters,
// that is no device name such as CON or aux.c, and that is not made of dots
// and spaces alone. Windows drops the dots and spaces a name ends in, so it
// holds "notes." as "notes".
func WindowsHolds(name string) bool {
	if strings.TrimRight(name, ". ") == "" {
		return false
	}

	if strings.IndexFunc(name, func(r rune) bool { return r < 0x20 || strings.ContainsRune(`<>:"|?*`, r) }) >= 0 {
		return false
	}

	return !reservedOnWindows(name)
}

// EscapeName is the name a restore with TreeOptions.EscapeNames writes an
// entry named name under: the characters Windows refuses, a device name's
// first character, the dots and spaces it ends in and every % escaped as
// %XX, followed by EscapedSuffix. UnescapeName reverses it.
func EscapeName(name string) string {
	end := len(strings.TrimRight(name, ". "))
	reserved := reservedOnWindows(name)

	var b strings.Builder

	for i := 0; i < len(name); i++ {
		c := name[i]
		if c < 0x20 || strings.IndexByte(`<>:"|?*%`, c) >= 0 || i >= end || i == 0 && reserved {
			fmt.Fprintf(&b, "%%%02X", c)
			continue
		}

		b.WriteByte(c)
	}

	return b.String() + EscapedSuffix
}

// UnescapeName is the name EscapeName turned into name, and whether name is
// one EscapeName gives: a name Windows cannot hold or would drop the dots
// and spaces of, or one ending in EscapedSuffix itself, escaped.
func UnescapeName(name string) (string, bool) {
	escaped, ok := strings.CutSuffix(name, EscapedSuffix)
	if !ok {
		return "", false
	}

	var b strings.Builder

	for i := 0; i < len(escaped); i++ {
		if escaped[i] != '%' {
			b.WriteByte(escaped[i])
			continue
		}

		if i+2 >= len(escaped) {
			return "", false
		}

		c, err := strconv.ParseUint(escaped[i+1:i+3], 16, 8)
		if err != nil {
			return "", false
		}

		b.WriteByte(byte(c))
		i += 2
	}

	original := b.String()
	kept := !strings.HasSuffix(original, EscapedSuffix) && strings.TrimRight(original, ". ") == original
	if WindowsHolds(original) && kept || EscapeName(original) != name {
		return "", false
	}

	return original, true
}

// reservedOnWindows reports whether name is a device name, whatever follows
// a dot or a colon after it and the spaces it ends in.
func reservedOnWindows(name string) bool {
	base := name
	if i := strings.IndexAny(base, ".:"); i >= 0 {
		base = base[:i]
	}

	base = strings.ToUpper(strings.TrimRight(base, " "))

	switch base {
	case "CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$":
		return true
	}

	if len(base) < 4 || base[:3] != "COM" && base[:3] != "LPT" {
		return false
	}

	switch digit := base[3:]; digit {
	case "1", "2", "3", "4", "5", "6", "7", "8", "9", "¹", "²", "³":
		return true
	}

	return false
}
