package watch

import "strings"

// fold is how the tracker keys a path: NTFS names are case-insensitive.
func fold(rel string) string {
	return strings.ToLower(rel)
}
