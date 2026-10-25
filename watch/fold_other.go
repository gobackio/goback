//go:build !windows

package watch

func fold(rel string) string {
	return rel
}
