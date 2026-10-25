// Package childenv gives the programs goback runs, hooks and database
// tools, an environment without goback's secrets.
package childenv

import (
	"os"
	"regexp"
	"runtime"
	"strings"
)

var (
	secrets   = []string{"GOBACK_PASSPHRASE", "GOBACK_ADMIN_KEY"}
	locations = []string{"GOBACK_STORAGE", "GOBACK_INDEX", "GOBACK_POSTGRES_DBNAME"}

	passwordKeyword = regexp.MustCompile(`(?i)(^|\s)password\s*=`)
)

// Environ is os.Environ without GOBACK_PASSPHRASE and GOBACK_ADMIN_KEY, and
// without GOBACK_STORAGE, GOBACK_INDEX and GOBACK_POSTGRES_DBNAME when they
// carry a credential: a URL with a user part, such as a store server's api
// key or a database password, or a password= keyword. Set it as an
// exec.Cmd's Env, appending what the program needs.
func Environ() []string {
	var env []string

	for _, kv := range os.Environ() {
		name, value, _ := strings.Cut(kv, "=")
		if !secret(name, value) {
			env = append(env, kv)
		}
	}

	return env
}

func secret(name, value string) bool {
	for _, s := range secrets {
		if sameName(name, s) {
			return true
		}
	}

	for _, l := range locations {
		if sameName(name, l) && credential(value) {
			return true
		}
	}

	return false
}

func sameName(a, b string) bool {
	if runtime.GOOS == "windows" {
		return strings.EqualFold(a, b)
	}

	return a == b
}

// credential reports whether a location holds a user part before its host,
// or a password keyword.
func credential(value string) bool {
	if passwordKeyword.MatchString(value) {
		return true
	}

	_, rest, ok := strings.Cut(value, "://")
	if !ok {
		return false
	}

	if end := strings.IndexAny(rest, "/?#"); end >= 0 {
		rest = rest[:end]
	}

	return strings.Contains(rest, "@")
}
