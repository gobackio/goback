package mysql

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
)

// Options are the connection settings MySQL's option files give the
// [client] group.
type Options struct {
	Host     string
	Port     int
	Socket   string
	User     string
	Password string
	// Protocol is tcp or socket, as --protocol takes it; empty picks by
	// Host.
	Protocol string
	// SSLMode, SSLCA, SSLCert and SSLKey are the --ssl-* options.
	SSLMode string
	SSLCA   string
	SSLCert string
	SSLKey  string
}

// ReadOptions reads the [client] group of extra, when set, and then of
// ~/.my.cnf, a later value winning, as MySQL's client programs do with
// --defaults-extra-file. A missing ~/.my.cnf is no error; a missing extra
// is.
func ReadOptions(extra string) (*Options, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		home = ""
	}

	return readOptions(extra, home)
}

func readOptions(extra, home string) (*Options, error) {
	values := map[string]string{}

	if extra != "" {
		if err := readOptionFile(extra, values, 0); err != nil {
			return nil, err
		}
	}

	if home != "" {
		err := readOptionFile(filepath.Join(home, ".my.cnf"), values, 0)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}

	o := &Options{
		Host:     values["host"],
		Socket:   values["socket"],
		User:     values["user"],
		Password: values["password"],
		Protocol: strings.ToLower(values["protocol"]),
		SSLMode:  strings.ToUpper(values["ssl-mode"]),
		SSLCA:    values["ssl-ca"],
		SSLCert:  values["ssl-cert"],
		SSLKey:   values["ssl-key"],
	}

	if port := values["port"]; port != "" {
		var err error
		if o.Port, err = strconv.Atoi(port); err != nil || o.Port <= 0 || o.Port > 65535 {
			return nil, fmt.Errorf("the option file's port %q", port)
		}
	}

	return o, nil
}

// maxIncludeDepth bounds !include chains, which may loop.
const maxIncludeDepth = 10

// readOptionFile reads the [client] group of the option file at path into
// values, following !include and !includedir.
func readOptionFile(path string, values map[string]string, depth int) error {
	if depth > maxIncludeDepth {
		return fmt.Errorf("%s: includes nest deeper than %d", path, maxIncludeDepth)
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	if info, err := f.Stat(); err == nil && runtime.GOOS != "windows" && info.Mode().Perm()&0o002 != 0 {
		return fmt.Errorf("%s is world-writable, and MySQL ignores such option files", path)
	}

	inGroup := false
	scanner := bufio.NewScanner(f)

	for n := 1; scanner.Scan(); n++ {
		line := strings.TrimSpace(scanner.Text())

		switch {
		case line == "" || line[0] == '#' || line[0] == ';':
			continue
		case strings.HasPrefix(line, "!includedir"):
			if err := readOptionDir(strings.TrimSpace(strings.TrimPrefix(line, "!includedir")), values, depth); err != nil {
				return err
			}

			continue
		case strings.HasPrefix(line, "!include"):
			included := strings.TrimSpace(strings.TrimPrefix(line, "!include"))
			if err := readOptionFile(included, values, depth+1); err != nil {
				return fmt.Errorf("%s:%d: %w", path, n, err)
			}

			continue
		case line[0] == '[':
			end := strings.IndexByte(line, ']')
			if end < 0 {
				return fmt.Errorf("%s:%d: a group line without ]", path, n)
			}

			inGroup = strings.EqualFold(strings.TrimSpace(line[1:end]), "client")

			continue
		}

		if !inGroup {
			continue
		}

		name, value, _ := strings.Cut(line, "=")
		values[optionName(name)] = optionValue(value)
	}

	return scanner.Err()
}

// readOptionDir reads the option files in dir: those ending in .cnf, and
// on Windows .ini too.
func readOptionDir(dir string, values map[string]string, depth int) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	var names []string
	for _, e := range entries {
		ext := filepath.Ext(e.Name())
		if e.Type().IsRegular() && (ext == ".cnf" || runtime.GOOS == "windows" && ext == ".ini") {
			names = append(names, e.Name())
		}
	}

	slices.Sort(names)

	for _, name := range names {
		if err := readOptionFile(filepath.Join(dir, name), values, depth+1); err != nil {
			return err
		}
	}

	return nil
}

// optionName is an option's name as goback looks it up: lowercased, with
// _ as -, and without the loose- prefix.
func optionName(name string) string {
	name = strings.ReplaceAll(strings.ToLower(strings.TrimSpace(name)), "_", "-")
	return strings.TrimPrefix(name, "loose-")
}

// optionValue reads an option's value as MySQL does: up to a # comment
// outside quotes, without the quotes around it, with MySQL's escape
// sequences.
func optionValue(raw string) string {
	raw = strings.TrimSpace(withoutComment(raw))

	if len(raw) > 1 && (raw[0] == '"' || raw[0] == '\'') && raw[len(raw)-1] == raw[0] {
		raw = raw[1 : len(raw)-1]
	}

	return unescapeOption(raw)
}

// withoutComment cuts s at the first # outside quotes; inside quotes, a
// backslash escapes a quote.
func withoutComment(s string) string {
	var quote byte

	escaped := false

	for i := 0; i < len(s); i++ {
		c := s[i]

		if (c == '"' || c == '\'') && !escaped {
			switch quote {
			case 0:
				quote = c
			case c:
				quote = 0
			}
		}

		if quote == 0 && c == '#' {
			return s[:i]
		}

		escaped = quote != 0 && c == '\\' && !escaped
	}

	return s
}

var optionEscapes = map[byte]byte{'b': '\b', 't': '\t', 'n': '\n', 'r': '\r', '\\': '\\', 's': ' ', '"': '"', '\'': '\''}

// unescapeOption converts \b, \t, \n, \r, \\, \s, \" and \'; a backslash
// before any other character stays.
func unescapeOption(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}

	var b strings.Builder

	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			if c, ok := optionEscapes[s[i+1]]; ok {
				b.WriteByte(c)
				i++

				continue
			}
		}

		b.WriteByte(s[i])
	}

	return b.String()
}
