//go:build !windows

package mysql

import "os"

// syncDir makes the directory's entries durable, so a renamed file
// survives a crash under its new name.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}

	err = d.Sync()
	if closeErr := d.Close(); err == nil {
		err = closeErr
	}

	return err
}
