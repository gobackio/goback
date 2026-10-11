package mysql

// syncDir does nothing: NTFS journals renames, and Windows cannot sync a
// directory handle.
func syncDir(string) error { return nil }
