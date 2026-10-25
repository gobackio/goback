package watch

// Open opens a Source following root through the NTFS change journal of
// its volume, which needs an elevated administrator or LocalSystem.
func Open(root string) (Source, error) {
	s, err := openUSN(root)
	if err != nil {
		return nil, err
	}

	return s, nil
}
