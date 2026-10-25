package watch

import "errors"

// Open opens a Source following root: fanotify where the process may mark
// the root's whole filesystem and the kernel names the directory of each
// event (Linux 5.9 on, with CAP_SYS_ADMIN and CAP_DAC_READ_SEARCH), and
// inotify otherwise.
func Open(root string) (Source, error) {
	s, err := asSource(openFanotify(root))
	if err == nil {
		return s, nil
	}

	if !errors.Is(err, ErrUnavailable) {
		return nil, err
	}

	return asSource(openInotify(root))
}

// asSource keeps a failed open's nil from becoming a non-nil Source.
func asSource[S Source](s S, err error) (Source, error) {
	if err != nil {
		return nil, err
	}

	return s, nil
}
