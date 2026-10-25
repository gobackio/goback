package watch

// OpenInotify and OpenFanotify open one backend for the tests.
func OpenInotify(root string) (Source, error)  { return asSource(openInotify(root)) }
func OpenFanotify(root string) (Source, error) { return asSource(openFanotify(root)) }
