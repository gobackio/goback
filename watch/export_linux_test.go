package watch

// TestBackend is a backend the tests run against.
type TestBackend struct {
	Name  string
	Open  func(root string) (Source, error)
	Needs string
}

// Backends are the system's backends.
var Backends = []TestBackend{
	{"inotify", func(root string) (Source, error) { return asSource(openInotify(root)) }, ""},
	{"fanotify", func(root string) (Source, error) { return asSource(openFanotify(root)) }, "fanotify needs root"},
}
