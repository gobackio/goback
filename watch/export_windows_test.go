package watch

// TestBackend is a backend the tests run against.
type TestBackend struct {
	Name  string
	Open  func(root string) (Source, error)
	Needs string
}

// Backends are the system's backends.
var Backends = []TestBackend{
	{"usn", Open, "reading the change journal needs an elevated shell"},
}
