package watch

// Open opens a Source following root with inotify.
func Open(root string) (Source, error) {
	return openInotify(root)
}
