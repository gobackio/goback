// Package wrapped finds the store beneath stores layered over it, such as
// a cache: Innermost(store) is the store that keeps the objects, to test
// for what it implements.
package wrapped

import (
	"github.com/gobackio/goback/backup"
)

// Wrapper is a store layered over another store.
type Wrapper interface {
	Unwrap() backup.ObjectStore
}

// Unwrap returns the store beneath store, nil when it wraps none.
func Unwrap(store backup.ObjectStore) backup.ObjectStore {
	if s, ok := store.(Wrapper); ok {
		return s.Unwrap()
	}

	return nil
}

// Innermost peels every wrapping store off store.
func Innermost(store backup.ObjectStore) backup.ObjectStore {
	for inner := Unwrap(store); inner != nil; inner = Unwrap(store) {
		store = inner
	}

	return store
}
