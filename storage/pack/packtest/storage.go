package packtest

import (
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/gobackio/goback/storage/pack"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

// TestArchiveStorage runs the ArchiveStorage contract against an empty
// store and leaves it empty.
func TestArchiveStorage(t *testing.T, store pack.ArchiveStorage) {
	type test struct {
		name string
		fn   func(t *testing.T, store pack.ArchiveStorage, files []file)
	}

	tests := []test{
		{"create files", testCreateFiles},
		{"read files", testReadFiles},
		{"missing file", testMissingFiles},
		{"list files", testListAllFiles},
		{"list files filtered", testListSomeFiles},
		{"list files with sizes and times", testListInfo},
		{"nested names", testNestedNames},
		{"create new", testCreateNew},
		{"move", testMove},
		{"test delete all", testDeleteAll},
	}

	files := getStorageTestFiles(t)
	runTests := func(t *testing.T, store pack.ArchiveStorage) {
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				test.fn(t, store, files)
			})
		}
	}

	runTests(t, store)

}

func testCreateFiles(t *testing.T, store pack.ArchiveStorage, files []file) {
	for _, f := range files {
		file, err := store.Create(f.key)
		if err != nil {
			t.Fatal(err)
		}

		n, err := file.Write(f.data)
		if err != nil {
			t.Fatal(err)
		}

		if n != len(f.data) {
			t.Fatal("didn't write full file content")
		}

		err = file.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func testMissingFiles(t *testing.T, store pack.ArchiveStorage, files []file) {
	file, err := store.Open("this file probably does not exist")
	if err != pack.ErrFileNotFound {
		t.Fatal("expected open to return error")
	}

	if file != nil {
		t.Fatal("expected file to be nil")
	}

	if err := store.Delete("this file probably does not exist"); err != pack.ErrFileNotFound {
		t.Fatalf("expected delete to return %v, got %v", pack.ErrFileNotFound, err)
	}
}

func testReadFiles(t *testing.T, store pack.ArchiveStorage, files []file) {
	for _, f := range files {
		file, err := store.Open(f.key)
		if err != nil {
			t.Fatal(err)
		}

		info, err := file.Stat()
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(info.Name(), path.Base(f.key)); diff != "" {
			t.Error("file name doesn't match")
			t.Fatal(diff)
		}

		if diff := cmp.Diff(info.Size(), int64(len(f.data))); diff != "" {
			t.Error("file size doesn't match")
			t.Fatal(diff)
		}

		data, err := io.ReadAll(file)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(data, f.data); diff != "" {
			t.Error("data doesn't match")
			t.Fatal(diff)
		}
	}
}

func testListAllFiles(t *testing.T, store pack.ArchiveStorage, files []file) {
	names, err := store.List("")
	if err != nil {
		t.Fatal(err)
	}

	var expected []string
	for _, f := range files {
		expected = append(expected, path.Base(f.key))
	}

	sort.Strings(names)
	sort.Strings(expected)

	if diff := cmp.Diff(expected, names); diff != "" {
		t.Error("unexpected list of files")
		t.Fatal(diff)
	}
}

func testListInfo(t *testing.T, store pack.ArchiveStorage, files []file) {
	lister, ok := store.(pack.InfoLister)
	if !ok {
		t.Skip("the store lists names only")
	}

	listed, err := lister.ListInfo(".goback")
	require.NoError(t, err)

	names, err := store.List(".goback")
	require.NoError(t, err)
	require.Len(t, listed, len(names))

	sizes := make(map[string]int64)
	for _, f := range files {
		sizes[path.Base(f.key)] = int64(len(f.data))
	}

	for _, l := range listed {
		require.Contains(t, names, l.Name)
		require.Equal(t, sizes[l.Name], l.Size, l.Name)

		file, err := store.Open(l.Name)
		require.NoError(t, err)
		info, err := file.Stat()
		require.NoError(t, err)
		require.NoError(t, file.Close())
		require.True(t, info.ModTime().Equal(l.Modified), "%s: listed %s, stored %s", l.Name, l.Modified, info.ModTime())
	}
}

func testListSomeFiles(t *testing.T, store pack.ArchiveStorage, files []file) {
	names, err := store.List(".goback")
	if err != nil {
		t.Fatal(err)
	}

	var expected []string
	for _, f := range files {
		if path.Ext(f.key) == ".goback" {
			expected = append(expected, path.Base(f.key))
		}
	}

	sort.Strings(names)
	sort.Strings(expected)

	if diff := cmp.Diff(expected, names); diff != "" {
		t.Error("unexpected list of files")
		t.Fatal(diff)
	}
}

func testDeleteAll(t *testing.T, store pack.ArchiveStorage, files []file) {
	list, err := store.List("")
	if err != nil {
		t.Fatal(err)
	}

	if len(list) == 0 {
		t.Fatal("store should contain files")
	}

	err = store.DeleteAll()
	if err != nil {
		t.Fatal(err)
	}

	list, err = store.List("")
	if err != nil {
		t.Fatal(err)
	}

	if len(list) != 0 {
		t.Fatal("store should not contain files:", list)
	}

}

type file struct {
	key  string
	data []byte
}

func getStorageTestFiles(t *testing.T) []file {
	t.Helper()

	// testdata lives beside this file, not the caller's
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("couldn't get caller")
	}
	t.Log(filepath.FromSlash(filename))

	root := filepath.Join(filepath.Dir(filename), "testdata")

	var files []file

	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		testKey := filepath.ToSlash(strings.TrimPrefix(p, root))[1:]

		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}

		files = append(files, file{
			key:  testKey,
			data: data,
		})

		return nil
	})

	if err != nil {
		t.Fatal(err)
	}

	if len(files) == 0 {
		t.Fatal("no test files loaded")
	}

	return files
}

// testNestedNames stores under a slash-separated name, as session archives
// are, and expects listing to return the full name.
func testNestedNames(t *testing.T, store pack.ArchiveStorage, files []file) {
	const name = "s1/t1/session/nested.goback"

	f, err := store.Create(name)
	if err != nil {
		t.Fatal(err)
	}

	if _, err = f.Write([]byte("nested")); err != nil {
		t.Fatal(err)
	}

	if err = f.Close(); err != nil {
		t.Fatal(err)
	}

	names, err := store.List(".goback")
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, n := range names {
		if n == name {
			found = true
		}
	}

	if !found {
		t.Fatalf("expected %q in %v", name, names)
	}

	f, err = store.Open(name)
	if err != nil {
		t.Fatal(err)
	}

	data, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}

	if string(data) != "nested" {
		t.Fatalf("unexpected content %q", data)
	}

	if err = f.Close(); err != nil {
		t.Fatal(err)
	}

	if err = store.Delete(name); err != nil {
		t.Fatal(err)
	}

	if _, err = store.Open(name); err != pack.ErrFileNotFound {
		t.Fatalf("expected the file to be gone, got %v", err)
	}
}

func testCreateNew(t *testing.T, store pack.ArchiveStorage, _ []file) {
	require.NoError(t, store.CreateNew("marker.new", []byte("first")))
	require.ErrorIs(t, store.CreateNew("marker.new", []byte("second")), pack.ErrFileExists)

	file, err := store.Open("marker.new")
	require.NoError(t, err)

	data, err := io.ReadAll(file)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	require.Equal(t, "first", string(data), "a taken name keeps what it held")

	require.NoError(t, store.Delete("marker.new"))
}

func testMove(t *testing.T, store pack.ArchiveStorage, _ []file) {
	require.NoError(t, store.CreateNew("s1/moved.goback", []byte("moved")))
	require.NoError(t, store.CreateNew("s1/moved.goback.quarantine", []byte("replaced")))
	require.NoError(t, store.Move("s1/moved.goback", "s1/moved.goback.quarantine"))

	_, err := store.Open("s1/moved.goback")
	require.ErrorIs(t, err, pack.ErrFileNotFound)

	names, err := store.List(".quarantine")
	require.NoError(t, err)
	require.Equal(t, []string{"s1/moved.goback.quarantine"}, names)

	file, err := store.Open("s1/moved.goback.quarantine")
	require.NoError(t, err)
	data, err := io.ReadAll(file)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	require.Equal(t, "moved", string(data))

	require.ErrorIs(t, store.Move("s1/moved.goback", "s1/elsewhere.goback"), pack.ErrFileNotFound)
	require.NoError(t, store.Delete("s1/moved.goback.quarantine"))
}
