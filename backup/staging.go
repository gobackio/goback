package backup

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync"
)

// StagingDir is the directory a restore makes in each directory it writes
// files into, to assemble them in before they are renamed into place. A
// failed restore leaves it behind with what it assembled, and the next one
// resumes from it. A tree entry of this name is left out, with
// OutcomeUnsupportedName.
const StagingDir = ".goback-restore"

// StagingMarker is the file in a StagingDir that says a restore made it,
// and so may resume from the partial files in it.
const StagingMarker = StagingDir

// mkdirFS is a WriteFS that makes a directory only where nothing is.
type mkdirFS interface {
	Mkdir(ctx context.Context, dir string) error
}

// staging is the staging directories a Restorer found or made its own, by
// path, with the number of files being assembled in each.
type staging struct {
	mtx  sync.Mutex
	dirs map[string]int
}

// partial is the file RestoreFile assembles path in.
func (r *Restorer) partial(path string) string {
	fsys := r.fs()
	dir, name := fsys.Split(path)

	return fsys.Join(fsys.Join(dir, StagingDir), name)
}

// stage makes or takes over the staging directory of path's directory and
// returns path's partial file in it; done is called once that file is
// renamed into place or given up on. A staging directory a restore did not
// make, which holds no marker but is not empty, is refused.
func (r *Restorer) stage(ctx context.Context, path string) (partial string, done func(), err error) {
	fsys := r.fs()
	dir, _ := fsys.Split(path)
	staged := fsys.Join(dir, StagingDir)

	r.staging.mtx.Lock()
	defer r.staging.mtx.Unlock()

	if r.staging.dirs == nil {
		r.staging.dirs = map[string]int{}
	}

	if _, ok := r.staging.dirs[staged]; !ok {
		if err := r.makeStaging(ctx, staged); err != nil {
			return "", nil, err
		}
	}

	r.staging.dirs[staged]++

	return r.partial(path), func() {
		r.staging.mtx.Lock()
		r.staging.dirs[staged]--
		r.staging.mtx.Unlock()
	}, nil
}

// makeStaging makes staged, or takes it over when a restore made it.
func (r *Restorer) makeStaging(ctx context.Context, staged string) error {
	fsys := r.fs()
	marker := fsys.Join(staged, StagingMarker)

	var err error
	if m, ok := fsys.(mkdirFS); ok {
		err = m.Mkdir(ctx, staged)
	} else if _, err = fsys.Lstat(ctx, staged); errors.Is(err, fs.ErrNotExist) {
		err = fsys.MkdirAll(ctx, staged)
	} else if err == nil {
		err = fs.ErrExist
	}

	switch {
	case errors.Is(err, fs.ErrExist):
		if !r.ownsStaging(ctx, staged) {
			return fmt.Errorf("%s is in the way: no restore made it", staged)
		}
	case err != nil:
		return err
	}

	if _, err := fsys.Lstat(ctx, marker); errors.Is(err, fs.ErrNotExist) {
		_, err = fsys.WriteFile(ctx, marker, strings.NewReader(""))
		return err
	}

	return nil
}

// ownsStaging reports whether staged is a directory a restore made, which
// holds the marker, or an empty one a restore stopped making.
func (r *Restorer) ownsStaging(ctx context.Context, staged string) bool {
	fsys := r.fs()

	info, err := fsys.Lstat(ctx, staged)
	if err != nil || !info.IsDir() {
		return false
	}

	if marker, err := fsys.Lstat(ctx, fsys.Join(staged, StagingMarker)); err == nil {
		return marker.Mode().IsRegular()
	}

	list, err := fsys.ReadDir(ctx, staged)

	return err == nil && len(list) == 0
}

// stagedPartial is path's partial file when its directory's staging
// directory is one a restore made, which a restore may remove.
func (r *Restorer) stagedPartial(ctx context.Context, path string) (string, bool) {
	fsys := r.fs()
	dir, _ := fsys.Split(path)
	staged := fsys.Join(dir, StagingDir)

	r.staging.mtx.Lock()
	defer r.staging.mtx.Unlock()

	if _, ok := r.staging.dirs[staged]; !ok {
		if !r.ownsStaging(ctx, staged) {
			return "", false
		}

		if r.staging.dirs == nil {
			r.staging.dirs = map[string]int{}
		}

		r.staging.dirs[staged] = 0
	}

	return r.partial(path), true
}

// cleanStaging removes each staging directory the restore found or made
// that no file is assembled in and that holds nothing but its marker. One
// that still holds the partial files of failed restores stays for the
// next one.
func (r *Restorer) cleanStaging(ctx context.Context) error {
	if r.DryRun {
		return nil
	}

	fsys := r.fs()

	r.staging.mtx.Lock()
	defer r.staging.mtx.Unlock()

	var errs []error

	for staged, users := range r.staging.dirs {
		if users > 0 {
			continue
		}

		delete(r.staging.dirs, staged)

		list, err := fsys.ReadDir(ctx, staged)
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) && !errors.Is(err, errors.ErrUnsupported) {
				errs = append(errs, err)
			}

			continue
		}

		if len(list) > 1 || len(list) == 1 && list[0].Name() != StagingMarker {
			continue
		}

		if err := removeIfThere(ctx, fsys, fsys.Join(staged, StagingMarker)); err != nil {
			errs = append(errs, err)
			continue
		}

		errs = append(errs, removeIfThere(ctx, fsys, staged))
	}

	return errors.Join(errs...)
}
