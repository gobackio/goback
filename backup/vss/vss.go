// Package vss reads Windows volumes from Volume Shadow Copy snapshots, so
// a backup sees every file as it was at one instant, after the writers of
// applications such as databases flushed their data.
package vss

import "errors"

var (
	// ErrUnsupported is returned by Take where shadow copies can't be
	// taken: off Windows, and on Windows on other than amd64.
	ErrUnsupported = errors.New("shadow copies are not supported here")

	// ErrPrivilege is returned by Take for a process that is not an
	// elevated administrator.
	ErrPrivilege = errors.New("taking a shadow copy needs an elevated administrator")
)
