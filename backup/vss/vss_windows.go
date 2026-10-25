//go:build windows

// The VSS calls in this file are derived from restic's
// internal/fs/vss_windows.go, under this licence:
//
// Copyright (c) 2014, Alexander Neumann <alexander@bumpern.de>
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// * Redistributions of source code must retain the above copyright notice, this
//   list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
//   this list of conditions and the following disclaimer in the documentation
//   and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package vss

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/gobackio/goback/backup"

	"golang.org/x/sys/windows"
)

// Snapshot is a set of shadow copies taken together.
type Snapshot struct {
	thread  *comThread
	vss     *backupComponents
	set     windows.GUID
	shadows []shadow
	taken   bool
}

// shadow is one volume's shadow copy: mount is where the volume is
// mounted, with a trailing separator, and device the shadow copy's root.
type shadow struct {
	mount  string
	device string
}

// Take creates a shadow copy of every volume holding one of paths, after
// the writers of applications on them flushed, and returns once all of
// them exist. Until Release, FS reads those volumes as they were.
func Take(ctx context.Context, paths ...string) (*Snapshot, error) {
	if runtime.GOARCH != "amd64" {
		return nil, fmt.Errorf("%w: %s", ErrUnsupported, runtime.GOARCH)
	}

	mounts, err := mountsOf(paths)
	if err != nil {
		return nil, err
	}

	thread, err := startCOM()
	if err != nil {
		return nil, err
	}

	s := &Snapshot{thread: thread}

	err = thread.do(func() error { return s.take(ctx, mounts) })
	if err != nil {
		_ = s.Release()
		return nil, err
	}

	return s, nil
}

// FS reads the local disk as backup.LocalFS does, except that a path on
// one of the snapshot's volumes is read from its shadow copy. Paths stay
// the live ones, so a walk records them as they are on the disk.
func (s *Snapshot) FS() backup.FS {
	return shadowFS{snapshot: s}
}

// Path returns where path, an absolute path on one of the snapshot's
// volumes, is in its shadow copy.
func (s *Snapshot) Path(path string) (string, error) {
	path = filepath.Clean(path)

	for _, sh := range s.shadows {
		rel, ok := under(path, sh.mount)
		if ok {
			return sh.device + `\` + rel, nil
		}
	}

	return "", fmt.Errorf("%s is not on a volume of the snapshot", path)
}

// Release tells the volumes' writers the backup is complete, deletes the
// shadow copies and ends the snapshot. A snapshot is released once.
func (s *Snapshot) Release() error {
	if s.thread == nil {
		return nil
	}

	err := s.thread.do(s.release)
	s.thread.stop()
	s.thread = nil

	return err
}

func (s *Snapshot) take(ctx context.Context, mounts []string) error {
	initSecurity()

	vss, err := newBackupComponents()
	if err != nil {
		return err
	}

	s.vss = vss

	if err := vss.call("InitializeForBackup", vss.vtbl().initializeForBackup, 0); err != nil {
		return err
	}

	if err := vss.call("SetContext", vss.vtbl().setContext, vssCtxBackup); err != nil {
		return err
	}

	// a copy backup leaves other backup products' history alone
	if err := vss.call("SetBackupState", vss.vtbl().setBackupState, 0, 0, vssBtCopy, 0); err != nil {
		return err
	}

	if err := vss.wait(ctx, "GatherWriterMetadata", vss.vtbl().gatherWriterMetadata); err != nil {
		return err
	}

	volumes := make([]string, len(mounts))
	for i, mount := range mounts {
		volumes[i], err = volumeName(mount)
		if err != nil {
			return err
		}

		supported, err := vss.isVolumeSupported(volumes[i])
		if err != nil {
			return err
		}

		if !supported {
			return fmt.Errorf("%w: volume %s", ErrUnsupported, mount)
		}
	}

	if s.set, err = vss.startSnapshotSet(ctx); err != nil {
		return err
	}

	ids := make([]windows.GUID, len(mounts))
	for i, volume := range volumes {
		if ids[i], err = vss.addToSnapshotSet(volume); err != nil {
			return err
		}
	}

	if err := vss.wait(ctx, "PrepareForBackup", vss.vtbl().prepareForBackup); err != nil {
		return err
	}

	if err := vss.wait(ctx, "DoSnapshotSet", vss.vtbl().doSnapshotSet); err != nil {
		return err
	}

	s.taken = true

	for i, mount := range mounts {
		device, err := vss.deviceObject(ids[i])
		if err != nil {
			return err
		}

		s.shadows = append(s.shadows, shadow{mount: mount, device: device})
	}

	// a volume mounted in a folder of another wins over that one
	slices.SortFunc(s.shadows, func(a, b shadow) int { return len(b.mount) - len(a.mount) })

	return nil
}

func (s *Snapshot) release() error {
	if s.vss == nil {
		return nil
	}

	defer s.vss.release()

	if !s.taken {
		return s.vss.call("AbortBackup", s.vss.vtbl().abortBackup)
	}

	err := s.vss.wait(context.Background(), "BackupComplete", s.vss.vtbl().backupComplete)

	var deleted int32
	var undeleted windows.GUID
	delErr := s.vss.call("DeleteSnapshots", s.vss.vtbl().deleteSnapshots,
		uintptr(unsafe.Pointer(&s.set)), vssObjectSnapshotSet, 1,
		uintptr(unsafe.Pointer(&deleted)), uintptr(unsafe.Pointer(&undeleted)))

	return errors.Join(err, delErr)
}

// mountsOf returns the mount points of the volumes paths are on, each
// once and with a trailing separator.
func mountsOf(paths []string) ([]string, error) {
	var mounts []string

	for _, path := range paths {
		abs, err := filepath.Abs(path)
		if err != nil {
			return nil, err
		}

		name, err := windows.UTF16PtrFromString(abs)
		if err != nil {
			return nil, err
		}

		buf := make([]uint16, windows.MAX_LONG_PATH)
		if err := windows.GetVolumePathName(name, &buf[0], uint32(len(buf))); err != nil {
			return nil, fmt.Errorf("finding the volume of %s: %w", abs, err)
		}

		mount := windows.UTF16ToString(buf)
		if !strings.HasSuffix(mount, `\`) {
			mount += `\`
		}

		if !slices.ContainsFunc(mounts, func(m string) bool { return strings.EqualFold(m, mount) }) {
			mounts = append(mounts, mount)
		}
	}

	return mounts, nil
}

// under returns path relative to mount, which ends in a separator, when
// path is mount or inside it.
func under(path, mount string) (string, bool) {
	if strings.EqualFold(path+`\`, mount) {
		return "", true
	}

	if len(path) < len(mount) || !strings.EqualFold(path[:len(mount)], mount) {
		return "", false
	}

	return path[len(mount):], true
}

// volumeName is the \\?\Volume{GUID}\ name of the volume mounted at mount.
func volumeName(mount string) (string, error) {
	name, err := windows.UTF16PtrFromString(mount)
	if err != nil {
		return "", err
	}

	buf := make([]uint16, 50)
	if err := windows.GetVolumeNameForVolumeMountPoint(name, &buf[0], uint32(len(buf))); err != nil {
		return "", fmt.Errorf("finding the volume mounted at %s: %w", mount, err)
	}

	return windows.UTF16ToString(buf), nil
}

var (
	vssapi                 = windows.NewLazySystemDLL("VssApi.dll")
	createBackupComponents = vssapi.NewProc("?CreateVssBackupComponents@@YAJPEAPEAVIVssBackupComponents@@@Z")
	freeSnapshotProperties = vssapi.NewProc("VssFreeSnapshotProperties")
	coInitializeSecurity   = windows.NewLazySystemDLL("ole32.dll").NewProc("CoInitializeSecurity")

	securityOnce sync.Once
)

const (
	vssCtxBackup            = 0
	vssBtCopy               = 5
	vssObjectSnapshotSet    = 2
	rpcCAuthnLevelPrivacy   = 6
	rpcCImpLevelImpersonate = 3
	eoacStaticCloaking      = 0x20
)

// initSecurity sets the process's COM security once, as a requester must
// for the writers' callbacks; a process whose COM security was already
// set keeps it.
func initSecurity() {
	securityOnce.Do(func() {
		authnSvc := int32(-1)
		_, _, _ = coInitializeSecurity.Call(0, uintptr(authnSvc), 0, 0,
			rpcCAuthnLevelPrivacy, rpcCImpLevelImpersonate, 0, eoacStaticCloaking, 0)
	})
}

// comThread runs every call on one snapshot's COM objects on one locked
// OS thread in the multithreaded apartment.
type comThread struct {
	calls chan func()
}

func startCOM() (*comThread, error) {
	t := &comThread{calls: make(chan func())}
	started := make(chan error)

	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		if err := windows.CoInitializeEx(0, windows.COINIT_MULTITHREADED); err != nil {
			started <- fmt.Errorf("initializing COM: %w", err)
			return
		}
		defer windows.CoUninitialize()

		started <- nil

		for call := range t.calls {
			call()
		}
	}()

	if err := <-started; err != nil {
		return nil, err
	}

	return t, nil
}

func (t *comThread) do(f func() error) error {
	done := make(chan error)
	t.calls <- func() { done <- f() }

	return <-done
}

func (t *comThread) stop() {
	close(t.calls)
}

// hresult is a COM result code.
type hresult uint32

const (
	sOK                       hresult = 0
	eAccessDenied             hresult = 0x80070005
	vssESnapshotSetInProgress hresult = 0x80042316
	vssSAsyncPending          hresult = 0x00042309
	vssSAsyncFinished         hresult = 0x0004230A
	vssSAsyncCancelled        hresult = 0x0004230B
)

var hresultNames = map[hresult]string{
	eAccessDenied:             "E_ACCESSDENIED",
	0x80070057:                "E_INVALIDARG",
	0x80042301:                "VSS_E_BAD_STATE",
	0x80042302:                "VSS_E_UNEXPECTED",
	0x80042306:                "VSS_E_PROVIDER_VETO",
	0x80042308:                "VSS_E_OBJECT_NOT_FOUND",
	0x8004230C:                "VSS_E_VOLUME_NOT_SUPPORTED",
	0x8004230E:                "VSS_E_VOLUME_NOT_SUPPORTED_BY_PROVIDER",
	0x8004230F:                "VSS_E_UNEXPECTED_PROVIDER_ERROR",
	0x80042312:                "VSS_E_MAXIMUM_NUMBER_OF_VOLUMES_REACHED",
	0x80042313:                "VSS_E_FLUSH_WRITES_TIMEOUT",
	0x80042314:                "VSS_E_HOLD_WRITES_TIMEOUT",
	0x80042315:                "VSS_E_UNEXPECTED_WRITER_ERROR",
	vssESnapshotSetInProgress: "VSS_E_SNAPSHOT_SET_IN_PROGRESS",
	0x80042317:                "VSS_E_MAXIMUM_NUMBER_OF_SNAPSHOTS_REACHED",
	0x80042318:                "VSS_E_WRITER_INFRASTRUCTURE",
	0x80042319:                "VSS_E_WRITER_NOT_RESPONDING",
	0x8004231F:                "VSS_E_INSUFFICIENT_STORAGE",
	0x8004232D:                "VSS_E_VOLUME_NOT_LOCAL",
}

func (h hresult) Error() string {
	if name, ok := hresultNames[h]; ok {
		return fmt.Sprintf("%s (%#x)", name, uint32(h))
	}

	return fmt.Sprintf("HRESULT %#x", uint32(h))
}

func check(name string, r uintptr) error {
	if h := hresult(r); h != sOK {
		return fmt.Errorf("VSS %s: %w", name, h)
	}

	return nil
}

// backupComponents is IVssBackupComponents.
type backupComponents struct {
	vtable *backupComponentsVtbl
}

type backupComponentsVtbl struct {
	queryInterface                uintptr
	addRef                        uintptr
	release                       uintptr
	getWriterComponentsCount      uintptr
	getWriterComponents           uintptr
	initializeForBackup           uintptr
	setBackupState                uintptr
	initializeForRestore          uintptr
	setRestoreState               uintptr
	gatherWriterMetadata          uintptr
	getWriterMetadataCount        uintptr
	getWriterMetadata             uintptr
	freeWriterMetadata            uintptr
	addComponent                  uintptr
	prepareForBackup              uintptr
	abortBackup                   uintptr
	gatherWriterStatus            uintptr
	getWriterStatusCount          uintptr
	freeWriterStatus              uintptr
	getWriterStatus               uintptr
	setBackupSucceeded            uintptr
	setBackupOptions              uintptr
	setSelectedForRestore         uintptr
	setRestoreOptions             uintptr
	setAdditionalRestores         uintptr
	setPreviousBackupStamp        uintptr
	saveAsXML                     uintptr
	backupComplete                uintptr
	addAlternativeLocationMapping uintptr
	addRestoreSubcomponent        uintptr
	setFileRestoreStatus          uintptr
	addNewTarget                  uintptr
	setRangesFilePath             uintptr
	preRestore                    uintptr
	postRestore                   uintptr
	setContext                    uintptr
	startSnapshotSet              uintptr
	addToSnapshotSet              uintptr
	doSnapshotSet                 uintptr
	deleteSnapshots               uintptr
	importSnapshots               uintptr
	breakSnapshotSet              uintptr
	getSnapshotProperties         uintptr
	query                         uintptr
	isVolumeSupported             uintptr
}

func newBackupComponents() (*backupComponents, error) {
	if err := createBackupComponents.Find(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnsupported, err)
	}

	var vss *backupComponents
	r, _, _ := createBackupComponents.Call(uintptr(unsafe.Pointer(&vss)))

	if hresult(r) == eAccessDenied {
		return nil, ErrPrivilege
	}

	if err := check("CreateVssBackupComponents", r); err != nil {
		return nil, err
	}

	return vss, nil
}

func (b *backupComponents) vtbl() *backupComponentsVtbl {
	return b.vtable
}

func (b *backupComponents) call(name string, method uintptr, args ...uintptr) error {
	r, _, _ := syscall.SyscallN(method, append([]uintptr{uintptr(unsafe.Pointer(b))}, args...)...)
	return check(name, r)
}

func (b *backupComponents) release() {
	_, _, _ = syscall.SyscallN(b.vtbl().release, uintptr(unsafe.Pointer(b)))
}

// wait starts an asynchronous call and waits for it to finish, cancelling
// it when ctx ends first.
func (b *backupComponents) wait(ctx context.Context, name string, method uintptr) error {
	var op *async
	if err := b.call(name, method, uintptr(unsafe.Pointer(&op))); err != nil {
		return err
	}
	defer op.release()

	for {
		status, err := op.queryStatus()
		if err != nil {
			return fmt.Errorf("VSS %s: %w", name, err)
		}

		switch status {
		case vssSAsyncFinished:
			return nil
		case vssSAsyncCancelled:
			return fmt.Errorf("VSS %s was cancelled", name)
		case vssSAsyncPending:
		default:
			return fmt.Errorf("VSS %s: %w", name, status)
		}

		select {
		case <-ctx.Done():
			op.cancel()
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (b *backupComponents) isVolumeSupported(volume string) (bool, error) {
	name, err := windows.UTF16PtrFromString(volume)
	if err != nil {
		return false, err
	}

	var provider windows.GUID
	var supported int32
	err = b.call("IsVolumeSupported", b.vtbl().isVolumeSupported,
		uintptr(unsafe.Pointer(&provider)), uintptr(unsafe.Pointer(name)), uintptr(unsafe.Pointer(&supported)))

	return supported != 0, err
}

// startSnapshotSet starts the set, waiting out another requester's set
// in progress until ctx ends.
func (b *backupComponents) startSnapshotSet(ctx context.Context) (windows.GUID, error) {
	for {
		var set windows.GUID
		err := b.call("StartSnapshotSet", b.vtbl().startSnapshotSet, uintptr(unsafe.Pointer(&set)))
		if !errors.Is(err, vssESnapshotSetInProgress) {
			return set, err
		}

		select {
		case <-ctx.Done():
			return set, err
		case <-time.After(5 * time.Second):
		}
	}
}

func (b *backupComponents) addToSnapshotSet(volume string) (windows.GUID, error) {
	name, err := windows.UTF16PtrFromString(volume)
	if err != nil {
		return windows.GUID{}, err
	}

	var provider, id windows.GUID
	err = b.call("AddToSnapshotSet", b.vtbl().addToSnapshotSet,
		uintptr(unsafe.Pointer(name)), uintptr(unsafe.Pointer(&provider)), uintptr(unsafe.Pointer(&id)))

	return id, err
}

// snapshotProperties is VSS_SNAPSHOT_PROP.
type snapshotProperties struct {
	snapshotID           windows.GUID
	snapshotSetID        windows.GUID
	snapshotsCount       int32
	snapshotDeviceObject *uint16
	originalVolumeName   *uint16
	originatingMachine   *uint16
	serviceMachine       *uint16
	exposedName          *uint16
	exposedPath          *uint16
	providerID           windows.GUID
	snapshotAttributes   int32
	creationTimestamp    int64
	status               int32
}

// deviceObject is the root of the shadow copy id, such as
// \\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy3.
func (b *backupComponents) deviceObject(id windows.GUID) (string, error) {
	var props snapshotProperties
	err := b.call("GetSnapshotProperties", b.vtbl().getSnapshotProperties,
		uintptr(unsafe.Pointer(&id)), uintptr(unsafe.Pointer(&props)))
	if err != nil {
		return "", err
	}
	defer freeSnapshotProperties.Call(uintptr(unsafe.Pointer(&props)))

	return windows.UTF16PtrToString(props.snapshotDeviceObject), nil
}

// async is IVssAsync.
type async struct {
	vtable *asyncVtbl
}

type asyncVtbl struct {
	queryInterface uintptr
	addRef         uintptr
	release        uintptr
	cancel         uintptr
	wait           uintptr
	queryStatus    uintptr
}

func (a *async) release() {
	_, _, _ = syscall.SyscallN(a.vtable.release, uintptr(unsafe.Pointer(a)))
}

func (a *async) cancel() {
	_, _, _ = syscall.SyscallN(a.vtable.cancel, uintptr(unsafe.Pointer(a)))
}

func (a *async) queryStatus() (hresult, error) {
	var status hresult
	r, _, _ := syscall.SyscallN(a.vtable.queryStatus, uintptr(unsafe.Pointer(a)), uintptr(unsafe.Pointer(&status)), 0)

	return status, check("QueryStatus", r)
}

// shadowFS is Snapshot.FS.
type shadowFS struct {
	snapshot *Snapshot
	local    backup.LocalFS
}

var (
	_ backup.FS         = shadowFS{}
	_ backup.MetadataFS = shadowFS{}
)

func (f shadowFS) ReadDir(ctx context.Context, dir string) ([]os.DirEntry, error) {
	path, err := f.snapshot.Path(dir)
	if err != nil {
		return nil, err
	}

	return f.local.ReadDir(ctx, path)
}

func (f shadowFS) Stat(ctx context.Context, path string) (os.FileInfo, error) {
	shadowed, err := f.snapshot.Path(path)
	if err != nil {
		return nil, err
	}

	return f.local.Stat(ctx, shadowed)
}

func (f shadowFS) Lstat(ctx context.Context, path string) (os.FileInfo, error) {
	shadowed, err := f.snapshot.Path(path)
	if err != nil {
		return nil, err
	}

	return f.local.Lstat(ctx, shadowed)
}

func (f shadowFS) Open(ctx context.Context, path string) (backup.File, error) {
	shadowed, err := f.snapshot.Path(path)
	if err != nil {
		return nil, err
	}

	return f.local.Open(ctx, shadowed)
}

func (f shadowFS) Readlink(ctx context.Context, path string) (string, error) {
	shadowed, err := f.snapshot.Path(path)
	if err != nil {
		return "", err
	}

	return f.local.Readlink(ctx, shadowed)
}

func (f shadowFS) Security(ctx context.Context, path string) ([]byte, error) {
	shadowed, err := f.snapshot.Path(path)
	if err != nil {
		return nil, err
	}

	return f.local.Security(ctx, shadowed)
}

func (f shadowFS) Streams(ctx context.Context, path string) ([]backup.StreamInfo, error) {
	shadowed, err := f.snapshot.Path(path)
	if err != nil {
		return nil, err
	}

	return f.local.Streams(ctx, shadowed)
}

func (f shadowFS) OpenStream(ctx context.Context, path, name string) (backup.File, error) {
	shadowed, err := f.snapshot.Path(path)
	if err != nil {
		return nil, err
	}

	return f.local.OpenStream(ctx, shadowed, name)
}

func (f shadowFS) Join(dir, name string) string {
	return f.local.Join(dir, name)
}

func (f shadowFS) Owners(info os.FileInfo) (string, string) {
	return f.local.Owners(info)
}

func (f shadowFS) Traits() backup.Traits {
	return f.local.Traits()
}
