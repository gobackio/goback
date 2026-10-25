package pack

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"
)

// MaintenanceExt names the leases on the store's maintenance: <n>.maintenance
// is the nth, held by the process that created it until it releases it or
// stops renewing it. Collect, RewritePlan, Repair, Revive and Compact each
// take it, and in another process that finds the newest one held they do
// nothing and report Busy.
const MaintenanceExt = ".maintenance"

// maintenanceLeaseTTL is how long a lease nobody renews stays held, and
// maintenanceLeaseRenewal how often its holder renews it.
var (
	maintenanceLeaseTTL     = time.Hour
	maintenanceLeaseRenewal = 10 * time.Minute
)

var leaseReleased = []byte("released")

func leaseName(n uint64) string {
	return fmt.Sprintf("%020d%s", n, MaintenanceExt)
}

// maintenanceLease is this process's lease on the store's maintenance.
type maintenanceLease struct {
	ps     *PackStorage
	n      uint64
	cancel context.CancelFunc
	done   chan struct{}
}

// storedLease is a lease a listing found.
type storedLease struct {
	n    uint64
	file ListedFile
}

// leases lists the stored leases, newest last.
func (ps *PackStorage) leases() ([]storedLease, error) {
	files, err := ListInfo(ps.storage, MaintenanceExt)
	if err != nil {
		return nil, err
	}

	var out []storedLease
	for _, file := range files {
		n, err := strconv.ParseUint(strings.TrimSuffix(file.Name, MaintenanceExt), 10, 64)
		if err == nil {
			out = append(out, storedLease{n: n, file: file})
		}
	}

	slices.SortFunc(out, func(a, b storedLease) int { return cmp.Compare(a.n, b.n) })

	return out, nil
}

// held reports whether the lease is neither released nor gone unrenewed.
func (ps *PackStorage) held(lease storedLease) (bool, error) {
	if time.Since(lease.file.Modified) >= maintenanceLeaseTTL {
		return false, nil
	}

	file, err := OpenListed(ps.storage, lease.file)
	if notExist(err) {
		return false, nil
	}

	if err != nil {
		return false, err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return false, err
	}

	return !bytes.Equal(data, leaseReleased), nil
}

// leaseMaintenance takes the store's maintenance for this process, or
// returns nil while another process holds it. The context it returns ends
// once another process may have taken the lease over.
func (ps *PackStorage) leaseMaintenance(ctx context.Context) (*maintenanceLease, context.Context, error) {
	leases, err := ps.leases()
	if err != nil {
		return nil, ctx, err
	}

	var n uint64
	if len(leases) > 0 {
		newest := leases[len(leases)-1]

		held, err := ps.held(newest)
		if err != nil || held {
			return nil, ctx, err
		}

		n = newest.n
	}

	err = ps.storage.CreateNew(leaseName(n+1), leaseData())
	if errors.Is(err, ErrFileExists) {
		return nil, ctx, nil
	}

	if err != nil {
		return nil, ctx, err
	}

	// a lease is deleted only by the holder of a newer one, which a
	// creation that came after it finds
	newer, err := ps.newerLease(n + 1)
	if err != nil || newer {
		return nil, ctx, err
	}

	for _, lease := range leases {
		ps.dropLease(lease.file.Name)
	}

	ctx, cancel := context.WithCancel(ctx)
	lease := &maintenanceLease{ps: ps, n: n + 1, cancel: cancel, done: make(chan struct{})}

	go lease.renew(ctx)

	return lease, ctx, nil
}

// newerLease reports whether a lease newer than the nth is stored.
func (ps *PackStorage) newerLease(n uint64) (bool, error) {
	leases, err := ps.leases()
	if err != nil {
		return false, err
	}

	return len(leases) > 0 && leases[len(leases)-1].n > n, nil
}

func leaseData() []byte {
	return []byte(time.Now().UTC().Format(time.RFC3339Nano))
}

func (ps *PackStorage) storeLease(n uint64, data []byte) error {
	file, err := ps.storage.Create(leaseName(n))
	if err != nil {
		return err
	}

	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}

	return file.Close()
}

func (ps *PackStorage) dropLease(name string) {
	if err := ps.storage.Delete(name); err != nil && !notExist(err) {
		ps.logger.Warn("deleting an old maintenance lease failed", "lease", name, "err", err)
	}
}

// renew keeps the lease fresh until ctx ends, and ends ctx once a newer
// lease is stored or this one went unrenewed for so long that another
// process may take it.
func (l *maintenanceLease) renew(ctx context.Context) {
	defer close(l.done)

	ticker := time.NewTicker(maintenanceLeaseRenewal)
	defer ticker.Stop()

	renewed := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if err := l.ps.storeLease(l.n, leaseData()); err != nil {
			l.ps.logger.Warn("renewing the maintenance lease failed", "lease", l.n, "err", err)

			if time.Since(renewed) >= maintenanceLeaseTTL-maintenanceLeaseRenewal {
				l.cancel()
				return
			}

			continue
		}

		renewed = time.Now()

		if newer, err := l.ps.newerLease(l.n); err == nil && newer {
			l.ps.logger.Warn("another process took the maintenance lease over", "lease", l.n)
			l.cancel()

			return
		}
	}
}

// release ends the lease, leaving it stored as released for the next one
// to follow.
func (l *maintenanceLease) release() {
	l.cancel()
	<-l.done

	if err := l.ps.storeLease(l.n, leaseReleased); err != nil {
		l.ps.logger.Warn("releasing the maintenance lease failed", "lease", l.n, "err", err)
	}
}
