package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/user"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gobackio/goback/proto"
)

// Owner says whose the files, directories and links a Restorer writes
// become. The zero Owner leaves them to whoever runs the restore;
// ParseOwner reads the others. Any other Owner needs a tree that is an
// OwnerWriteFS, which on the local disk means Unix and root.
type Owner struct {
	// Recorded gives each entry the user and group its backup recorded,
	// looked up by name here. An entry whose user or group has no account
	// here keeps the restoring user's.
	Recorded bool
	// User, unless Recorded, gives every entry to this account and to
	// Group, or to the user's primary group when Group is empty.
	User, Group string
}

// ParseOwner reads "none", "recorded" or "USER[:GROUP]".
func ParseOwner(s string) (Owner, error) {
	switch s {
	case "none":
		return Owner{}, nil
	case "recorded":
		return Owner{Recorded: true}, nil
	}

	name, group, _ := strings.Cut(s, ":")
	if name == "" {
		return Owner{}, fmt.Errorf("unknown owner %q: use none, recorded or USER[:GROUP]", s)
	}

	return Owner{User: name, Group: group}, nil
}

// String is the owner as ParseOwner reads it.
func (o Owner) String() string {
	switch {
	case o.Recorded:
		return "recorded"
	case o.User == "":
		return "none"
	case o.Group == "":
		return o.User
	}

	return o.User + ":" + o.Group
}

// accounts looks up the accounts of the machine restored onto; a nil
// function asks os/user.
type accounts struct {
	user  func(name string) (*user.User, error)
	group func(name string) (*user.Group, error)
}

func (a accounts) lookupUser(name string) (uid, gid int, err error) {
	lookup := a.user
	if lookup == nil {
		lookup = user.Lookup
	}

	u, err := lookup(name)
	if err != nil {
		return 0, 0, err
	}

	if uid, err = strconv.Atoi(u.Uid); err != nil {
		return 0, 0, fmt.Errorf("user %s has uid %q: %w", name, u.Uid, err)
	}

	if gid, err = strconv.Atoi(u.Gid); err != nil {
		return 0, 0, fmt.Errorf("user %s has gid %q: %w", name, u.Gid, err)
	}

	return uid, gid, nil
}

func (a accounts) lookupGroup(name string) (int, error) {
	lookup := a.group
	if lookup == nil {
		lookup = user.LookupGroup
	}

	g, err := lookup(name)
	if err != nil {
		return 0, err
	}

	gid, err := strconv.Atoi(g.Gid)
	if err != nil {
		return 0, fmt.Errorf("group %s has gid %q: %w", name, g.Gid, err)
	}

	return gid, nil
}

// owners is the uid and gid each entry of a restore is given, looking up
// every recorded name once.
type owners struct {
	accounts accounts
	recorded bool
	uid, gid int

	mtx    sync.Mutex
	users  map[string]int
	groups map[string]int
}

func newOwners(o Owner, a accounts) (*owners, error) {
	res := &owners{accounts: a, recorded: o.Recorded, users: map[string]int{}, groups: map[string]int{}}

	if o.Recorded {
		if o.User != "" || o.Group != "" {
			return nil, errors.New("an owner is either recorded or an account")
		}

		return res, nil
	}

	var err error
	if res.uid, res.gid, err = a.lookupUser(o.User); err != nil {
		return nil, fmt.Errorf("owner: %w", err)
	}

	if o.Group != "" {
		if res.gid, err = a.lookupGroup(o.Group); err != nil {
			return nil, fmt.Errorf("owner: %w", err)
		}
	}

	return res, nil
}

// of is the uid and gid stat's entry is given; ok is false when its
// recorded user or group is empty or has no account here.
func (o *owners) of(stat *proto.FileInfo) (uid, gid int, ok bool, err error) {
	if !o.recorded {
		return o.uid, o.gid, true, nil
	}

	o.mtx.Lock()
	defer o.mtx.Unlock()

	uid, err = o.id(o.users, string(stat.GetUser()), func(name string) (int, error) {
		uid, _, err := o.accounts.lookupUser(name)
		return uid, err
	})
	if err != nil {
		return 0, 0, false, err
	}

	gid, err = o.id(o.groups, string(stat.GetGroup()), o.accounts.lookupGroup)
	if err != nil {
		return 0, 0, false, err
	}

	return uid, gid, uid >= 0 && gid >= 0, nil
}

// id is name's id from cache or lookup, -1 for a name with no account.
func (o *owners) id(cache map[string]int, name string, lookup func(string) (int, error)) (int, error) {
	if name == "" {
		return -1, nil
	}

	if id, ok := cache[name]; ok {
		return id, nil
	}

	id, err := lookup(name)

	var noUser user.UnknownUserError
	var noGroup user.UnknownGroupError

	if errors.As(err, &noUser) || errors.As(err, &noGroup) {
		id, err = -1, nil
	}

	if err != nil {
		return 0, err
	}

	cache[name] = id

	return id, nil
}

// resolveOwners checks that the tree can take the owner Owner says and
// looks up its account.
func (r *Restorer) resolveOwners() (*owners, error) {
	if r.Owner == (Owner{}) {
		return nil, nil
	}

	fsys := r.fs()
	if _, ok := fsys.(OwnerWriteFS); !ok {
		if isLocalWrite(fsys) {
			return nil, errors.New("ownership is a Unix restore option")
		}

		return nil, fmt.Errorf("%T cannot hold owners", fsys)
	}

	if isLocalWrite(fsys) && os.Geteuid() != 0 {
		return nil, errors.New("giving restored entries an owner needs root")
	}

	return newOwners(r.Owner, r.accounts)
}

// putOwner gives path the owner Owner says. It comes before the mode is
// set, since a chown clears the setuid and setgid bits.
func (r *Restorer) putOwner(ctx context.Context, fsys WriteFS, path string, stat *proto.FileInfo) error {
	if r.owners == nil {
		return nil
	}

	uid, gid, ok, err := r.owners.of(stat)
	if err != nil {
		return err
	}

	if !ok {
		atomic.AddInt64(&r.stats.OwnersLeftOut, 1)
		return nil
	}

	if err := fsys.(OwnerWriteFS).Lchown(ctx, path, uid, gid); err != nil {
		return err
	}

	atomic.AddInt64(&r.stats.Owned, 1)

	return nil
}
