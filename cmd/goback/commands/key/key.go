// Package key manages the client-held store key and a store's at-rest key.
package key

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/storage/pack"

	"github.com/urfave/cli"
)

// Command is the key command.
var Command = cli.Command{
	Name:  "key",
	Usage: "Manage the store key that encrypts names and contents before upload, and the at-rest key",
	Subcommands: []cli.Command{
		{
			Name:      "new",
			Usage:     "Generate a store key file",
			ArgsUsage: "<name>",
			Action:    newAction,
			Flags:     []cli.Flag{outFlag("store.key")},
		},
		{
			Name:   "escrow",
			Usage:  "Encrypt a store key with a passphrase into an armored age file, which `age -d` can also open; print it, or keep it with the store using --upload",
			Action: escrowAction,
			Flags: []cli.Flag{
				cli.StringFlag{Name: "key", Usage: "key file to escrow", Value: "store.key"},
				passphraseFlag,
				passphraseFileFlag,
				cli.BoolFlag{Name: "upload", Usage: "keep the escrowed key with the --storage store instead of printing it"},
			},
		},
		{
			Name:   "id",
			Usage:  "Print a store key's id; the store and the key's escrowed copies refer to the key by this id",
			Action: idAction,
			Flags:  []cli.Flag{cli.StringFlag{Name: "key", Usage: "key file", Value: "store.key"}},
		},
		{
			Name:   "recover",
			Usage:  "Rebuild a key file from an escrowed key and its passphrase",
			Action: recoverAction,
			Flags: []cli.Flag{
				cli.StringFlag{Name: "escrow", Usage: "escrowed key file, as printed by key escrow; - reads standard input", Value: "-"},
				outFlag("store.key"),
				passphraseFlag,
				passphraseFileFlag,
			},
		},
		{
			Name:   "at-rest",
			Usage:  "Generate or rotate the key that seals the archives of a local, gcs:// or s3:// store",
			Action: atRestAction,
			Flags: []cli.Flag{
				outFlag("at-rest.key"),
				cli.BoolFlag{Name: "rotate", Usage: "add a new primary key to the file; the older keys still open the archives they sealed"},
			},
		},
	},
}

func outFlag(value string) cli.StringFlag {
	return cli.StringFlag{Name: "out", Usage: "key file to write", Value: value}
}

var passphraseFlag = cli.StringFlag{
	Name:   "passphrase",
	Usage:  "escrow passphrase",
	EnvVar: "GOBACK_PASSPHRASE",
}

var passphraseFileFlag = cli.StringFlag{
	Name:   "passphrase-file",
	Usage:  "file holding the escrow passphrase; used when --passphrase is not set",
	EnvVar: "GOBACK_PASSPHRASE_FILE",
}

func passphrase(c *cli.Context) (string, error) {
	p, err := common.Passphrase(c.String("passphrase"), c.String("passphrase-file"))
	if err != nil {
		return "", err
	}

	if p == "" {
		return "", errors.New("a passphrase is required (--passphrase, --passphrase-file, GOBACK_PASSPHRASE or GOBACK_PASSPHRASE_FILE)")
	}

	return p, nil
}

// storeName is the store name the command was given.
func storeName(c *cli.Context) (string, error) {
	if c.NArg() != 1 || c.Args().First() == "" {
		return "", fmt.Errorf("usage: key %s <name>", c.Command.Name)
	}

	return c.Args().First(), nil
}

// fresh refuses to replace an existing key file.
func fresh(path string) error {
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("%s exists, refusing to overwrite a key", path)
	}

	return nil
}

// write saves the key under --out.
func write(c *cli.Context, key *storekey.Key, verb string) error {
	out := c.String("out")

	err := key.Save(out)
	if err != nil {
		return err
	}

	common.Result(keyWritten{Action: verb, KeyID: key.IDString(), Store: key.Name, Path: out}, func() {
		fmt.Printf("%s key %s for store %s to %s\n", verb, key.IDString(), key.Name, out)
	})

	return nil
}

// keyWritten is a key a command saved, as JSON output shows it.
type keyWritten struct {
	Action string `json:"action"`
	KeyID  string `json:"key_id,omitempty"`
	Store  string `json:"store,omitempty"`
	Path   string `json:"path"`
}

func newAction(c *cli.Context) error {
	name, err := storeName(c)
	if err != nil {
		return err
	}

	key, err := storekey.Generate(name)
	if err != nil {
		return err
	}

	return write(c, key, "wrote")
}

func escrowAction(c *cli.Context) error {
	pass, err := passphrase(c)
	if err != nil {
		return err
	}

	key, err := storekey.Load(c.String("key"))
	if err != nil {
		return err
	}

	escrowed, err := key.Escrow(pass)
	if err != nil {
		return err
	}

	if c.Bool("upload") {
		return upload(c, key.IDString(), escrowed)
	}

	if common.JSON() {
		common.Result(struct {
			KeyID    string `json:"key_id"`
			Escrowed string `json:"escrowed"`
		}{key.IDString(), string(escrowed)}, nil)

		return nil
	}

	_, err = os.Stdout.Write(escrowed)

	return err
}

// upload keeps the escrowed key with the store --storage names.
func upload(c *cli.Context, keyID string, escrowed []byte) error {
	repo := common.OpenRepo(c)
	store := repo.Store
	defer common.CloseRepo(repo)

	escrow, ok := store.(backup.KeyEscrow)
	if !ok {
		return fmt.Errorf("store %T keeps no escrowed key", store)
	}

	err := escrow.PutEscrowedKey(common.Context(c), backup.EscrowedKey{KeyID: keyID, Escrowed: escrowed})
	if err != nil {
		return fmt.Errorf("the store refused the escrowed key: %w", err)
	}

	common.Result(struct {
		Action string `json:"action"`
		KeyID  string `json:"key_id"`
	}{"uploaded", keyID}, func() {
		fmt.Printf("the store keeps key %s escrowed; start agents with GOBACK_PASSPHRASE and no --store-key\n", keyID)
	})

	return nil
}

func idAction(c *cli.Context) error {
	key, err := storekey.Load(c.String("key"))
	if err != nil {
		return err
	}

	if common.JSON() {
		common.Result(struct {
			KeyID string `json:"key_id"`
			Store string `json:"store"`
		}{key.IDString(), key.Name}, nil)

		return nil
	}

	_, err = fmt.Fprintln(c.App.Writer, key.IDString())

	return err
}

func recoverAction(c *cli.Context) error {
	pass, err := passphrase(c)
	if err != nil {
		return err
	}

	var escrowed []byte
	if path := c.String("escrow"); path == "-" {
		escrowed, err = io.ReadAll(os.Stdin)
	} else {
		escrowed, err = os.ReadFile(path)
	}

	if err != nil {
		return fmt.Errorf("reading the escrowed key: %w", err)
	}

	key, err := storekey.Recover(escrowed, pass)
	if err != nil {
		return err
	}

	return write(c, key, "recovered")
}

func atRestAction(c *cli.Context) error {
	out := c.String("out")

	if !c.Bool("rotate") {
		if err := fresh(out); err != nil {
			return err
		}

		key, err := pack.GenerateAtRestKey()
		if err != nil {
			return err
		}

		err = key.Save(out)
		if err != nil {
			return err
		}

		common.Result(keyWritten{Action: "wrote", KeyID: fmt.Sprintf("%x", key.ID()), Path: out}, func() {
			fmt.Printf("wrote at-rest key %x to %s\n", key.ID(), out)
		})

		return nil
	}

	key, err := pack.LoadAtRestKey(out)
	if err != nil {
		return err
	}

	rotated, err := key.Rotate()
	if err != nil {
		return err
	}

	err = rotated.Save(out)
	if err != nil {
		return err
	}

	common.Result(keyWritten{Action: "rotated", KeyID: fmt.Sprintf("%x", rotated.ID()), Path: out}, func() {
		fmt.Printf("rotated %s: records are sealed under %x from now on; a compaction re-seals the older ones\n", out, rotated.ID())
	})

	return nil
}
