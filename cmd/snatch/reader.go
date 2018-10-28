package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/urfave/cli.v2"
)

func runReader(c *cli.Context) error {
	res := c.Duration(flagResolution)

	db, err := newDB(c.String(flagDbDsn))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer db.Close()

	store := newStore(res)

	app := newApplication(res, db, store)

	scan := time.NewTicker(res)
	defer scan.Stop()
	go func() {
		for range scan.C {
			if err := app.Flush(); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}
	}()

	err = app.Parse(os.Stdin, handleInvalidLine)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if err := app.Flush(); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}

	return nil
}

func handleInvalidLine(b []byte) {
	fmt.Fprint(os.Stdout, string(b))
}
