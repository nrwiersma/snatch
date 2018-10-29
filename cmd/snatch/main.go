package main

import (
	"fmt"
	"os"
	"os/user"
	"path"
	"time"

	"gopkg.in/urfave/cli.v2"
	"gopkg.in/urfave/cli.v2/altsrc"
)

const (
	flagDbDsn = "db"

	flagResolution = "res"

	flagConfig = "config"
)

var version = "¯\\_(ツ)_/¯"

var flags = []cli.Flag{
	altsrc.NewStringFlag(&cli.StringFlag{
		Name:  flagDbDsn,
		Usage: "The Influx DSN for metrics creation",
	}),
	altsrc.NewDurationFlag(&cli.DurationFlag{
		Name:  flagResolution,
		Value: 10 * time.Second,
		Usage: "The time resolution of metrics",
	}),
	&cli.StringFlag{
		Name:  flagConfig,
		Value: "~/.snatch.yaml",
		Usage: "The YAML file to read config from.",
	},
}

func newYamlSourceFromFlagFunc(flagFileName string) func(context *cli.Context) (altsrc.InputSourceContext, error) {
	return func(context *cli.Context) (altsrc.InputSourceContext, error) {
		filePath := context.String(flagFileName)
		if filePath[0] == '~' {
			u, err  := user.Current()
			if err != nil {
				return nil, err
			}

			filePath = path.Join(u.HomeDir, filePath[1:])
		}

		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			return &altsrc.MapInputSource{}, nil
		}

		return altsrc.NewYamlSourceFromFile(filePath)
	}
}

func main() {
	app := cli.App{}
	app.Name = "snatch"
	app.Usage = "Reads l2met from stdin, sending them to the specified database"
	app.Version = version
	app.Before = altsrc.InitInputSourceWithContext(flags, newYamlSourceFromFlagFunc(flagConfig))
	app.Flags = flags
	app.Action = runReader

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
	}
}
