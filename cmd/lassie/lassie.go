package main

import (
	"log"
	"os"
	"time"

	"github.com/urfave/cli/v2"
)

func main() { os.Exit(main1()) }

func main1() int {
	app := &cli.App{
		Name:  "lassie",
		Usage: "Utility for retrieving content from the Filecoin network",
		Commands: []*cli.Command{
			{
				Name:   "fetch",
				Usage:  "fetch content from Filecoin",
				Action: Fetch,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:      "output",
						Aliases:   []string{"o"},
						Usage:     "The CAR file to write to",
						TakesFile: true,
					},
					&cli.DurationFlag{
						Name:    "timeout",
						Aliases: []string{"t"},
						Usage:   "Consider it an error after not receiving a response from a storage provider for this long",
						Value:   20 * time.Second,
					},
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Println(err)
		return 1
	}
	return 0
}
