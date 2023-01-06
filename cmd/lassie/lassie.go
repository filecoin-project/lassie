package main

import (
	"log"
	"os"

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
