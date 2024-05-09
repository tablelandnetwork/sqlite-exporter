package main

import (
	"log"
	"os"

	_ "github.com/mattn/go-sqlite3"
	"github.com/urfave/cli/v2"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

var caser = cases.Title(language.English, cases.NoLower)

var version = "dev"

func main() {
	cliApp := &cli.App{
		Name:    "sqlite-exporter",
		Usage:   "Export SQLite tables to Parquet",
		Version: version,
		Commands: []*cli.Command{
			newExportCommand(),
		},
	}

	if err := cliApp.Run(os.Args); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
