package main

import (
	"log"

	"github.com/urfave/cli/v2"
)

func newExportCommand() *cli.Command {
	var upload bool
	var tables *cli.StringSlice
	var machine, output string
	return &cli.Command{
		Name:        "export",
		Usage:       "Export tables",
		ArgsUsage:   "<db_path>",
		Description: "Export SQLite tables to parquet files",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:        "upload",
				Category:    "OPTIONAL:",
				Usage:       "If the exported table should be uploaded to Basin or not",
				DefaultText: "false",
				Destination: &upload,
				Value:       false,
			},
			&cli.StringSliceFlag{
				Name:        "tables",
				Category:    "OPTIONAL:",
				Usage:       "The tables you want to export",
				DefaultText: "empty",
				Destination: tables,
				Value:       nil,
			},
			&cli.StringFlag{
				Name:        "output",
				Aliases:     []string{"o"},
				Category:    "REQUIRED:",
				Usage:       "The path of the exported Parquet file on disk",
				DefaultText: ".",
				Destination: &output,
				Value:       ".",
			},
			&cli.StringFlag{
				Name:        "machine",
				Category:    "REQUIRED:",
				Usage:       "machine's hash",
				DefaultText: "empty",
				Destination: &machine,
				Value:       "",
			},
		},
		Action: func(cCtx *cli.Context) error {
			dbPath := cCtx.Args().First()

			db, err := NewSQLite(dbPath)
			if err != nil {
				log.Fatal(err)
			}

			sink := Sink(&MockSink{})
			if upload {
				sink = &BasinSink{
					provider: "http://34.106.97.87:8002",
					machine:  machine,
				}
			}

			tables := cCtx.StringSlice("tables")

			exporter := NewDatabaseExporter(db, sink, output)
			if len(tables) > 0 {
				if err := exporter.ExportTables(cCtx.Context, tables); err != nil {
					log.Fatal(err)
				}
				return nil
			}

			if err := exporter.ExportAll(cCtx.Context); err != nil {
				log.Fatal(err)
			}

			return nil
		},
	}
}
