package main

import (
	"context"
	"log"
	"os"
	"text/template"
	"unicode"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/urfave/cli/v2"
)

func newGenerateCommand() *cli.Command {
	return &cli.Command{
		Name:        "generate",
		Usage:       "Generate structs",
		ArgsUsage:   "<db_path>",
		Description: "Generate structs for each one of the SQLite table",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:        "tables",
				Category:    "OPTIONAL:",
				Usage:       "The tables you want to export",
				DefaultText: "empty",
			},
		},
		Action: func(cCtx *cli.Context) error {
			dbPath := cCtx.Args().First()

			db, err := NewSQLite(dbPath, cCtx.StringSlice("tables"))
			if err != nil {
				log.Fatal(err)
			}

			templateData := TemplateData{
				Structs: make([]Struct, 0),
				Tables:  make([]string, 0),
			}

			iter, err := db.GetTables(context.Background())
			if err != nil {
				log.Fatal(err)
			}
			defer iter.Close()
			for {
				table, hasNext := iter.Next()
				if !hasNext {
					break
				}

				columns, err := db.GetColumnsByTable(context.Background(), table)
				if err != nil {
					log.Print(err)
				}

				if !unicode.IsLetter(rune(table[0])) {
					table = "X" + table
				}

				templateData.Tables = append(templateData.Tables, table)
				templateData.Structs = append(templateData.Structs, Struct{
					Table:  table,
					Schema: columns,
				})
			}

			funcMap := template.FuncMap{
				"CamelCase": camelCaseString,
			}

			tmpl, _ := template.New("schemas.tmpl").Funcs(funcMap).ParseFiles("schemas.tmpl")

			f, err := os.OpenFile("schemas.go", os.O_CREATE|os.O_RDWR, 0x666)
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()

			if err := tmpl.Execute(f, templateData); err != nil {
				log.Fatal(err)
			}

			return nil
		},
	}
}

func newExportCommand() *cli.Command {
	var upload bool
	var tables *cli.StringSlice
	var privateKey, vault string
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
				Name:        "basin-private-key",
				Category:    "REQUIRED:",
				Usage:       "Basin's private key",
				DefaultText: "empty",
				Destination: &privateKey,
				Value:       "",
			},
			&cli.StringFlag{
				Name:        "basin-vault",
				Category:    "REQUIRED:",
				Usage:       "Basin's vault",
				DefaultText: "empty",
				Destination: &vault,
				Value:       "",
			},
		},
		Action: func(cCtx *cli.Context) error {
			dbPath := cCtx.Args().First()

			db, err := NewSQLite(dbPath, cCtx.StringSlice("tables"))
			if err != nil {
				log.Fatal(err)
			}

			privateKey, err := crypto.HexToECDSA(privateKey)
			if err != nil {
				log.Fatal(err)
			}
			sink := Sink(&MockSink{})
			if upload {
				sink = &BasinSink{
					provider:   "https://basin.tableland.xyz",
					vault:      vault,
					privateKey: privateKey,
				}
			}

			// TODO: make them configurable
			pool := NewPool(10, 1000)
			pool.Start(context.Background())

			iter, err := db.GetTables(context.Background())
			if err != nil {
				log.Fatal(err)
			}
			defer iter.Close()

			for {
				table, hasNext := iter.Next()
				if !hasNext {
					break
				}

				task, err := NewTableExporter(db, table, sink)
				if err != nil {
					log.Print(err)
				}
				pool.AddTask(task)
			}

			pool.Close()

			return nil
		},
	}
}
