# SQLite Exporter

Exports SQLite tables to Basin. This tool is used to export Tableland tables to Basin.

## How it works?

Basin Exporter scans a SQLite database, export the tables to Parquet files, and upload them to Basin.
In order to export the tables to Parquet files, you must first generate the Go structs for each one of the tables. You do that by running

```bash
go run . generate [DB_PATH]
```

This will genenerate the structs in `schemas.go`.
Then you build a shared object out of that file by running

```bash
go build -buildmode=plugin -o schemas.so schemas.go
```

Now, you can export the all tables to Parquet by running

```bash
go run . export [DB_PATH]
```

This command does not upload the files to Basin. It exports Parquet files inside the `output` directory. You can also, choose specific tables to export by using the `tables` flag.

To export and push the files to Basin you must provide your private key and the vault's name, together with the `upload` flag.

```bash
go run . export --upload --basin-private-key=[PRIVATE_KEY] --basin-vault=[VAULT_NAME] [DB_PATH]
```

## Issues

- You have to generate the struct schemas. Gotta figure out a way of exporting without doing that.
- How to deal with tables that has GENERATED COLUMNS?