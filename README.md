# SQLite Exporter

Exports SQLite tables to Basin. This tool is used to export Tableland tables to Basin.

## How it works?

Basin Exporter scans a SQLite database, export the tables to Parquet files, and upload them to Basin.

Build it by running

```bash
make build
```

Now, export the all tables to Parquet by running

```bash
./sqlite-export export -o ./output [DB_PATH]
```

This command does not upload the files to Basin. It exports Parquet files inside the `output` directory. You can also, choose specific tables to export by using the `tables` flag.

To export and push the files to Basin you must the machine identifier, together with the `upload` flag.

```bash
go run . export --upload --machine=[HASH] [DB_PATH]
```
