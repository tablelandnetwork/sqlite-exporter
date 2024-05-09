package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go"
)

// Sink represents a destination of the exported file.
type Sink interface {
	Send(context.Context, string) error
}

// Exporter represents a kind of exporter (e.g. parquet, ...).
type Exporter interface {
	Export(context.Context, string, []Column, *sql.Rows, Sink) error
}

// ParquetExporter is an exporter that exports Parquet files.
type ParquetExporter struct {
	outputDir string
}

// NewParquetExporter creates new ParquetExporter.
func NewParquetExporter(outputDir string) *ParquetExporter {
	return &ParquetExporter{
		outputDir: outputDir,
	}
}

// Export exports a table to Parquet.
func (e *ParquetExporter) Export(ctx context.Context, table string, columns []Column, rows *sql.Rows, sink Sink) error {
	hasNext := false
	if rows.Next() {
		hasNext = true
	}

	if !hasNext {
		return nil
	}

	filename := fmt.Sprintf("%s/%s.parquet", e.outputDir, table)
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0o755)
	if err != nil {
		return fmt.Errorf("open file: %s", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Println(err)
		}
	}()

	rawBuffer := make([]sql.RawBytes, len(columns))
	scanCallArgs := make([]interface{}, len(rawBuffer))
	for i := range rawBuffer {
		scanCallArgs[i] = &rawBuffer[i]
	}

	fields := make([]reflect.StructField, len(columns))
	for i, column := range columns {
		var typ reflect.Type
		switch column.DataType {
		case "INT", "INTEGER":
			typ = reflect.TypeOf(int64(0))
		case "TEXT":
			typ = reflect.TypeOf("")
		case "BLOB":
			typ = reflect.TypeOf([]byte{})
		default:
			return fmt.Errorf("unknown column type %s from table %s of column %s", column.DataType, table, column.Name)
		}

		tag := fmt.Sprintf(`parquet:"%v"`, column.Name)
		if column.IsNullable {
			tag = fmt.Sprintf(`parquet:"%v,optional"`, column.Name)
		}

		fields[i] = reflect.StructField{
			Name: column.GoName(),
			Type: typ,
			Tag:  reflect.StructTag(tag),
		}
	}

	v := reflect.New(reflect.StructOf(fields))
	writer := parquet.NewGenericWriter[any](f, &parquet.WriterConfig{
		Schema: parquet.SchemaOf(v.Interface()),
	})

	for {
		if err := rows.Scan(scanCallArgs...); err != nil {
			return fmt.Errorf("failed to scan row: %s", err)
		}

		structValue := v.Elem()
		for i, arg := range scanCallArgs {
			field := structValue.FieldByName(columns[i].GoName())

			bytes, ok := arg.(*sql.RawBytes)
			if !ok {
				return fmt.Errorf("converting arg %s type to *sql.RawBytes", field.Kind())
			}
			// handle NULL
			if len(*bytes) == 0 {
				structValue.FieldByName(columns[i].GoName()).SetZero()
				continue
			}

			switch field.Kind() {
			case reflect.Int, reflect.Int64:
				i64, err := strconv.ParseInt(string(*bytes), 10, field.Type().Bits())
				if err != nil {
					return fmt.Errorf("converting arg type %T to a %s: %v", arg, field.Kind(), err)
				}
				structValue.FieldByName(columns[i].GoName()).SetInt(i64)
			case reflect.String:
				structValue.FieldByName(columns[i].GoName()).SetString(string(*bytes))
			case reflect.Slice:
				structValue.FieldByName(columns[i].GoName()).SetBytes(*bytes)
			default:
				return fmt.Errorf("unknown destination type %s", field.Kind())
			}
		}
		if _, err := writer.Write([]any{structValue.Interface()}); err != nil {
			return fmt.Errorf("failed to write row: %s", err)
		}

		hasNext = rows.Next()
		if !hasNext {
			break
		}
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close writer: %s", err)
	}

	if err := sink.Send(ctx, filename); err != nil {
		return fmt.Errorf("send to sink: %s", err)
	}

	return nil
}

// DatabaseExporter represents an exports of the entire database.
type DatabaseExporter struct {
	db     *SQLite
	sink   Sink
	output string
}

// NewDatabaseExporter creates a new DatabaseExporter.
func NewDatabaseExporter(db *SQLite, sink Sink, output string) *DatabaseExporter {
	return &DatabaseExporter{
		db:     db,
		sink:   sink,
		output: output,
	}
}

// ExportTables exports specific tables of a database.
func (de *DatabaseExporter) ExportTables(ctx context.Context, tables []string) error {
	return de.export(ctx, tables)
}

// ExportAll exports all tables of a database.
func (de *DatabaseExporter) ExportAll(ctx context.Context) error {
	return de.export(ctx, []string{})
}

func (de *DatabaseExporter) export(ctx context.Context, tables []string) error {
	iter, err := de.db.GetTablesIterator(ctx, tables)
	if err != nil {
		return fmt.Errorf("get tables iterato: %s", err)
	}
	defer func() {
		if err := iter.Close(); err != nil {
			log.Println(err)
		}
	}()

	for {
		table, hasNext := iter.Next()
		if !hasNext {
			break
		}

		tableExporter := NewTableExporter(de.db, table, de.output, de.sink)
		if err := tableExporter.Execute(ctx); err != nil {
			log.Println(err)
		}
	}

	return nil
}

// TableExporter represents an exporter of a single table.
type TableExporter struct {
	db       *SQLite
	table    string
	exporter Exporter
	sink     Sink
}

// NewTableExporter creates new TableExporter.
func NewTableExporter(db *SQLite, table string, output string, sink Sink) *TableExporter {
	parquet := NewParquetExporter(output)

	return &TableExporter{
		db:       db,
		table:    table,
		exporter: parquet,
		sink:     sink,
	}
}

// Execute executes the exportation process.
func (te *TableExporter) Execute(ctx context.Context) error {
	columns, err := te.db.GetColumnsByTable(ctx, te.table)
	if err != nil {
		return fmt.Errorf("get columns by table: %s", err)
	}

	rows, err := te.db.GetRowsByTable(ctx, te.table)
	if err != nil {
		return fmt.Errorf("get rows by table: %s", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Println(err)
		}
	}()

	if err := te.exporter.Export(ctx, te.table, columns, rows, te.sink); err != nil {
		return fmt.Errorf("export: %s", err)
	}

	return nil
}

// MockSink represents a mocked sink.
type MockSink struct{}

// Send does nothing.
func (mock *MockSink) Send(_ context.Context, _ string) error {
	return nil
}

// BasinSink represents a sink that sends file to Basin.
type BasinSink struct {
	provider string
	machine  string
}

// Send sends the exported file to Basin.
func (s *BasinSink) Send(ctx context.Context, filepath string) error {
	f, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Println(err)
		}
	}()

	fi, err := f.Stat()
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/v1/os/%s/%s", s.provider, s.machine, strings.TrimSuffix(path.Base(f.Name()), ".parquet"))
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPut,
		url,
		f,
	)
	if err != nil {
		return fmt.Errorf("could not create request: %s", err)
	}

	req.Header.Add("X-ADM-BroadcastMode", "commit")

	req.ContentLength = fi.Size()

	client := &http.Client{
		Timeout: 0,
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request to write vault event failed: %s", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	return nil
}
