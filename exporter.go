package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"plugin"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/parquet-go/parquet-go"
)

type Sink interface {
	Send(context.Context, string) error
}

type Exporter interface {
	Export(context.Context, string, *sqlx.Rows, Sink) error
}

type ParquetExporter struct {
	GetRowInstance func(table string) (interface{}, error)
}

func NewParquetExporter() (*ParquetExporter, error) {
	plugin, err := plugin.Open("schemas.so")
	if err != nil {
		return nil, err
	}

	symbol, err := plugin.Lookup("GetRowInstance")
	if err != nil {
		return nil, err
	}

	getRowInstance, ok := symbol.(func(table string) (interface{}, error))
	if !ok {
		return nil, err
	}

	return &ParquetExporter{
		GetRowInstance: getRowInstance,
	}, nil
}

func (e *ParquetExporter) Export(ctx context.Context, table string, rows *sqlx.Rows, sink Sink) error {
	hasNext := false
	if rows.Next() {
		hasNext = true
	}

	if !hasNext {
		return nil
	}

	filename := fmt.Sprintf("./output/%s.parquet", table)
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0o755)
	if err != nil {
		return fmt.Errorf("open file: %s", err)
	}
	defer f.Close()

	row, err := e.GetRowInstance(table)
	if err != nil {
		return fmt.Errorf("get row instance: %s", err)
	}

	writer := parquet.NewWriter(f)
	for {
		fmt.Println(table)

		if err := rows.StructScan(row); err != nil {
			log.Print(err)
		}

		if err == nil {
			if err := writer.Write(row); err != nil {
				log.Print(err)
			}
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

type TableExporter struct {
	db       *SQLite
	table    string
	exporter Exporter
	sink     Sink
}

func NewTableExporter(db *SQLite, table string, sink Sink) (*TableExporter, error) {
	parquet, err := NewParquetExporter()
	if err != nil {
		return nil, err
	}

	return &TableExporter{
		db:       db,
		table:    table,
		exporter: parquet,
		sink:     sink,
	}, nil
}

func (te *TableExporter) Execute(ctx context.Context, worker int) error {
	rows, err := te.db.GetRowsByTable(ctx, te.table)
	if err != nil {
		return fmt.Errorf("query: %s", err)
	}
	defer rows.Close()

	if err := te.exporter.Export(ctx, te.table, rows, te.sink); err != nil {
		return fmt.Errorf("export: %s", err)
	}

	return nil
}

type MockSink struct{}

func (mock *MockSink) Send(_ context.Context, _ string) error {
	return nil
}

type BasinSink struct {
	provider string
	machine  string
}

func (s *BasinSink) Send(ctx context.Context, filepath string) error {
	f, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer f.Close()

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

	fmt.Println(resp.StatusCode)

	return nil
}
