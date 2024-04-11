package main

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"plugin"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/parquet-go/parquet-go"
	"github.com/tablelandnetwork/basin-cli/pkg/signing"
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
	provider   string
	vault      string
	privateKey *ecdsa.PrivateKey
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

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		fmt.Sprintf("%s/vaults/%s/events", s.provider, s.vault),
		f,
	)
	if err != nil {
		return fmt.Errorf("could not create request: %s", err)
	}

	req.Header.Add("filename", f.Name())

	signer := signing.NewSigner(s.privateKey)
	signatureBytes, err := signer.SignFile(filepath)
	if err != nil {
		return fmt.Errorf("signing the file: %s", err)
	}
	signature := hex.EncodeToString(signatureBytes)

	q := req.URL.Query()
	q.Add("timestamp", fmt.Sprint(time.Now().UTC().Unix()))
	q.Add("signature", fmt.Sprint(signature))
	req.URL.RawQuery = q.Encode()
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

	if resp.StatusCode != http.StatusCreated {
		type response struct {
			Error string
		}
		var r response
		if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
			return fmt.Errorf("failed to decode response: %s", err)
		}

		return fmt.Errorf(r.Error)
	}

	return nil
}
