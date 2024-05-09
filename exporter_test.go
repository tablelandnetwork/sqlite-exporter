package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/stretchr/testify/require"
)

func TestTableExporter(t *testing.T) {
	// setup database initial state
	setup := &DatabaseSetup{
		t: t,
	}
	setup.Open()
	setup.Exec("CREATE TABLE test (a_id INT, b TEXT, c BLOB)")
	setup.Exec("INSERT INTO test (a_id, b, c) VALUES (1, 'Hello 1', X'010203'), (2, 'Hello 2', null)")
	defer setup.Close()

	assertion := &sinkAssertion{
		t,
		[]struct {
			aid int64
			b   string
			c   []byte
		}{
			{
				1, "Hello 1", []byte{0x01, 0x02, 0x03},
			},
			{
				2, "Hello 2", nil,
			},
		},
		false,
	}
	// create table exporter
	exporter := NewTableExporter(setup.sqlite, "test", t.TempDir(), assertion)

	err := exporter.Execute(context.Background())
	require.NoError(t, err)
	require.True(t, assertion.isCalled)
}

func TestDatabaseExporter(t *testing.T) {
	// setup database initial state
	setup := &DatabaseSetup{
		t: t,
	}
	setup.Open()
	setup.Exec("CREATE TABLE test (a_id INT, b TEXT, c BLOB)")
	setup.Exec("INSERT INTO test (a_id, b, c) VALUES (1, 'Hello 1', X'010203'), (2, 'Hello 2', null)")
	defer setup.Close()

	assertion := &sinkAssertion{
		t,
		[]struct {
			aid int64
			b   string
			c   []byte
		}{
			{
				1, "Hello 1", []byte{0x01, 0x02, 0x03},
			},
			{
				2, "Hello 2", nil,
			},
		},
		false,
	}
	// create database exporter
	exporter := NewDatabaseExporter(setup.sqlite, assertion, t.TempDir())

	err := exporter.ExportAll(context.Background())
	require.NoError(t, err)
	require.True(t, assertion.isCalled)
}

// This sink implementation opens the exported file from disk using DuckDB
// and read it using read_parquet and assert the data inside the file.
type sinkAssertion struct {
	t    *testing.T
	data []struct {
		aid int64
		b   string
		c   []byte
	}
	isCalled bool
}

func (s *sinkAssertion) Send(ctx context.Context, filepath string) error {
	s.isCalled = true

	db, err := sql.Open("duckdb", "")
	require.NoError(s.t, err)
	defer func() {
		require.NoError(s.t, db.Close())
	}()

	rows, err := db.QueryContext(ctx, fmt.Sprintf("select * from read_parquet(['%s'])", filepath))
	require.NoError(s.t, err)
	defer func() {
		require.NoError(s.t, rows.Close())
	}()

	i := 0
	for rows.Next() {
		var aid int64
		var b string
		var c []byte
		err := rows.Scan(&aid, &b, &c)

		require.NoError(s.t, err)
		require.Equal(s.t, aid, s.data[i].aid)
		require.Equal(s.t, b, s.data[i].b)
		require.Equal(s.t, c, s.data[i].c)
		i++
	}

	return nil
}

type DatabaseSetup struct {
	t      *testing.T
	sqlite *SQLite
}

func (db *DatabaseSetup) Open() {
	f, err := os.CreateTemp("", "")
	if err != nil {
		db.t.Fatal(err)
	}
	database, err := sqlx.Open("sqlite3", f.Name())
	if err != nil {
		db.t.Fatal(err)
	}

	db.sqlite = &SQLite{
		db: database,
	}
}

func (db *DatabaseSetup) Exec(sql string) {
	if _, err := db.sqlite.db.Exec(sql); err != nil {
		db.t.Fatal(err)
	}
}

func (db *DatabaseSetup) Close() {
	if err := db.sqlite.db.Close(); err != nil {
		db.t.Fatal(err)
	}
}
