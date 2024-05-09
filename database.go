package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/jmoiron/sqlx"
)

// SQLite represents a SQLite Database.
type SQLite struct {
	db *sqlx.DB
}

// NewSQLite creates a new SQLite object.
func NewSQLite(path string) (*SQLite, error) {
	db, err := sqlx.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	return &SQLite{db}, nil
}

// GetTablesIterator returns an iterator of tables.
func (s *SQLite) GetTablesIterator(ctx context.Context, tables []string) (TablesIter, error) {
	sql := `SELECT name AS table_name
				FROM sqlite_master
				WHERE type = 'table'
				AND name NOT LIKE 'sqlite?_%' escape '?' 
				AND name NOT LIKE 'system?_%' escape '?'`
	if len(tables) > 0 {
		t := make([]string, len(tables))
		for i, table := range tables {
			t[i] = fmt.Sprintf("'%s'", table)
		}
		sql = sql + fmt.Sprintf(" AND name IN (%s)", strings.Join(t, ","))
	}

	rows, err := s.db.QueryContext(ctx, sql)
	if err != nil {
		return TablesIter{}, fmt.Errorf("query tables: %s", err)
	}

	return TablesIter{rows}, nil
}

// GetRowsByTable returns an iterators of all rows of a specific table.
func (s *SQLite) GetRowsByTable(ctx context.Context, table string) (*sql.Rows, error) {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s", table))
	if err != nil {
		return nil, fmt.Errorf("query tables: %s", err)
	}

	return rows, nil
}

// GetColumnsByTable returns the columns of a table.
func (s *SQLite) GetColumnsByTable(ctx context.Context, table string) ([]Column, error) {
	type column struct {
		CID          int            `db:"cid"`
		Name         string         `db:"name"`
		DataType     string         `db:"type"`
		NotNull      int            `db:"notnull"`
		DefaultValue sql.NullString `db:"dflt_value"`
		PrimaryKey   int            `db:"pk"`
		Hidden       int            `db:"hidden"`
	}

	rows, err := s.db.QueryxContext(ctx, fmt.Sprintf("SELECT * FROM PRAGMA_TABLE_XINFO('%s')", table))
	if err != nil {
		return nil, fmt.Errorf("query tables: %s", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Println(err)
		}
	}()

	columns := []Column{}
	for rows.Next() {
		var col column
		if err = rows.StructScan(&col); err != nil {
			return []Column{}, err
		}

		isNullable := true
		if col.NotNull == 1 {
			isNullable = false
		}

		isPrimaryKey := ""
		if col.PrimaryKey == 1 {
			isPrimaryKey = "PK"
		}

		columns = append(columns, Column{
			OrdinalPosition: col.CID,
			Name:            col.Name,
			DataType:        col.DataType,
			DefaultValue:    col.DefaultValue,
			IsNullable:      isNullable,
			ColumnKey:       isPrimaryKey,
		})
	}

	return columns, nil
}

// TablesIter represents an iterator of tables.
type TablesIter struct {
	rows *sql.Rows
}

// Next returns the next table of the iterator.
func (i *TablesIter) Next() (string, bool) {
	hasNext := i.rows.Next()
	if hasNext {
		var table string
		if err := i.rows.Scan(&table); err != nil {
			log.Print(err)
		}
		return table, true
	}

	return "", false
}

// Close closes the table's iterator.
func (i *TablesIter) Close() error {
	return i.rows.Close()
}

// Column represents information of a column of a database's table.
type Column struct {
	OrdinalPosition int            `db:"ordinal_position"`
	Name            string         `db:"column_name"`
	DataType        string         `db:"data_type"`
	DefaultValue    sql.NullString `db:"column_default"`
	IsNullable      bool           `db:"is_nullable"`
	ColumnKey       string         `db:"column_key"` // mysql specific
}

// GoName returns the name of the column in Golang's format.
func (c *Column) GoName() string {
	return camelCaseString(c.Name)
}

func camelCaseString(s string) string {
	if s == "" {
		return s
	}

	splitted := strings.Split(s, "_")

	if len(splitted) == 1 {
		return caser.String(s)
	}

	var cc string
	for _, part := range splitted {
		cc += caser.String(strings.ToLower(part))
	}
	return cc
}
