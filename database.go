package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"unicode"

	"github.com/jmoiron/sqlx"
	"golang.org/x/exp/slices"
)

type SQLite struct {
	db     *sqlx.DB
	tables []string
}

func NewSQLite(path string, tables []string) (*SQLite, error) {
	db, err := sqlx.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	return &SQLite{db, tables}, nil
}

func (s *SQLite) GetTables(ctx context.Context) (TablesIter, error) {
	sql := `SELECT name AS table_name
				FROM sqlite_master
				WHERE type = 'table'
				AND name NOT LIKE 'sqlite?_%' escape '?' 
				AND name NOT LIKE 'system?_%' escape '?'`
	if len(s.tables) > 0 {
		tables := make([]string, len(s.tables))
		for i, t := range s.tables {
			tables[i] = fmt.Sprintf("'%s'", t)
		}
		sql = sql + fmt.Sprintf(" AND name IN (%s)", strings.Join(tables, ","))
	}

	rows, err := s.db.QueryxContext(ctx, sql)
	if err != nil {
		return TablesIter{}, fmt.Errorf("query tables: %s", err)
	}

	return TablesIter{rows}, nil
}

func (s *SQLite) GetRowsByTable(ctx context.Context, table string) (*sqlx.Rows, error) {
	rows, err := s.db.QueryxContext(ctx, fmt.Sprintf("SELECT * FROM %s", table))
	if err != nil {
		return nil, fmt.Errorf("query tables: %s", err)
	}

	return rows, nil
}

func (s *SQLite) GetColumnsByTable(ctx context.Context, table string) ([]Column, error) {
	type column struct {
		CID          int            `db:"cid"`
		Name         string         `db:"name"`
		DataType     string         `db:"type"`
		NotNull      int            `db:"notnull"`
		DefaultValue sql.NullString `db:"dflt_value"`
		PrimaryKey   int            `db:"pk"`
	}

	rows, err := s.db.QueryxContext(ctx, fmt.Sprintf("SELECT * FROM PRAGMA_TABLE_INFO('%s')", table))
	if err != nil {
		return nil, fmt.Errorf("query tables: %s", err)
	}
	defer rows.Close()

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

type TablesIter struct {
	rows *sqlx.Rows
}

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

func (i *TablesIter) Close() error {
	return i.rows.Close()
}

type Column struct {
	OrdinalPosition int            `db:"ordinal_position"`
	Name            string         `db:"column_name"`
	DataType        string         `db:"data_type"`
	DefaultValue    sql.NullString `db:"column_default"`
	IsNullable      bool           `db:"is_nullable"`
	ColumnKey       string         `db:"column_key"` // mysql specific
}

func (c *Column) IsInteger() bool {
	return slices.Contains([]string{
		"integer",
		"int",
	}, strings.ToLower(c.DataType))
}

func (c *Column) IsFloat() bool {
	return slices.Contains([]string{
		"real",
		"numeric",
	}, strings.ToLower(c.DataType))
}

func (s *Column) IsTemporal() bool {
	return false
}

type Formatted struct {
	text       string
	isNullable bool
	isTemporal bool
	tags       string
}

func (f Formatted) Text() string {
	return f.text
}

func (f Formatted) Tags() string {
	return f.tags
}

func (c *Column) Format() Formatted {
	var isNullable, isTemporal bool
	var goType string
	columnName := camelCaseString(c.Name)
	if !unicode.IsLetter(rune(columnName[0])) {
		columnName = "X" + columnName
	}

	if c.IsInteger() {
		goType = "int"
		if c.IsNullable {
			goType = "*int"
			isNullable = true
		}
	} else if c.IsFloat() {
		goType = "float64"
		if c.IsNullable {
			goType = "*float64"
			isNullable = true
		}
	} else if c.IsTemporal() {
		isTemporal = true
		if !c.IsNullable {
			goType = "time.Time"
		} else {
			goType = "*time.Time"
			isNullable = true
		}
	} else {
		switch c.DataType {
		case "boolean":
			goType = "bool"
			if c.IsNullable {
				goType = "*bool"
				isNullable = true
			}
		default:
			goType = "string"
			if c.IsNullable {
				goType = "*string"
				isNullable = true
			}
		}
	}

	return Formatted{
		text:       fmt.Sprintf("%s %s", columnName, goType),
		isNullable: isNullable,
		isTemporal: isTemporal,
		tags:       "`parquet:\"" + c.Name + "\" db:\"" + c.Name + "\"`",
	}
}
