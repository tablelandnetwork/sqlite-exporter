package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/marcboeker/go-duckdb"
)

func main() {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("INSTALL https; LOAD https;")
	if err != nil {
		log.Fatal(err)
	}

	rows, err := db.QueryContext(context.Background(), "select *from read_parquet(['http://34.106.97.87:8002/v1/os/t2gh7m2iaqvwv2oexsaetncmdm6hhac7k6ne3teda/pilot_sessions_80001_7137'])")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var link, name string
		if err := rows.Scan(&link, &name); err != nil {
			log.Fatal(err)
		}

		fmt.Println(link, name)
	}
}
