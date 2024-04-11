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

	rows, err := db.QueryContext(context.Background(), "select external_link, name from read_parquet(['https://bafkreifc4vdzhoibzpaszxt3i3s4bi7uzpgjg52hgoo6xp4zm6uh4mt5de.ipfs.w3s.link/'])")
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
