// ==============================================================================
// Author : Mohamed Tanveer (tanveer.munavar@gmail.com)
// Description : script to replicate ddl changes from source to target in postgresl
// ==============================================================================

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

func main() {

	// arguments passed in command line - source
	sHost := flag.String("shost", "", "mention the host name/ip")
	sPort := flag.Int("sport", 5432, "mention the port")
	sDatabase := flag.String("sdatabase", "", "mention the database name")
	sUser := flag.String("suser", "flyway_prod", "mention the user name")

	// arguments passed in command line - target
	tHost := flag.String("thost", "", "mention the host name/ip")
	tPort := flag.Int("tport", 5432, "mention the port")
	tDatabase := flag.String("tdatabase", "", "mention the database name")
	tUser := flag.String("tuser", "flyway_prod", "mention the user name")

	// parse the arguments passed in the command line
	flag.Parse()

	// Creating the connection string
	sPsqlInfo := fmt.Sprintf("host=%s port=%d dbname=%s user=%s sslmode=require", *sHost, *sPort, *sDatabase, *sUser)
	tPsqlInfo := fmt.Sprintf("host=%s port=%d dbname=%s user=%s sslmode=require", *tHost, *tPort, *tDatabase, *tUser)

	// validates the connection to our database
	sDb, err := sql.Open("postgres", sPsqlInfo)
	// error out if the output is not null
	if err != nil {
		panic(err)
	}
	defer sDb.Close()

	// test connection
	err = sDb.Ping()
	// error out if the output is not null
	if err != nil {
		panic(err)
	}

	// print message
	// fmt.Println("Source Connection Successful!")

	// validates the connection to our database
	tDb, err := sql.Open("postgres", tPsqlInfo)
	// error out if the output is not null
	if err != nil {
		panic(err)
	}
	defer tDb.Close()

	// test connection
	err = tDb.Ping()
	// error out if the output is not null
	if err != nil {
		panic(err)
	}

	// print message
	// fmt.Println("Target Connection Successful!")

	// insert data into the table and return the serial id
	sqlStatement := `select id,query from admin.log_ddl_publication_tables`
	// exceute the insert query and store the id value in the serialId variable
	rows, err := sDb.Query(sqlStatement)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id, query string
		)

		// scan copies the value to the query var
		if err := rows.Scan(&id, &query); err != nil {
			log.Fatal(err)
		}

		// fmt.Println(query)

		// execute the ddl query in the target
		_, err := tDb.Query(query)
		if err != nil {
			log.Fatal(err)
		}

		// delete the row from the source
		sqlStatement = `DELETE FROM admin.log_ddl_publication_tables WHERE id = $1`
		_, err = sDb.Query(sqlStatement, id)
		if err != nil {
			log.Fatal(err)
		}
	}

	sDb.Close()
	tDb.Close()

}
