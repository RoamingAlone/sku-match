package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

func main() {
	// Load environment variables from .env file
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Get database credentials from environment variables
	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	dbName := os.Getenv("DB_NAME")

	// Open the CSV file
	csvFile, err := os.Open("sku.csv")
	if err != nil {
		log.Fatal("Unable to open CSV file", err)
	}
	defer csvFile.Close()

	// Read the CSV file
	reader := csv.NewReader(csvFile)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal("Unable to read CSV file", err)
	}

	// Open a new CSV file to write the result
	outputFile, err := os.Create("matched.csv")
	if err != nil {
		log.Fatal("Unable to create output CSV file", err)
	}
	defer outputFile.Close()

	// Create a CSV writer
	writer := csv.NewWriter(outputFile)
	defer writer.Flush()

	// Set up the database connection using environment variables
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbUser, dbPassword, dbHost, dbPort, dbName))
	if err != nil {
		log.Fatal("Unable to connect to the database", err)
	}
	defer db.Close()

	// Query the SKUs from the database
	rows, err := db.Query("SELECT sku FROM store_product")
	if err != nil {
		log.Fatal("Unable to query database", err)
	}
	defer rows.Close()

	// Store the SKUs in a map for quick lookup
	dbSKUs := make(map[string]bool)
	for rows.Next() {
		var sku string
		err := rows.Scan(&sku)
		if err != nil {
			log.Fatal("Error scanning row", err)
		}
		dbSKUs[sku] = true
	}

	// Process CSV records and compare SKUs
	for i, record := range records {
		if i == 0 {
			// Add a header for the new column
			record = append(record, "RES SKU")
		} else {
			// Assume SKU is in the first column of the CSV (adjust if necessary)
			csvSKU := strings.TrimSpace(record[0])
			if dbSKUs[csvSKU] {
				// If SKU exists in the database, mark it as matched
				record = append(record, csvSKU)
			} else {
				// If no match, leave the column empty
				record = append(record, "")
			}
		}
		// Write the updated row to the output CSV
		writer.Write(record)
	}

	fmt.Println("Comparison complete. Output written to 'matched.csv'")
}
