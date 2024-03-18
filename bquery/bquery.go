package bquery

import (
	"cloud.google.com/go/bigquery"
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/api/option"
	"log"
	"mdata/metadata"
	"time"
)

// BQTableQuery
// Query struct{}
type BQTableQuery struct {
	ProjectID string
	BQDataSet string
	BQTable   string
}

// Record
// Represents a row in the BigQuery table.
// Important >>
// BigQuery Columns (make sure you create that in your BigQuery)
// scrape_ts -> is of type TIMESTAMP in BigQuery
// payload   -> is of type JSON in BigQuery
type Record struct {
	ScrapeTS bigquery.NullTimestamp `bigquery:"scrape_ts" json:"scrape_ts"`
	Payload  string                 `bigquery:"payload" json:"payload"`
}

func MapToRecord(payloadObj interface{}) (Record, error) {

	payloadBytes, err := json.Marshal(payloadObj)
	if err != nil {
		return Record{}, err
	}
	record := Record{
		ScrapeTS: bigquery.NullTimestamp{Timestamp: time.Now().UTC(), Valid: true},
		Payload:  string(payloadBytes), // `payload` is a JSON string
	}
	return record, nil
}

func (bqTableQuery BQTableQuery) FetchData() ([]interface{}, error) {
	result, err := QueryBQTable(bqTableQuery.ProjectID, bqTableQuery.BQDataSet, bqTableQuery.BQTable)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (bqTableQuery BQTableQuery) InsertData(records []*Record) error {
	err := InsertRecords(bqTableQuery.ProjectID, bqTableQuery.BQDataSet, bqTableQuery.BQTable, records)
	return err
}

func QueryBQTable(projectID string, datasetID string, tableID string) ([]interface{}, error) {
	result := make([]interface{}, 0)

	//iterationErr := false
	var iterErr error

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Printf("error : bigquery.NewClient: %v", err)
		return result, err
	}
	defer func() {
		_ = client.Close()
	}()

	bqQuery := fmt.Sprintf("SELECT * FROM `%s.%s.%s`", projectID, datasetID, tableID)

	query := client.Query(bqQuery)

	job, err := query.Run(ctx)
	if err != nil {
		log.Printf("error : query.Run: %v", err)
		return result, err
	}

	status, err := job.Wait(ctx)
	if err != nil {
		log.Printf("error : job.Wait: %v", err)
		return result, err
	}

	if err := status.Err(); err != nil {
		log.Printf("error : Query failed: %v", err)
		return result, err
	}

	it, err := job.Read(ctx)
	if err != nil {
		log.Fatalf("error : job.Read: %v", err)
		return result, err
	}

	schema := it.Schema
	var columnNames []string
	for _, fieldSchema := range schema {
		columnNames = append(columnNames, fieldSchema.Name)
	}
	fmt.Println("column names:", columnNames)

	// Iterate and print rows with column names
	for {
		var values []bigquery.Value
		iterErr = it.Next(&values)
		if iterErr != nil {
			log.Printf("iterErr : %v", iterErr.Error())
			break
		}
		rowMap := make(map[string]bigquery.Value)
		for i, value := range values {
			rowMap[columnNames[i]] = value
		}
		//fmt.Println(rowMap)
		result = append(result, rowMap)
	}
	return result, nil
}

func InsertRecords(projectID string, datasetID string, tableID string, records []*Record) error {
	ctx := context.Background()

	// Initialize BigQuery client
	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(metadata.CredentialsFile))
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	inserter := client.Dataset(datasetID).Table(tableID).Inserter()

	// Insert records
	if err := inserter.Put(ctx, records); err != nil {
		return fmt.Errorf("Inserter.Put failed: %v", err)
	}

	fmt.Println("successfully inserted multiple records.")
	return nil
}
