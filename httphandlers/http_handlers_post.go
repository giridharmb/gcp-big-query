package httphandlers

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"log"
	"mdata/bquery"
	"mdata/utils"
	"net/http"
)

func InsertIntoBQTable(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	msg := ""
	result := make(map[string]interface{})
	result["response"] = make(map[string]interface{})
	result["error"] = ""

	params := mux.Vars(r) // Get params

	projectId, ok := params["projectId"]
	if !ok {
		msg = fmt.Sprintf("error : missing 'projectId' in the URL")
		result["error"] = msg
		_ = json.NewEncoder(w).Encode(result)
		return
	}
	dataSet, ok := params["dataSet"]
	if !ok {
		msg = fmt.Sprintf("error : missing 'dataSet' in the URL")
		result["error"] = msg
		_ = json.NewEncoder(w).Encode(result)
		return
	}
	bigQueryTable, ok := params["bigQueryTable"]
	if !ok {
		msg = fmt.Sprintf("error : missing 'bigQueryTable' in the URL")
		result["error"] = msg
		_ = json.NewEncoder(w).Encode(result)
		return
	}

	// ---------------->> check for validity | start -------------
	if !utils.IsValidString(projectId) {
		msg = fmt.Sprintf("error : invalid characters string 'projectId'")
		result["error"] = msg
		_ = json.NewEncoder(w).Encode(result)
		return
	}

	if !utils.IsValidString(dataSet) {
		msg = fmt.Sprintf("error : invalid character 'dataSet' in the URL")
		result["error"] = msg
		_ = json.NewEncoder(w).Encode(result)
		return
	}

	if !utils.IsValidString(bigQueryTable) {
		msg = fmt.Sprintf("error : invalid characters 'bigQueryTable' in the URL")
		result["error"] = msg
		_ = json.NewEncoder(w).Encode(result)
		return
	}
	// ---------------->> check for validity | end -------------

	bqTableQuery := bquery.BQTableQuery{
		ProjectID: projectId,
		BQDataSet: dataSet,
		BQTable:   bigQueryTable,
	}

	var postData interface{}

	err := json.NewDecoder(r.Body).Decode(&postData)
	if err != nil {
		msg = fmt.Sprintf("error : could get post payload (json) : %v", err.Error())
		log.Printf("%v", msg)
		result["error"] = msg
		_ = json.NewEncoder(w).Encode(result)
		return
	}
	defer func() {
		_ = r.Body.Close()
	}()

	log.Printf("received data :%v", postData)

	dataItems, ok := postData.([]interface{})
	if !ok {
		msg = fmt.Sprintf("error : jsonData is not of type []interface{} (an array of items)")
		log.Printf("%v", msg)
		result["error"] = msg
		_ = json.NewEncoder(w).Encode(result)
		return
	}

	recordsToInsert := make([]*bquery.Record, 0)

	for _, dataItem := range dataItems {
		jsonData, err := bquery.MapToRecord(dataItem)
		if err != nil {
			msg = fmt.Sprintf("error : could convert (json) > postData : ( %v ) < to record : %v", postData, err.Error())
			log.Printf("%v", msg)
			result["error"] = msg
			_ = json.NewEncoder(w).Encode(result)
			return
		}
		recordsToInsert = append(recordsToInsert, &jsonData)
	}

	recordChunks := utils.SplitIntoChunks(recordsToInsert, 500)

	for index, records := range recordChunks {
		log.Printf("inserting records (index : %v) , (total length : %v)", index, len(records))
		err = bqTableQuery.InsertData(records)
		if err != nil {
			msg = fmt.Sprintf("error : could not insert data into BQ : %v", err.Error())
			log.Printf("%v", msg)
			result["error"] = msg
			_ = json.NewEncoder(w).Encode(result)
			return
		}
	}
}
