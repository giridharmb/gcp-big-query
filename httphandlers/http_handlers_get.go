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

func FetchBQTableRows(w http.ResponseWriter, r *http.Request) {
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
		msg = fmt.Sprintf("error : invalid characters in 'projectId'")
		result["error"] = msg
		_ = json.NewEncoder(w).Encode(result)
		return
	}

	if !utils.IsValidString(dataSet) {
		msg = fmt.Sprintf("error : invalid characters in 'dataSet' in the URL")
		result["error"] = msg
		_ = json.NewEncoder(w).Encode(result)
		return
	}

	if !utils.IsValidString(bigQueryTable) {
		msg = fmt.Sprintf("error : invalid characters in 'bigQueryTable' in the URL")
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

	bqData, err := bqTableQuery.FetchData()
	if err != nil {
		msg = fmt.Sprintf("error : could not fetch data from BQ : %v", err.Error())
		log.Printf("%v", msg)
		result["error"] = msg
		_ = json.NewEncoder(w).Encode(result)
		return
	}
	_ = json.NewEncoder(w).Encode(bqData)
}
