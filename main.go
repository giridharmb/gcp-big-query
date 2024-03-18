package main

import (
	"github.com/gorilla/mux"
	"log"
	"mdata/httphandlers"
	"mdata/metadata"
	"net/http"
)

func main() {

	err := metadata.Initialize()
	if err != nil {
		log.Printf("error : %v", err.Error())
		return
	}
	// Init router
	r := mux.NewRouter()

	// Route handles & endpoints
	r.HandleFunc("/api/v1/bq/{projectId}/{dataSet}/{bigQueryTable}", httphandlers.FetchBQTableRows).Methods("GET")
	r.HandleFunc("/api/v1/bq/{projectId}/{dataSet}/{bigQueryTable}", httphandlers.InsertIntoBQTable).Methods("POST")

	// Start server
	log.Fatal(http.ListenAndServe(":9900", r))
}
