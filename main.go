package main

import (
	"fmt"
	"net/http"
)

const bucketName = "ecmwf-open-data"

func main() {
	http.HandleFunc("/api", singleQueryHandler)
	http.HandleFunc("/range", rangeQueryHandler)
	http.HandleFunc("/daterange", dateRangeQueryHandler)
	http.HandleFunc("/typhoon", typhonAPIHandler)
	port := ":8080"
	fmt.Printf("Listening on http://localhost%s\n", port)
	fmt.Printf("  - Single point API: /api\n")
	fmt.Printf("  - Range coord API:  /range\n")
	fmt.Printf("  - Date range API:   /daterange\n")
	fmt.Printf("  - Typhoon API: /typhoon\n")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		println(err)
	}
}
