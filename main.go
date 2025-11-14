package main

import (
	"fmt"
	"net/http"
)

const bucketName = "ecmwf-open-data"

func main() {
	http.HandleFunc("/api", singleQueryHandler)
	port := ":8080"
	fmt.Printf("Listening on http://localhost%s/api\n", port)
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		return
	}
}
