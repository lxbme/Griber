package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"strconv"
)

type SingleAPIParams struct {
	Lat   float64 `json:"lat"`
	Lon   float64 `json:"lon"`
	Date  string  `json:"date"`
	Batch string  `json:"batch"`
}

type SingleResponse struct {
	U       float64 `json:"u"`
	V       float64 `json:"v"`
	Status  int     `json:"status"`
	Success bool    `json:"success"`
}

var singleFailResponse = SingleResponse{
	U:       0,
	V:       0,
	Status:  http.StatusBadRequest,
	Success: false,
}

func sendSingleJsonError(w http.ResponseWriter, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode) // 写入HTTP状态码 (例如 400, 500)
	json.NewEncoder(w).Encode(singleFailResponse)
}

func singleQueryHandler(w http.ResponseWriter, r *http.Request) {
	httpQuery := r.URL.Query()

	latStr := httpQuery.Get("lat")
	if latStr == "" {
		// 如果参数丢失，发送一个 400 Bad Request 错误
		sendSingleJsonError(w, http.StatusBadRequest)
		return
	}
	lat, err := strconv.ParseFloat(latStr, 64)
	if err != nil {
		sendSingleJsonError(w, http.StatusBadRequest)
		return
	}

	lonStr := httpQuery.Get("lon")
	if lonStr == "" {
		sendSingleJsonError(w, http.StatusBadRequest)
		return
	}
	lon, err := strconv.ParseFloat(lonStr, 64)
	if err != nil {
		sendSingleJsonError(w, http.StatusBadRequest)
		return
	}

	date := httpQuery.Get("date")
	if date == "" {
		sendSingleJsonError(w, http.StatusBadRequest)
		return
	}

	batch := httpQuery.Get("batch")
	if batch == "" {
		sendSingleJsonError(w, http.StatusBadRequest)
		return
	}

	params := SingleAPIParams{
		Lat:   lat,
		Lon:   lon,
		Date:  date,
		Batch: batch,
	}

	// final respons
	data, err2 := SingleQuery(params)
	if err2 != nil {
		sendSingleJsonError(w, http.StatusBadRequest)
		log.Println(err2)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(data)
	if err != nil {
		log.Printf("Met Error when writing json to ResponseWriter: %v", err)
	}
}

func SingleQuery(params SingleAPIParams) (SingleResponse, error) {
	date := params.Date
	batch := params.Batch
	filePath := filepath.Join("tmp", date+"-"+batch+".json")

	// First try
	response, err := readAndParseFile(filePath, params)
	if err == nil {
		return response, nil
	}

	// Try to download
	if err := downloadAndSave(date, batch); err != nil {
		return singleFailResponse, fmt.Errorf("download failed: %w", err)
	}

	// Second try
	response, err = readAndParseFile(filePath, params)
	if err != nil {
		log.Printf("Second read/parse failed after download: %v", err)
		return singleFailResponse, fmt.Errorf("read/parse failed after download: %w", err)
	}

	// finally
	return response, nil
}
