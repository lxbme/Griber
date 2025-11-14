package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
)

type RangeAPIParams struct {
	SLat  float64 `json:"slat"`  // Start Latitude
	SLon  float64 `json:"slon"`  // Start Longitude
	ELat  float64 `json:"elat"`  // End Latitude
	ELon  float64 `json:"elon"`  // End Longitude
	Step  float64 `json:"step"`  // Step size
	Date  string  `json:"date"`  // Date
	Batch string  `json:"batch"` // Batch
}

type RangeResponse struct {
	U       []float64 `json:"u"`
	V       []float64 `json:"v"`
	Lats    []float64 `json:"lats"`
	Lons    []float64 `json:"lons"`
	Status  int       `json:"status"`
	Success bool      `json:"success"`
}

var rangeFailResponse = RangeResponse{
	U:       []float64{},
	V:       []float64{},
	Lats:    []float64{},
	Lons:    []float64{},
	Status:  http.StatusBadRequest,
	Success: false,
}

func sendRangeJsonError(w http.ResponseWriter, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(rangeFailResponse)
}

func rangeQueryHandler(w http.ResponseWriter, r *http.Request) {
	httpQuery := r.URL.Query()

	// Parse slat
	slatStr := httpQuery.Get("slat")
	if slatStr == "" {
		sendRangeJsonError(w, http.StatusBadRequest)
		return
	}
	slat, err := strconv.ParseFloat(slatStr, 64)
	if err != nil {
		sendRangeJsonError(w, http.StatusBadRequest)
		return
	}

	// Parse slon
	slonStr := httpQuery.Get("slon")
	if slonStr == "" {
		sendRangeJsonError(w, http.StatusBadRequest)
		return
	}
	slon, err := strconv.ParseFloat(slonStr, 64)
	if err != nil {
		sendRangeJsonError(w, http.StatusBadRequest)
		return
	}

	// Parse elat
	elatStr := httpQuery.Get("elat")
	if elatStr == "" {
		sendRangeJsonError(w, http.StatusBadRequest)
		return
	}
	elat, err := strconv.ParseFloat(elatStr, 64)
	if err != nil {
		sendRangeJsonError(w, http.StatusBadRequest)
		return
	}

	// Parse elon
	elonStr := httpQuery.Get("elon")
	if elonStr == "" {
		sendRangeJsonError(w, http.StatusBadRequest)
		return
	}
	elon, err := strconv.ParseFloat(elonStr, 64)
	if err != nil {
		sendRangeJsonError(w, http.StatusBadRequest)
		return
	}

	// Parse step
	stepStr := httpQuery.Get("step")
	if stepStr == "" {
		sendRangeJsonError(w, http.StatusBadRequest)
		return
	}
	step, err := strconv.ParseFloat(stepStr, 64)
	if err != nil || step <= 0 {
		sendRangeJsonError(w, http.StatusBadRequest)
		return
	}

	// Parse date
	date := httpQuery.Get("date")
	if date == "" {
		sendRangeJsonError(w, http.StatusBadRequest)
		return
	}

	// Parse batch
	batch := httpQuery.Get("batch")
	if batch == "" {
		sendRangeJsonError(w, http.StatusBadRequest)
		return
	}

	params := RangeAPIParams{
		SLat:  slat,
		SLon:  slon,
		ELat:  elat,
		ELon:  elon,
		Step:  step,
		Date:  date,
		Batch: batch,
	}

	// Query range
	data, err2 := RangeQuery(params)
	if err2 != nil {
		sendRangeJsonError(w, http.StatusBadRequest)
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

func RangeQuery(params RangeAPIParams) (RangeResponse, error) {
	date := params.Date
	batch := params.Batch
	filePath := filepath.Join("tmp", date+"-"+batch+".json")

	// First try
	response, err := readAndParseRangeFile(filePath, params)
	if err == nil {
		return response, nil
	}

	// Try to download
	if err := downloadAndSave(date, batch); err != nil {
		return rangeFailResponse, fmt.Errorf("download failed: %w", err)
	}

	// Second try
	response, err = readAndParseRangeFile(filePath, params)
	if err != nil {
		log.Printf("Second read/parse failed after download: %v", err)
		return rangeFailResponse, fmt.Errorf("read/parse failed after download: %w", err)
	}

	return response, nil
}

func readAndParseRangeFile(filePath string, params RangeAPIParams) (RangeResponse, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return RangeResponse{}, fmt.Errorf("failed to read file %s: %w", filePath, err)
	}

	var data struct {
		U []float64 `json:"10u"`
		V []float64 `json:"10v"`
	}

	if err := json.Unmarshal(content, &data); err != nil {
		return RangeResponse{}, fmt.Errorf("failed to unmarshal json from %s: %w", filePath, err)
	}

	if len(data.U) == 0 {
		return RangeResponse{}, fmt.Errorf("json data for '10u' is empty or missing")
	}
	if len(data.V) == 0 {
		return RangeResponse{}, fmt.Errorf("json data for '10v' is empty or missing")
	}

	// Generate grid points
	var uValues []float64
	var vValues []float64
	var lats []float64
	var lons []float64

	// Calculate number of steps
	latSteps := int(math.Abs(params.ELat-params.SLat)/params.Step) + 1
	lonSteps := int(math.Abs(params.ELon-params.SLon)/params.Step) + 1

	// Iterate through the grid
	for latIdx := 0; latIdx < latSteps; latIdx++ {
		lat := params.SLat + float64(latIdx)*params.Step*getSign(params.ELat-params.SLat)
		// Clamp latitude to valid range
		if lat > 90 {
			lat = 90
		}
		if lat < -90 {
			lat = -90
		}

		for lonIdx := 0; lonIdx < lonSteps; lonIdx++ {
			lon := params.SLon + float64(lonIdx)*params.Step*getSign(params.ELon-params.SLon)
			// Normalize longitude to -180 to 180
			for lon > 180 {
				lon -= 360
			}
			for lon < -180 {
				lon += 360
			}

			// Get index for this coordinate
			valueIndex, err := GetIndexForCoord(lat, lon)
			if err != nil {
				log.Printf("Warning: failed to get index for coord (%f, %f): %v", lat, lon, err)
				continue
			}

			// Bounds check
			if valueIndex < 0 || valueIndex >= len(data.U) || valueIndex >= len(data.V) {
				log.Printf("Warning: index %d out of bounds for coord (%f, %f)", valueIndex, lat, lon)
				continue
			}

			uValues = append(uValues, data.U[valueIndex])
			vValues = append(vValues, data.V[valueIndex])
			lats = append(lats, lat)
			lons = append(lons, lon)
		}
	}

	if len(uValues) == 0 {
		return RangeResponse{}, fmt.Errorf("no valid data points found in range")
	}

	response := RangeResponse{
		U:       uValues,
		V:       vValues,
		Lats:    lats,
		Lons:    lons,
		Status:  http.StatusOK,
		Success: true,
	}

	return response, nil
}

// getSign returns 1 if x >= 0, -1 otherwise
func getSign(x float64) float64 {
	if x >= 0 {
		return 1.0
	}
	return -1.0
}
