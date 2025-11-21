package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type DateRangeAPIParams struct {
	Lat       float64 `json:"lat"`
	Lon       float64 `json:"lon"`
	StartDate string  `json:"start_date"` // yyyymmdd format
	EndDate   string  `json:"end_date"`   // yyyymmdd format
	Batch     string  `json:"batch"`
}

type DateRangeResponse struct {
	Dates   []string  `json:"dates"`   // dates array yyyymmdd
	U       []float64 `json:"u"`       // u array
	V       []float64 `json:"v"`       // v array
	Status  int       `json:"status"`  // HTTP status code
	Success bool      `json:"success"` // whether success
}

var dateRangeFailResponse = DateRangeResponse{
	Dates:   []string{},
	U:       []float64{},
	V:       []float64{},
	Status:  http.StatusBadRequest,
	Success: false,
}

// file data cache structure
type FileCache struct {
	U []float64
	V []float64
}

// global cache
var (
	fileCache   = make(map[string]*FileCache)
	cacheMutex  sync.RWMutex
	maxCacheSize = 100
)

func sendDateRangeJsonError(w http.ResponseWriter, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(dateRangeFailResponse)
}

func dateRangeQueryHandler(w http.ResponseWriter, r *http.Request) {
	httpQuery := r.URL.Query()

	// parse lat
	latStr := httpQuery.Get("lat")
	if latStr == "" {
		sendDateRangeJsonError(w, http.StatusBadRequest)
		return
	}
	lat, err := strconv.ParseFloat(latStr, 64)
	if err != nil {
		sendDateRangeJsonError(w, http.StatusBadRequest)
		return
	}

	// parse lon
	lonStr := httpQuery.Get("lon")
	if lonStr == "" {
		sendDateRangeJsonError(w, http.StatusBadRequest)
		return
	}
	lon, err := strconv.ParseFloat(lonStr, 64)
	if err != nil {
		sendDateRangeJsonError(w, http.StatusBadRequest)
		return
	}

	// parse start_date
	startDate := httpQuery.Get("start_date")
	if startDate == "" {
		sendDateRangeJsonError(w, http.StatusBadRequest)
		return
	}
	// validate start_date format (yyyymmdd)
	if !isValidDateFormat(startDate) {
		sendDateRangeJsonError(w, http.StatusBadRequest)
		return
	}

	// parse end_date
	endDate := httpQuery.Get("end_date")
	if endDate == "" {
		sendDateRangeJsonError(w, http.StatusBadRequest)
		return
	}
	// validate end_date format (yyyymmdd)
	if !isValidDateFormat(endDate) {
		sendDateRangeJsonError(w, http.StatusBadRequest)
		return
	}

	// parse batch
	batch := httpQuery.Get("batch")
	if batch == "" {
		sendDateRangeJsonError(w, http.StatusBadRequest)
		return
	}
	// validate batch format
	if batch != "00z" && batch != "06z" && batch != "12z" && batch != "18z" {
		sendDateRangeJsonError(w, http.StatusBadRequest)
		return
	}

	params := DateRangeAPIParams{
		Lat:       lat,
		Lon:       lon,
		StartDate: startDate,
		EndDate:   endDate,
		Batch:     batch,
	}

	// execute query
	data, err2 := DateRangeQuery(params)
	if err2 != nil {
		sendDateRangeJsonError(w, http.StatusBadRequest)
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

func DateRangeQuery(params DateRangeAPIParams) (DateRangeResponse, error) {
	lat := params.Lat
	lon := params.Lon
	startDate := params.StartDate
	endDate := params.EndDate
	batch := params.Batch

	// get coordinate index (one-time calculation)
	valueIndex, err := GetIndexForCoord(lat, lon)
	if err != nil {
		return dateRangeFailResponse, fmt.Errorf("failed to get index for coord: %w", err)
	}

	// generate all dates in the date range
	dates, err := generateDateRange(startDate, endDate)
	if err != nil {
		return dateRangeFailResponse, fmt.Errorf("failed to generate date range: %w", err)
	}

	var resultDates []string
	var uValues []float64
	var vValues []float64

	// iterate through all dates
	for _, date := range dates {
		filePath := filepath.Join("tmp", date+"-"+batch+".json")
		
		// read data from cache or file
		cache, err := getOrLoadFileCache(filePath, date, batch)
		if err != nil {
			log.Printf("Warning: failed to load data for date %s: %v", date, err)
			// set to 0 if data fetch failed
			resultDates = append(resultDates, date)
			uValues = append(uValues, 0)
			vValues = append(vValues, 0)
			continue
		}

		// boundary check
		if valueIndex < 0 || valueIndex >= len(cache.U) || valueIndex >= len(cache.V) {
			log.Printf("Warning: index %d out of bounds for date %s", valueIndex, date)
			// set to 0 if index out of bounds
			resultDates = append(resultDates, date)
			uValues = append(uValues, 0)
			vValues = append(vValues, 0)
			continue
		}

		// add to result
		resultDates = append(resultDates, date)
		uValues = append(uValues, cache.U[valueIndex])
		vValues = append(vValues, cache.V[valueIndex])
	}

	if len(resultDates) == 0 {
		return dateRangeFailResponse, fmt.Errorf("no data found in date range %s to %s", startDate, endDate)
	}

	response := DateRangeResponse{
		Dates:   resultDates,
		U:       uValues,
		V:       vValues,
		Status:  http.StatusOK,
		Success: true,
	}

	return response, nil
}

// get or load file cache
func getOrLoadFileCache(filePath string, date string, batch string) (*FileCache, error) {
	// try to read from cache first
	cacheMutex.RLock()
	cache, exists := fileCache[filePath]
	cacheMutex.RUnlock()

	if exists {
		return cache, nil
	}

	// cache not exist, read file
	cache, err := loadFileToCache(filePath, date, batch)
	if err != nil {
		return nil, err
	}

	// write to cache
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	// check cache size, if over limit, clear old cache
	if len(fileCache) >= maxCacheSize {
		// simple strategy: clear all cache
		fileCache = make(map[string]*FileCache)
		log.Printf("Cache size exceeded %d, cleared all cache", maxCacheSize)
	}

	fileCache[filePath] = cache
	return cache, nil
}

// load data from file to cache
func loadFileToCache(filePath string, date string, batch string) (*FileCache, error) {
	// try to read file
	content, err := os.ReadFile(filePath)
	if err != nil {
		// file not exist, try to download
		if os.IsNotExist(err) {
			if err := downloadAndSave(date, batch); err != nil {
				return nil, fmt.Errorf("download failed: %w", err)
			}
			// read again
			content, err = os.ReadFile(filePath)
			if err != nil {
				return nil, fmt.Errorf("failed to read file after download: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to read file: %w", err)
		}
	}

	// parse JSON
	var data struct {
		U []float64 `json:"10u"`
		V []float64 `json:"10v"`
	}

	if err := json.Unmarshal(content, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal json: %w", err)
	}

	if len(data.U) == 0 || len(data.V) == 0 {
		return nil, fmt.Errorf("json data is empty or missing")
	}

	cache := &FileCache{
		U: data.U,
		V: data.V,
	}

	return cache, nil
}

// isValidDateFormat validates date format (yyyymmdd)
func isValidDateFormat(dateStr string) bool {
	if len(dateStr) != 8 {
		return false
	}
	_, err := time.Parse("20060102", dateStr)
	return err == nil
}

// generateDateRange generates all dates between start and end (inclusive, yyyymmdd format)
func generateDateRange(startDate, endDate string) ([]string, error) {
	// parse start date
	start, err := time.Parse("20060102", startDate)
	if err != nil {
		return nil, fmt.Errorf("invalid start_date format: %w", err)
	}

	// parse end date
	end, err := time.Parse("20060102", endDate)
	if err != nil {
		return nil, fmt.Errorf("invalid end_date format: %w", err)
	}

	// check if start date is before or equal to end date
	if start.After(end) {
		return nil, fmt.Errorf("start_date (%s) must be before or equal to end_date (%s)", startDate, endDate)
	}

	// generate all dates
	var dates []string
	current := start
	for !current.After(end) {
		dateStr := current.Format("20060102")
		dates = append(dates, dateStr)
		current = current.AddDate(0, 0, 1)
	}

	return dates, nil
}

func ClearDateRangeCache() {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	fileCache = make(map[string]*FileCache)
	log.Println("DateRange API cache cleared")
}

