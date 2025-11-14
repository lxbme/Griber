package main

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
)

func makeRelative(date string, batch string, suffix string, prot string) string {
	fileName := date + batch[:2] + "0000-0h-" + prot + "-fc" + suffix
	relative := filepath.Join(date, batch, "ifs/0p25", prot, fileName)
	return relative
}

func makeAbs(bucketName string, date string, batch string, suffix string, prot string) string {
	basePath := "/" + bucketName
	relative := makeRelative(date, batch, suffix, prot)
	path := filepath.Join(basePath, relative)
	return path
}

func makeUrl(domain string, path string) string {
	u := url.URL{
		Scheme: "https",
		Host:   domain,
		Path:   path,
	}
	resultUrl := u.String()
	return resultUrl
}

func writeFile(path string, data []byte) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	file.Write(data)
	return nil
}

func unwarpGribRawJsonValue(raw string) ([]float64, error) {
	type NormalJson map[string]interface{}
	jsonHolder := NormalJson{}
	if err := json.Unmarshal([]byte(raw), &jsonHolder); err != nil {
		return nil, fmt.Errorf("fail to parse Json: %w", err)
	}

	messages := jsonHolder["messages"].([]interface{})[0].([]interface{})
	var values []float64
	for _, message := range messages {
		if message.(map[string]interface{})["key"] == "values" {
			// JSON 解析后，数字数组是 []interface{}，需要逐个转换
			valueInterface := message.(map[string]interface{})["value"].([]interface{})
			values = make([]float64, len(valueInterface))
			for i, v := range valueInterface {
				values[i] = v.(float64)
			}
		}
	}
	return values, nil
}

const (
	Ni          int     = 1440
	Nj          int     = 721
	LatFirst    float64 = 90.0
	LatStep     float64 = 0.25
	LonFirst    float64 = 180.0 // GRIB data starts from 180° longitude
	LonStep     float64 = 0.25
	TotalPoints int     = 1038240
)

// GetIndexForCoord targetLat: (-90 to 90)
// targetLon: (-180 to 180)
func GetIndexForCoord(targetLat, targetLon float64) (int, error) {
	// Normalize lon to 0 to 360
	normalizedLon := math.Mod(targetLon, 360)
	if normalizedLon < 0 {
		normalizedLon += 360
	}

	// Calculate offset from LonFirst (180)
	// Data array starts at 180 and wraps: 180, 180.25, ..., 359.75, 0, 0.25, ..., 179.75
	lonOffset := normalizedLon - LonFirst
	if lonOffset < 0 {
		lonOffset += 360 // Handle wrap-around
	}

	// calc nearest lon index
	iFloat := lonOffset / LonStep
	i := int(math.Round(iFloat)) % Ni

	// GRIB scan from 90 (North) to -90 (South)
	// j = (LatFirst - targetLat) / LatStep
	jFloat := (LatFirst - targetLat) / LatStep
	j := int(math.Round(jFloat))

	// no looping but constraint
	if j < 0 {
		j = 0 // targetLat > 90
	}
	if j >= Nj {
		j = Nj - 1 // targetLat < -90
	}

	// calc slice index
	index := (j * Ni) + i

	// safe check
	if index < 0 || index >= TotalPoints {
		return -1, fmt.Errorf("index %d out of range [0, %d)", index, TotalPoints)
	}

	return index, nil
}
