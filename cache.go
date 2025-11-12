package main

import (
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
)

func downloadAndSave(date string, batch string) error {
	// date : yyyymmdd ; batch in 06z 18z UTC Time
	var objectName string
	var IndexPath string
	if batch == "00z" || batch == "12z" {
		objectName = makeRelative(date, batch, ".grib2", "oper")
		IndexPath = makeAbs(bucketName, date, batch, ".index", "oper")
		log.Println("Parsing oper")
	} else if batch == "06z" || batch == "18z" {
		objectName = makeRelative(date, batch, ".grib2", "scda")
		IndexPath = makeAbs(bucketName, date, batch, ".index", "scda")
		log.Println("Parsing scda")
	}

	indexUrl := makeUrl("storage.googleapis.com", IndexPath)
	indexScanner, err := queryIndex(indexUrl) // index resp scanner
	if err != nil {
		return fmt.Errorf("fail to query index: %w", err)
	}
	gribChunk, err := parseIndexResponse(indexScanner) // [10u, 10v]
	if err != nil {
		return fmt.Errorf("fail to parse index response: %w", err)
	}
	gribJsonMap, err := getGribData(gribChunk, bucketName, objectName) // {"10u":.. "10v":..}
	if err != nil {
		return fmt.Errorf("fail to get grib data: %w", err)
	}

	uValues, err := unwarpGribRawJsonValue(gribJsonMap["10u"])
	if err != nil {
		return fmt.Errorf("fail to unwrap 10u: %w", err)
	}
	vValues, err := unwarpGribRawJsonValue(gribJsonMap["10v"])
	if err != nil {
		return fmt.Errorf("fail to unwrap 10v: %w", err)
	}

	processedMap := map[string][]float64{
		"10u": uValues,
		"10v": vValues,
	}

	processedJson, err := json.Marshal(processedMap)
	if err != nil {
		return fmt.Errorf("fail to marshal Map to Json: %w", err)
	}

	fileName := fmt.Sprintf("%s-%s.json", date, batch)
	fileName = filepath.Join("tmp", fileName)
	err = writeFile(fileName, []byte(processedJson))
	if err != nil {
		return fmt.Errorf("fail to write file: %w", err)
	}

	return nil
}
