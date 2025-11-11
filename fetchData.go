package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"cloud.google.com/go/storage"
)

type GribChunkInfo struct {
	ParamName string
	Offset    int64
	Length    int64
}

func getGribData(gribChunk []GribChunkInfo, bucketName string, objectName string) (map[string]string, error) {
	// GCS auth context
	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("fail to init GCS (Check gcloud auth): %w", err)
	}
	defer func(client *storage.Client) {
		err := client.Close()
		if err != nil {
			log.Printf("Fail to close GCS: %v", err)
		}
	}(client)

	log.Printf("GCS Connected processing obj: %s", objectName)

	// 遍历并处理您需要的每一个数据块
	resultJsonMap := make(map[string]string)
	for _, chunk := range gribChunk {
		result, err := fetchAndProcessGribChunk(ctx, client, bucketName, objectName, chunk)
		if err != nil {
			return nil, fmt.Errorf("fail to fetch and process chunk %s: %w", chunk.ParamName, err)
		}
		resultJsonMap[chunk.ParamName] = result
	}
	return resultJsonMap, nil
}

func queryIndex(url string) (string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", fmt.Errorf("fail to get index url: %w", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Printf("Fail to close response body: %v", err)
		}
	}(resp.Body)

	scanner := bufio.NewScanner(resp.Body)
	buffer := ""
	for scanner.Scan() {
		buffer += scanner.Text() + "\n"
	}

	return buffer, nil
}

type IndexData map[string]interface{}

func parseIndexResponse(index string) ([]GribChunkInfo, error) {
	scanner := bufio.NewScanner(strings.NewReader(index))
	var data []GribChunkInfo
	for scanner.Scan() {
		var lineData IndexData
		line := scanner.Text()
		//fmt.Println(line)
		if err := json.Unmarshal([]byte(line), &lineData); err != nil {
			return nil, fmt.Errorf("fail to unmarshal index line: %w", err)
		}
		if (lineData["param"].(string) == "10u" || lineData["param"].(string) == "10v") && (lineData["levtype"].(string) == "sfc") {
			gribChunk := GribChunkInfo{
				ParamName: lineData["param"].(string),
				Offset:    int64(lineData["_offset"].(float64)),
				Length:    int64(lineData["_length"].(float64)),
			}

			data = append(data, gribChunk)
		}
	}
	return data, nil
}
