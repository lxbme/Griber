package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"

	"cloud.google.com/go/storage"
)

func fetchAndProcessGribChunk(ctx context.Context, client *storage.Client, bucketName, objectName string, chunk GribChunkInfo) (string, error) {
	log.Printf("Fetching: %s (Offset: %d, Length: %d)", chunk.ParamName, chunk.Offset, chunk.Length)

	// 1. 获取 GCS 对象句柄
	obj := client.Bucket(bucketName).Object(objectName)

	reader, err := obj.NewRangeReader(ctx, chunk.Offset, chunk.Length)
	if err != nil {
		return "", fmt.Errorf("fail to create RangeReader for %s: %w", chunk.ParamName, err)
	}
	defer reader.Close()

	tempFile, err := os.CreateTemp("", fmt.Sprintf("gribchunk-%s-*.grib2", chunk.ParamName))
	if err != nil {
		return "", fmt.Errorf("fail to create tmp file for %s: %w", chunk.ParamName, err)
	}

	defer func(name string) {
		err := os.Remove(name)
		if err != nil {
			log.Printf("Fail to remove temp file %s: %v", tempFile.Name(), err)
		}
	}(tempFile.Name())
	//defer tempFile.Close()

	// 4. 将 GCS 范围读取器的数据流复制到临时文件中
	if _, err := io.Copy(tempFile, reader); err != nil {
		return "", fmt.Errorf("fail to copy gcs data for %s: %w", chunk.ParamName, err)
	}

	// 确保在调用 exec 之前关闭文件句柄
	err = tempFile.Close()
	if err != nil {
		return "", fmt.Errorf("fail to close temp file: %w", err)
	}

	log.Printf("Parameter %s has downloaded to %s", chunk.ParamName, tempFile.Name())

	// 5. 使用 os/exec 调用 grib_to_json
	log.Printf("Transforming %s by grib_to_json...", chunk.ParamName)

	// grib_dump -j 会自动将 JSON 输出到 stdout
	cmd := exec.Command("grib_dump", "-j", tempFile.Name())

	// CombinedOutput 会同时捕获 stdout 和 stderr，便于调试
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("Fail to exec grib_to_json %s: %v", chunk.ParamName, err)
		log.Printf("grib_to_json error output: %s", string(output))
		return "", fmt.Errorf("fail to exec grib_to_json %s: %w", chunk.ParamName, err)
	}

	// 6. 成功！打印 JSON (或您需要的任何处理)
	//log.Printf("--- %s 的 JSON 数据 (前 500 字节) ---", chunk.ParamName)
	//if len(output) > 500 {
	//	fmt.Println(string(output[:500]) + "\n... (数据已截断)")
	//} else {
	//	fmt.Println(string(output))
	//}
	log.Printf("%s done.", chunk.ParamName)
	return strings.TrimSpace(string(output)), nil
}
