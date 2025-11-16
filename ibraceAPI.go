package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
)

type TyphonAPIParams struct {
	date  string
	batch string
}

type TyphonAPIResponse struct {
	Now    []map[string]string         `json:"now"`
	Trace  map[string]map[int][]string `json:"trace"`
	Status int                         `json:"status"`
	Some   bool                        `json:"some"`
}

var typhonAPIErrorResponse = TyphonAPIResponse{
	Now:    nil,
	Trace:  nil,
	Status: http.StatusBadRequest,
	Some:   false,
}

var typhonData, typhonErr = readCSV("data/ibtracs.csv")

func sendTyphonAPIError(w http.ResponseWriter, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode) // 写入HTTP状态码 (例如 400, 500)
	json.NewEncoder(w).Encode(typhonAPIErrorResponse)
}

func typhonAPIHandler(w http.ResponseWriter, r *http.Request) {
	httpQuery := r.URL.Query()
	date := httpQuery.Get("date")
	batch := httpQuery.Get("batch")
	if date == "" || batch == "" {
		sendTyphonAPIError(w, http.StatusBadRequest)
	}

	params := TyphonAPIParams{
		date:  date,
		batch: batch,
	}

	resp, err := getTyphon(params)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(resp)
	if err != nil {
		log.Printf("Met Error when writing json to ResponseWriter: %v", err)
	}
}

func getTyphon(params TyphonAPIParams) (TyphonAPIResponse, error) {
	if typhonErr != nil {
		fmt.Printf("Met Error when reading csv: %v", typhonErr)
		return typhonAPIErrorResponse, typhonErr
	}

	// 将 batch (如 "00z", "06z") 转换为小时数
	batchHour := strings.TrimSuffix(strings.ToLower(params.batch), "z")
	// 确保小时数是两位数
	if len(batchHour) == 1 {
		batchHour = "0" + batchHour
	}
	// 构建目标 ISO_TIME 格式: yyyymmddHH0000
	targetIsoTimeStr := params.date + batchHour + "0000"
	// 转换为整数以便比较
	targetIsoTime, err := strconv.ParseInt(targetIsoTimeStr, 10, 64)
	if err != nil {
		return typhonAPIErrorResponse, err
	}

	// CSV 列索引
	// 0: SID, 1: SEASON, 2: NUMBER, 3: BASIN, 4: SUBBASIN, 5: NAME, 6: ISO_TIME
	// 7: NATURE, 8: CMA_LAT, 9: CMA_LON, 10: CMA_CAT, 11: CMA_WIND, 12: CMA_PRES

	// 用于存储每个 SID 在当天最接近目标时间的记录
	sidClosestRecord := make(map[string][]string)
	sidMinDiff := make(map[string]int64) // 存储每个 SID 与目标时间的最小差值

	// 提取目标日期（yyyymmdd）
	targetDate := params.date

	// 第一遍遍历：找到每个台风在当天最接近目标小时的记录
	for i := 1; i < len(typhonData); i++ {
		record := typhonData[i]
		if len(record) < 13 {
			continue
		}

		isoTimeStr := record[6]
		sid := record[0]

		// 检查是否是当天的数据（只比较日期部分 yyyymmdd）
		if len(isoTimeStr) < 8 || isoTimeStr[:8] != targetDate {
			continue
		}

		// 解析 ISO_TIME 为整数
		isoTime, err := strconv.ParseInt(isoTimeStr, 10, 64)
		if err != nil {
			continue
		}

		// 计算时间差的绝对值（只比较小时部分）
		diff := isoTime - targetIsoTime
		absDiff := diff
		if absDiff < 0 {
			absDiff = -absDiff
		}

		// 如果是第一个记录，或者这个记录更接近目标时间
		if _, exists := sidMinDiff[sid]; !exists || absDiff < sidMinDiff[sid] {
			sidMinDiff[sid] = absDiff
			sidClosestRecord[sid] = record
		}
	}

	// 构建 Now 数组
	var now []map[string]string
	matchedSIDs := make(map[string]bool)

	for sid, record := range sidClosestRecord {
		matchedSIDs[sid] = true
		nowItem := map[string]string{
			"sid":      record[0],
			"season":   record[1],
			"number":   record[2],
			"basin":    record[3],
			"subbasin": record[4],
			"name":     record[5],
			"iso_time": record[6],
			"nature":   record[7],
			"cma_lat":  record[8],
			"cma_lon":  record[9],
			"cma_cat":  record[10],
			"cma_wind": record[11],
			"cma_pres": record[12],
		}
		now = append(now, nowItem)
	}

	// 输出匹配的 SID 数量（用于调试）
	fmt.Printf("Found %d typhoons on date %s\n", len(now), targetDate)

	// 第二遍遍历：为匹配的台风构建 Trace（所有轨迹点）
	// 只包含与 Now 中 SID 相同的台风数据
	trace := make(map[string]map[int][]string)
	for i := 1; i < len(typhonData); i++ {
		record := typhonData[i]
		if len(record) < 13 {
			continue
		}

		sid := record[0]
		name := record[5]
		numberStr := record[2]

		// 只处理在 Now 中出现的 SID（确保 trace 中的内容与 now 中的 SID 相同）
		if !matchedSIDs[sid] {
			continue
		}

		// 将 number 转换为 int
		number, err := strconv.Atoi(numberStr)
		if err != nil {
			continue
		}

		// 构建 Trace: 按名称和编号组织轨迹数据
		// 只添加 SID 在 matchedSIDs 中的记录（确保 trace 中的内容与 now 中的 SID 相同）
		if name != "" {
			if trace[name] == nil {
				trace[name] = make(map[int][]string)
			}
			// 将轨迹点转换为 JSON 字符串
			tracePoint := map[string]string{
				"sid":      record[0],
				"season":   record[1],
				"number":   record[2],
				"basin":    record[3],
				"subbasin": record[4],
				"name":     record[5],
				"iso_time": record[6],
				"nature":   record[7],
				"cma_lat":  record[8],
				"cma_lon":  record[9],
				"cma_cat":  record[10],
				"cma_wind": record[11],
				"cma_pres": record[12],
			}
			traceJson, err := json.Marshal(tracePoint)
			if err == nil {
				trace[name][number] = append(trace[name][number], string(traceJson))
			}
		}
	}

	// 设置 Some 标志
	some := len(now) > 0

	response := TyphonAPIResponse{
		Now:    now,
		Trace:  trace,
		Status: http.StatusOK,
		Some:   some,
	}

	return response, nil
}
