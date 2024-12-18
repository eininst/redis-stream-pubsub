package pubsub

import (
	"encoding/json"
	"time"
)

func ChunkArray[T any](arr []*T, size int) [][]*T {
	var result [][]*T
	for i := 0; i < len(arr); i += size {
		// 计算当前子数组的结束索引
		end := i + size
		// 确保结束索引不超过数组长度
		if end > len(arr) {
			end = len(arr)
		}
		// 将当前子数组添加到结果中
		result = append(result, arr[i:end])
	}
	return result
}

func SplitDict(inputDict map[string]int, numParts int) []map[string]int {
	// 计算字典的总长度
	totalLength := len(inputDict)

	// 计算每部分的大小和剩余部分
	partSize := totalLength / numParts
	remainder := totalLength % numParts

	// 将字典的 keys 转换为切片
	keys := make([]string, 0, len(inputDict))
	for k := range inputDict {
		keys = append(keys, k)
	}

	// 用于存储分割后的结果
	var result []map[string]int

	// 追踪索引位置
	startIndex := 0

	for i := 0; i < numParts; i++ {
		// 计算当前部分的大小
		currentPartSize := partSize
		if i < remainder {
			currentPartSize++
		}

		// 计算当前部分的结束索引
		endIndex := startIndex + currentPartSize

		// 创建当前部分的字典
		currentDict := make(map[string]int)
		for _, key := range keys[startIndex:endIndex] {
			currentDict[key] = inputDict[key]
		}

		// 仅在字典非空时添加到结果中
		if len(currentDict) > 0 {
			result = append(result, currentDict)
		}

		// 更新起始索引
		startIndex = endIndex
	}

	return result
}

// Time 复制 time.Time 对象，并返回复制体的指针
func Time(t time.Time) *time.Time {
	return &t
}

// String 复制 string 对象，并返回复制体的指针
func String(s string) *string {
	return &s
}

// Bool 复制 bool 对象，并返回复制体的指针
func Bool(b bool) *bool {
	return &b
}

// Float64 复制 float64 对象，并返回复制体的指针
func Float64(f float64) *float64 {
	return &f
}

// Float32 复制 float32 对象，并返回复制体的指针
func Float32(f float32) *float32 {
	return &f
}

// Int64 复制 int64 对象，并返回复制体的指针
func Int64(i int64) *int64 {
	return &i
}

// Int32 复制 int64 对象，并返回复制体的指针
func Int32(i int32) *int32 {
	return &i
}

func Int(i int) *int {
	return &i
}

func ToMap(data interface{}) map[string]interface{} {
	_bytes, _ := json.Marshal(data)
	result := map[string]interface{}{}
	_ = json.Unmarshal(_bytes, &result)
	return result
}
