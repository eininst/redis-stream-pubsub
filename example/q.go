package main

import (
	"context"
	"fmt"
	"github.com/eininst/flog"
	"github.com/go-redis/redis/v8"
	"time"
)

func trimStreamByTime(rdb *redis.Client, stream string, minAge time.Duration) {
	// 获取最小允许的消息时间戳（UNIX 时间戳）
	minID := fmt.Sprintf("%d-0", time.Now().Add(-minAge).Unix()*1000)

	// 修剪消息小于 minID 的记录
	err := rdb.Do(context.TODO(), "XTRIM", stream, "MINID", "~", minID).Err()
	if err != nil {
		fmt.Println("Failed to trim stream by time:", err)
	} else {
		fmt.Printf("Trimmed stream %s to minimum ID %s\n", stream, minID)
	}
}

func main() {
	minAge := time.Second * 10
	minID := fmt.Sprintf("%d-0", time.Now().Add(-minAge).Unix()*1000)

	flog.Info(minID)

	//stop := make(chan int)
	//ch := make(chan interface{}, 10)
	//
	//streams := []interface{}{}
	//go func() {
	//	for {
	//		ch <- "Hello, world!"
	//		time.Sleep(200 * time.Millisecond)
	//	}
	//}()
	//go func() {
	//	for {
	//		select {
	//		case v := <-ch:
	//			if len(streams) >= 10 {
	//				flog.Info(streams)
	//				streams = streams[:0]
	//			} else {
	//				streams = append(streams, v)
	//			}
	//		case <-time.After(2 * time.Second):
	//			flog.Info(streams)
	//			streams = streams[:0]
	//		}
	//	}
	//}()
	//
	//<-stop
}
