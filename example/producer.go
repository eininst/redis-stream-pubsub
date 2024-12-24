package main

import (
	"context"
	"fmt"
	"github.com/eininst/redis-stream-pubsub/pubsub"
)

func main() {
	// 1. 创建一个 Producer
	producer := pubsub.NewProducer("redis://localhost:6379/0", &pubsub.ProducerOptions{
		MaxLen: 1,
		Approx: false,
	})
	// 或者 pubsub.NewProducerWithClient(...) 注入自定义 redis.Client

	// 2. 构建要发送的消息
	msg := &pubsub.Msg{
		Stream: "test_stream",
		Payload: pubsub.H{
			"field1": "hello",
			"field2": 123,
		},
	}

	for i := 0; i < 5; i++ {
		// 3. 发送消息到 Redis Stream
		err := producer.Publish(context.Background(), msg)
		if err != nil {
			fmt.Println("Publish error:", err)
			return
		}

		// msg.ID 中将会包含 Redis 生成的消息ID
		fmt.Printf("Publish success! ID=%v\n", msg.ID)
	}
}
