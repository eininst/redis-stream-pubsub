package main

import (
	"fmt"
	"github.com/eininst/redis-stream-pubsub/pubsub"
	"syscall"
	"time"
)

// 处理消息的函数
func handleMsg(ctx *pubsub.Context) error {
	// 解析消息
	stream := ctx.Msg.Stream
	id := ctx.Msg.ID
	payload := ctx.Msg.Payload

	// 业务逻辑
	fmt.Printf("Consume from stream=%s ID=%s payload=%v\n", stream, id, payload)

	// 返回 error 则会触发 panic -> recover -> 不进行 XAck，消息进入 pending
	// 返回 nil 则表示消费成功，会执行 XAck
	return nil
}

func main() {
	// 1. 创建一个 Consumer
	consumer := pubsub.NewConsumer("redis://localhost:6379/0",
		pubsub.WithTimeout(time.Second*10),                  // 使用 ants 协程池，worker 数量为 5
		pubsub.WithSignals(syscall.SIGINT, syscall.SIGTERM), //默认SIGTERM,
	)

	// 2. 注册消息处理函数
	consumer.Handler("test_stream", handleMsg)

	// 3. 启动消费，阻塞等待消息
	consumer.Spin()

	// 当捕捉到退出信号或手动调用 Shutdown()，会优雅停止
}
