# redis-stream-pubsub

[![Go Reference](https://pkg.go.dev/badge/github.com/eininst/redis-stream-pubsub.svg)](https://pkg.go.dev/github.com/eininst/redis-stream-pubsub)
[![License](https://img.shields.io/github/license/eininst/redis-stream-pubsub.svg)](LICENSE)

一个**健壮且高效**的 Redis Streams 发布订阅（PubSub）系统，使用 Go 语言编写。该项目利用 [go-redis](https://github.com/redis/go-redis) 客户端和 [ants](https://github.com/panjf2000/ants) 协程池，实现高性能的消息消费与处理。支持**自动认领**、**并发处理**、**优雅关闭**等功能，适用于需要**高可靠性**和**高并发**消息处理的场景。

This project **requires Redis version 7 or higher**.

## 功能特性

1. **生产者 (Producer)**
    - 使用 `XAdd` 将消息写入 Redis Streams
    - 可设置 `MaxLen`、`Approx`，控制 Stream 的最大长度
    - 支持自定义 `redis.Client`


2. **消费者 (Consumer)**

   - **并发处理**：利用`ants`协程池实现高效的消息处理。
   - **自动认领**：自动认领待处理消息，确保消息不丢失。
   - **可配置选项**：通过多种配置选项高度自定义。
   - **优雅关闭**：确保所有正在处理的消息在关闭前完成。
   - **信号处理**：监听操作系统信号以启动优雅关闭。
   - **支持多流**：同时处理多个 Redis Streams。
   - **错误处理与恢复**：在处理过程中捕获并记录错误，确保系统稳定运行。
   - **高可用**
     - 结合 Redis Streams 自带的消费组机制，多实例部署时可自动进行负载均衡与容错
     - 分布式环境下保障消息可靠性

---


## 安装

```bash
go get github.com/eininst/redis-stream-pubsub
```



## 快速开始
下面给出几个常见场景的使用案例，分别介绍 生产者 与 消费者 如何使用。

### 1. 简单生产者示例

```go
package main

import (
    "context"
    "fmt"
    "github.com/eininst/redis-stream-pubsub"
)

func main() {
    // 1. 创建 Producer
    p := pubsub.NewProducer("redis://localhost:6379/0") // 或者 NewProducerWithClient(...)
    
    // 2. 准备要发送的消息
    msg := &pubsub.Msg{
        Stream: "test_stream",
        Payload: pubsub.H{
            "field1": "hello",
            "field2": 123,
        },
    }

    // 3. 发送消息到 Redis Stream
    err := p.Publish(context.TODO(), msg)
    if err != nil {
        fmt.Println("Publish error:", err)
    } else {
        fmt.Printf("Publish success! ID=%v\n", msg.ID)
    }
}
```

```text
Publish success! ID=1734968851711-0
```

#### 关键参数
* `MaxLen`: 用于限制 Stream 长度，默认 1024
* `Approx`: 是否使用 ~ 模式（近似长度），默认 true

可通过 `NewProducerWithClient(...)` 方式，注入自定义的 redis.Client 或自定义 `ProducerOptions`。

### 2. 简单消费者示例
```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/eininst/redis-stream-pubsub"
)

// 处理函数
func handleMsg(ctx *pubsub.Context) error {
    // 解析消息
    stream := ctx.Msg.Stream
    id := ctx.Msg.ID
    payload := ctx.Msg.Payload

    // 业务处理逻辑
    fmt.Printf("Consume from stream=%s ID=%s payload=%v\n", stream, id, payload)
    
    // 如果返回非 nil，会触发 panic -> recover -> 不进行 XAck 或触发重试
    // 这里返回 nil 表示消费成功
    return nil
}

func main() {
    // 1. 创建 Consumer
   // 1. 创建一个 Consumer
   consumer := pubsub.NewConsumer("redis://localhost:6379/0",
      pubsub.WithWorkers(32), // ants 协程池数量, 默认0(不限制)
      pubsub.WithSignals(syscall.SIGINT, syscall.SIGTERM), //默认SIGTERM,
   )

    // 2. 注册 Handler (Stream + 函数)
    consumer.Handler("test_stream", handleMsg)

    // 3. 启动消费
    consumer.Spin()

    // Spin() 内部会阻塞，直到捕捉到退出信号或手动 Shutdown
    // 在优雅关闭时，内部会等待未处理完的消息执行完成
}
```

```text
2024/12/23 23:47:09 [Spin] NoAck=false Workers=32 ReadCount=10 BlockTime=5s BatchSize=16
2024/12/23 23:47:09 [Spin] XpendingInterval=3s Timeout=5m0s MaxRetries=64
2024/12/23 23:47:09 [Spin] Start 1 goroutines to perform XRead from Redis...
2024/12/23 23:47:09 [Spin] Start 1 goroutines to perform XPending from Redis...
Consume from stream=test_stream ID=1734968851711-0 payload=map[field1:hello field2:123]
```

#### 消费者常用选项

| 选项                                                        | 描述                    | 默认值                       |
|-------------------------------------------------------------|-----------------------|------------------------------|
| `WithName(name string)`                                     | 设置消费者名称。              | `"CONSUMER"`                 |
| `WithGroup(group string)`                                   | 设置消费者组名称。             | `"CONSUMER-GROUP"`           |
| `WithWorkers(workers int)`                                  | 设置工作协程的数量。默认`0`,`不限制`     | `0`（根据 CPU 动态调整）     |
| `WithReadCount(readCount int64)`                            | 设置每次读取的消息数量。          | `10`                         |
| `WithBatchSize(batchSize int)`                              | 设置每批处理的处理器数量。         | `16`                         |
| `WithBlockTime(blockTime time.Duration)`                    | 设置读取消息时的阻塞时间。         | `5s`                         |
| `WithTimeout(timeout time.Duration)`                        | 设置自动认领消息的空闲超时时间。      | `300s`                       |
| `WithXautoClaimInterval(xautoClaimInterval time.Duration)`  | 设置自动认领的间隔时间。          | `5s`                         |
| `WithNoAck(noAck bool)`                                     | 如果设置为 `true`，则禁用消息确认。 | `false`                      |
| `WithSignals(signals ...os.Signal)`                         | 设置自定义的操作系统信号以监听关闭。    | `SIGTERM`                    |
| `WithExitWaitTime(exitWaitTime time.Duration)`              | 设置优雅关闭的最大等待时间。        | `10s`                        |


> See [example](/example)

## 开发与贡献
欢迎参与本项目开发和提交 Issue/PR：

* Issue: 遇到问题或有新需求，可在 Issues 反馈
* PR: 修复 Bug 或实现新特性后，可发起 Pull Request 贡献给社区

## License

*MIT*