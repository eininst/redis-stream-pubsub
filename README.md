# redis-stream-pubsub

[![Go Reference](https://pkg.go.dev/badge/github.com/eininst/redis-stream-pubsub.svg)](https://pkg.go.dev/github.com/eininst/redis-stream-pubsub)
[![License](https://img.shields.io/github/license/eininst/redis-stream-pubsub.svg)](LICENSE)

基于 **Redis Streams** 实现的发布订阅（Pub/Sub）框架，提供简单、轻量级的 **生产者** 与 **消费者** 接口，封装了 `XAdd`、`XReadGroup`、`XPending`、`XAck` 等操作，简化了在 Go 语言中使用 Redis 流式消息的过程。  
使用本项目，你可以快速搭建一个分布式的消息队列或事件系统。

## 功能特性

1. **生产者 (Producer)**
    - 使用 `XAdd` 将消息写入 Redis Streams
    - 可设置 `MaxLen`、`Approx`，控制 Stream 的最大长度
    - 支持自定义 `redis.Client`

2. **消费者 (Consumer)**
    - 基于 Redis `XGroup` 实现消费组，防止消息丢失
    - 支持多协程并发消费，通过内置的协程池（`ants`）管理并发
    - 配置灵活：可设置 `ReadCount`、`BlockTime`、`BatchSize`、`MaxRetries`、`Timeout` 等
    - 自动处理 Pending 消息（`XPending` + `XClaim`），可重试或放弃超时未确认的消息

3. **优雅关闭 (Graceful Shutdown)**
    - 内部捕捉系统信号（可自定义），在进程退出时，安全停止消费并等待任务执行完毕
    - 配合 `context.Context`，支持超时或取消

4. **高可用**
    - 结合 Redis Streams 自带的消费组机制，多实例部署时可自动进行负载均衡与容错
    - 分布式环境下保障消息可靠性

5. **易扩展**
    - 生产者、消费者均提供可选的参数结构体 (`ProducerOptions` / `Options`)
    - 可以自定义初始化逻辑、复用已有的 `redis.Client`

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
    consumer := pubsub.NewConsumer("redis://localhost:6379/0",
        pubsub.WithWorkers(32),           // 函数处理(ants 协程池大小)，默认0不限制
        //pubsub.WithXXXX(), 
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
* `WithWorkers`: 并发协程数量（由 `ants` 池控制）, 默认`0`
* `WithReadCount`: 每次 XReadGroup 拉取的消息数, 默认`10`
* `WithBlockTime`: 拉取消息的阻塞时间, 默认`5s`
* `WithBatchSize`: 将多个 Handler 分批启动 Goroutine, 默认`16`
* `WithXpendingInterval`: 定期检查 Pending 消息的时间间隔, 默认`3s`
* `WithTimeout`: 用于 Pending 消息的 MinIdleTime，超时可能会触发 XClaim, 默认`300s`
* `WithMaxRetries`: 若消息被重试次数超过此值，自动 XAck 并丢弃, 默认`64`


> See [example](/example)

## 开发与贡献
欢迎参与本项目开发和提交 Issue/PR：

* Issue: 遇到问题或有新需求，可在 Issues 反馈
* PR: 修复 Bug 或实现新特性后，可发起 Pull Request 贡献给社区

## License

*MIT*