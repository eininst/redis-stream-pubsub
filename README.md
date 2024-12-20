# redis-stream-pubsub

`A message queue implemented based on Redis Stream`

## ⚙ Installation

```text
go get -u github.com/eininst/redis-stream-pubsub
```

## ⚡ Quickstart


### Producer

```go
package main

import (
	"context"
	"github.com/eininst/redis-stream-pubsub/pubsub"
	"log"
)

func main() {
	p := pubsub.NewProducer("redis://localhost:6379/0")

	msg := &pubsub.Msg{
		Stream: "test",
		Payload: pubsub.H{
			"name": "hello",
		},
	}

	er := p.Publish(context.TODO(), msg)

	if er != nil {
		log.Panic(er)
	}

	log.Println(msg.ID)
}
```

### Consumer

```go
package main

import (
	"github.com/eininst/redis-stream-pubsub/pubsub"
	"log"
)

func main() {
	cs := pubsub.NewConsumer("redis://localhost:6379/0")

	cs.Handler("test", func(ctx *pubsub.Context) error {
		log.Printf("received test msg:%v", ctx.Payload)
		return nil
	})

	cs.Spin()
}
```

```text
2024/12/21 01:05:07 [Spin] NoAck=false Workers=0 ReadCount=10 BlockTime=5s BatchSize=16
2024/12/21 01:05:07 [Spin] Xpending=3s Timeout=5m0s MaxRetries=64
2024/12/21 01:05:07 [Spin] Start 1 goroutines to perform XRead from Redis...
2024/12/21 01:05:07 [Spin] Start 1 goroutines to perform XPending from Redis...
2024/12/21 01:05:09 received test msg:map[name:hello]
```

> Graceful Shutdown
> 
```text
kill -SIGTERM pid

2024/12/20 16:45:53 [Spin] Shutdown...
2024/12/20 16:45:53 [Spin] Graceful shutdown success!
```

> See [example](/example)

## License

*MIT*
