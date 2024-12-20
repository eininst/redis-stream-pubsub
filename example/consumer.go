package main

import (
	"github.com/eininst/redis-stream-pubsub/pubsub"
	"log"
	"time"
)

func main() {
	cs := pubsub.NewConsumer("redis://localhost:6379/0",
		pubsub.WithTimeout(time.Second*5))

	cs.Handler("test", func(ctx *pubsub.Context) error {
		log.Printf("received test msg:%v", ctx.Payload)
		return nil
	})

	cs.Spin()
}
