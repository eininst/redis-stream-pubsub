package main

import (
	"github.com/eininst/redis-stream-pubsub/pubsub"
	"log"
)

func main() {
	cs := pubsub.NewConsumer("redis://localhost:6379/0")

	cs.Handler("test", func(ctx *pubsub.Context) error {
		log.Panicf("received test msg:%v", ctx.Payload)
		return nil
	})

	cs.Spin()
}
