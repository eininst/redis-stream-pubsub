package main

import (
	"github.com/eininst/redis-stream-pubsub/pubsub"
	"log"
	"syscall"
)

func main() {
	cs := pubsub.NewConsumer("redis://localhost:6379/0",
		pubsub.WithSignal(syscall.SIGTERM, syscall.SIGINT))

	cs.Handler("test", func(ctx *pubsub.Context) error {
		log.Printf("received test msg:%v", ctx.Payload)
		return nil
	})

	cs.Spin()
}
