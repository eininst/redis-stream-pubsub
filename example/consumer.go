package main

import (
	"github.com/eininst/redis-stream-pubsub/pubsub"
	"log"
	"syscall"
	"time"
)

func main() {
	cs := pubsub.NewConsumer("redis://localhost:6379/0",
		pubsub.WithTimeout(time.Second*10),
		pubsub.WithExitWaitTime(time.Second*3),
		//pubsub.WithNoAck(false),
		pubsub.WithSignal(syscall.SIGTERM, syscall.SIGINT))

	cs.Handler("test", func(ctx *pubsub.Context) error {
		log.Printf("received test msg:%v", ctx.Payload)
		time.Sleep(time.Second * 3)
		return nil
	})

	cs.Spin()
}
