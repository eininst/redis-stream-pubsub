package main

import (
	"github.com/eininst/flog"
	"github.com/eininst/redis-stream-pubsub/pubsub"
)

func main() {
	cs := pubsub.NewConsumer("redis://localhost:6379/0")

	cs.Handler("test", func(ctx *pubsub.Context) error {
		flog.Infof("received test msg:%v", ctx.Payload)
		return nil
	})

	cs.Spin()
}
