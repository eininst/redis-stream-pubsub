package main

import (
	"github.com/eininst/flog"
	"github.com/eininst/redis-stream-pubsub/pubsub"
)

func main() {
	cs := pubsub.NewConsumer("redis://localhost:6379/0",
		pubsub.WithNoAck(true),
	)

	cs.Handler("wwe", func(ctx *pubsub.Context) error {
		return nil
	})

	cs.Handler("test", func(ctx *pubsub.Context) error {
		flog.Info(ctx.Payload)
		return nil
	})

	cs.Spin()
}
