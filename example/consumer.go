package main

import (
	"github.com/eininst/flog"
	"github.com/eininst/redis-stream-pubsub/pubsub"
	"time"
)

func main() {
	cs := pubsub.NewConsumer("redis://localhost:6379/0",
		pubsub.WithMaxRetries(1),
		pubsub.WithBlockTime(time.Second*5),
		pubsub.WithTimeout(time.Second*5),
	)

	cs.Handler("test", func(ctx *pubsub.Context) error {
		flog.Info(ctx.JSON.Raw)
		return nil
	})

	cs.Spin()
}
