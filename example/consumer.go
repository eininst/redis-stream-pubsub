package main

import (
	"github.com/eininst/flog"
	"github.com/eininst/redis-stream-pubsub/pubsub"
	"github.com/go-redis/redis/v8"
	"time"
)

func main() {
	rcli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		//Password:     "7c3cD505",
		DB:           0,
		DialTimeout:  30 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		PoolSize:     100,
		MinIdleConns: 25,
		PoolTimeout:  30 * time.Second,
	})

	cs := pubsub.NewConsumer(rcli, pubsub.WithBlockTime(time.Second*2))

	cs.Handler("test", func(ctx *pubsub.Context) {
		defer ctx.Ack()

		flog.Info(ctx.Msg)
	})

	cs.Spin()
}
