package main

import (
	"context"
	"github.com/eininst/flog"
	"github.com/eininst/redis-stream-pubsub/pubsub"
)

func main() {
	p := pubsub.NewProducer("redis://localhost:6379/0")

	er := p.Publish(context.TODO(), "test", pubsub.H{
		"title": "this a simple message",
	})

	if er != nil {
		flog.Error(er)
	}
}
