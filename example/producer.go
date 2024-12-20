package main

import (
	"context"
	"github.com/eininst/flog"
	"github.com/eininst/redis-stream-pubsub/pubsub"
)

func main() {
	p := pubsub.NewProducer("redis://localhost:6379/0", &pubsub.ProducerOptions{
		MaxLen: 1024,
		Approx: true,
	})

	msg := &pubsub.Msg{
		Stream: "test",
		Payload: pubsub.H{
			"name": "hello",
		},
	}

	er := p.Publish(context.TODO(), msg)

	if er != nil {
		flog.Error(er)
	}

	flog.Info(msg.ID)
}
