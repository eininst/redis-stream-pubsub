package main

import (
	"context"
	"github.com/eininst/redis-stream-pubsub/pubsub"
	"log"
)

func main() {
	p := pubsub.NewProducer("redis://localhost:6379/0")

	msg := &pubsub.Msg{
		Stream: "test",
		Payload: pubsub.H{
			"name": "hello",
		},
	}

	er := p.Publish(context.TODO(), msg)

	if er != nil {
		log.Panic(er)
	}

	log.Println(msg.ID)
}
