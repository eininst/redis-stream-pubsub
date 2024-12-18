package pubsub

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
)

type H map[string]interface{}

type Producer interface {
	Publish(ctx context.Context, stream string, msg map[string]interface{}) error
}
type producer struct {
	Rcli *redis.Client
}

func NewProducer(uri string) Producer {
	return &producer{
		Rcli: NewRedisClient(uri),
	}
}

func NewProducerWithClient(rcli *redis.Client) Producer {
	return &producer{
		Rcli: rcli,
	}
}

func (p *producer) Publish(ctx context.Context,
	stream string, msg map[string]interface{}) error {

	if msg == nil {
		return errors.New(
			fmt.Sprintf("Send msg Cannot be empty by Stream \"%s\"", stream))
	}

	if len(msg) == 0 {
		return errors.New(
			fmt.Sprintf("Send msg Cannot be empty by Stream \"%s\"", stream))
	}

	return p.Rcli.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		MaxLen: 1024,
		Approx: true,
		ID:     "*",
		Values: msg,
	}).Err()
}

type Msg struct {
	Stream  string
	Payload map[string]interface{}
}

func (p *producer) PublishInBatches(ctx context.Context,
	msgs []*Msg, batchSize int) error {

	for _, msg := range msgs {
		if msg.Payload == nil {
			return errors.New(
				fmt.Sprintf("Send msg Cannot be empty by Stream \"%s\"", msg.Stream))
		}

		if len(msg.Payload) == 0 {
			return errors.New(
				fmt.Sprintf("Send msg Cannot be empty by Stream \"%s\"", msg.Stream))
		}
	}
	chunks := ChunkArray(msgs, batchSize)

	for _, chunkMsgs := range chunks {
		_, err := p.Rcli.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, msg := range chunkMsgs {
				pipe.XAdd(ctx, &redis.XAddArgs{
					Stream: msg.Stream,
					MaxLen: 1024,
					Approx: true,
					ID:     "*",
					Values: msg.Payload,
				})
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}
