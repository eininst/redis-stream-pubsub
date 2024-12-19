package pubsub

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
)

type H map[string]interface{}

type Producer interface {
	Publish(ctx context.Context, msg *Msg) error
}

type ProducerOptions struct {
	MaxLen int64
	Approx bool
}

var defaultProducerOptions = &ProducerOptions{
	MaxLen: 1024,
	Approx: true,
}

type producer struct {
	Rcli    *redis.Client
	options *ProducerOptions
}

func NewProducer(uri string, options ...*ProducerOptions) Producer {
	return NewProducerWithClient(NewRedisClient(uri), options...)
}

func NewProducerWithClient(rcli *redis.Client, options ...*ProducerOptions) Producer {
	var opt = defaultProducerOptions
	if len(options) > 0 {
		opt = options[0]
	}

	return &producer{
		Rcli:    rcli,
		options: opt,
	}
}

func (p *producer) Publish(ctx context.Context, msg *Msg) error {
	if msg.Payload == nil {
		return errors.New(
			fmt.Sprintf("Send msg Cannot be empty by Stream \"%s\"", msg.Stream))
	}

	if len(msg.Payload) == 0 {
		return errors.New(
			fmt.Sprintf("Send msg Cannot be empty by Stream \"%s\"", msg.Stream))
	}

	id, er := p.Rcli.XAdd(ctx, &redis.XAddArgs{
		Stream: msg.Stream,
		MaxLen: p.options.MaxLen,
		Approx: p.options.Approx,
		Values: msg.Payload,
	}).Result()

	if er != nil {
		return er
	}

	msg.ID = id

	return nil
}
