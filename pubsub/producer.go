package pubsub

import (
	"context"
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
	rcli    *redis.Client
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
		rcli:    rcli,
		options: opt,
	}
}

func (p *producer) Publish(ctx context.Context, msg *Msg) error {
	// 1. 校验 Payload
	if msg.Payload == nil || len(msg.Payload) == 0 {
		return fmt.Errorf("Send msg cannot be empty by stream %q", msg.Stream)
	}

	// 2. 执行 XAdd
	id, er := p.rcli.XAdd(ctx, &redis.XAddArgs{
		Stream: msg.Stream,
		MaxLen: p.options.MaxLen,
		Approx: p.options.Approx,
		Values: msg.Payload,
	}).Result()

	if er != nil {
		return er
	}

	// 3. 回写 ID
	msg.ID = id

	return nil
}
