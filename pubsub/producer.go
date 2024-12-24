package pubsub

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
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
	rcli, er := NewRedisClient(uri)
	if er != nil {
		clog.Fatal(er)
	}

	return NewProducerWithClient(rcli, options...)
}

func NewProducerWithClient(rcli *redis.Client, opts ...*ProducerOptions) Producer {
	opt := defaultProducerOptions
	if len(opts) > 0 && opts[0] != nil {
		opt = opts[0]
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

	select {
	case <-ctx.Done():
		// 如果外部已经cancel或超时，此时不必再进行任何操作
		return ctx.Err()
	default:
		// 继续
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
