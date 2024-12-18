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

func NewProducer(rcli *redis.Client) Producer {
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
		Stream: fmt.Sprintf("%s%s", "", stream),
		MaxLen: 2048,
		Approx: true,
		ID:     "*",
		Values: msg,
	}).Err()
}
