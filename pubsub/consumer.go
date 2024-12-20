package pubsub

import (
	"context"
	"errors"
	"fmt"
	"github.com/eininst/flog"
	"github.com/go-redis/redis/v8"
	"github.com/panjf2000/ants/v2"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Context struct {
	context.Context
	*Msg
}

type Option struct {
	F func(o *Options)
}

type Options struct {
	Name             string
	Group            string
	Workers          int
	ReadCount        int64
	BlockTime        time.Duration
	MaxRetries       int64
	Timeout          time.Duration
	XpendingInterval time.Duration
	BatchSize        int
	NoAck            bool
	Signals          []os.Signal
}

func (o *Options) Apply(opts []Option) {
	for _, op := range opts {
		op.F(o)
	}
}

func WithName(name string) Option {
	return Option{F: func(o *Options) {
		o.Name = name
	}}
}

func WithGroup(group string) Option {
	return Option{F: func(o *Options) {
		o.Group = group
	}}
}

func WithWorkers(workers int) Option {
	return Option{F: func(o *Options) {
		o.Workers = workers
	}}
}

func WithReadCount(readCount int64) Option {
	return Option{F: func(o *Options) {
		o.ReadCount = readCount
	}}
}

func WithBatchSize(batchSize int) Option {
	return Option{F: func(o *Options) {
		o.BatchSize = batchSize
	}}
}

func WithBlockTime(blockTime time.Duration) Option {
	return Option{F: func(o *Options) {
		o.BlockTime = blockTime
	}}
}

func WithMaxRetries(maxRetries int64) Option {
	return Option{F: func(o *Options) {
		o.MaxRetries = maxRetries
	}}
}

func WithTimeout(timeout time.Duration) Option {
	return Option{F: func(o *Options) {
		o.Timeout = timeout
	}}
}

func WithXpendingInterval(xpendingInterval time.Duration) Option {
	return Option{F: func(o *Options) {
		o.XpendingInterval = xpendingInterval
	}}
}

func WithNoAck(noAck bool) Option {
	return Option{F: func(o *Options) {
		o.NoAck = noAck
	}}
}

func WithSignal(sig ...os.Signal) Option {
	return Option{F: func(o *Options) {
		o.Signals = sig
	}}
}

type Function func(ctx *Context) error

type HandlerContext struct {
	Stream string
	Fc     Function
}

type Consumer interface {
	Handler(stream string, fc Function)
	Spin()
}

type consumer struct {
	rcli     *redis.Client
	handlers []*HandlerContext
	cancels  []context.CancelFunc
	stop     chan int
	options  *Options
	pool     *ants.Pool
	wg       sync.WaitGroup
	signals  []os.Signal
}

func NewConsumer(uri string, opts ...Option) Consumer {

	return NewConsumerWithClient(NewRedisClient(uri), opts...)
}

func NewConsumerWithClient(rcli *redis.Client, opts ...Option) Consumer {
	options := &Options{
		Name:             "CONSUMER",
		Group:            "CONSUMER-GROUP",
		Workers:          0,
		ReadCount:        10,
		BlockTime:        time.Second * 5,
		MaxRetries:       64,
		Timeout:          time.Second * 300,
		BatchSize:        16,
		XpendingInterval: time.Second * 3,
		NoAck:            false,
	}
	options.Apply(opts)

	p, _ := ants.NewPool(options.Workers)

	return &consumer{
		rcli:     rcli,
		options:  options,
		stop:     make(chan int, 1),
		handlers: []*HandlerContext{},
		cancels:  []context.CancelFunc{},
		pool:     p,
	}
}

func (c *consumer) Handler(stream string, fc Function) {
	c.handlers = append(c.handlers, &HandlerContext{
		Stream: stream,
		Fc:     fc,
	})
}

func (c *consumer) Spin() {
	ctx := context.Background()

	clog := flog.New(flog.Config{
		Format: fmt.Sprintf("${time} %s[Spin]%s ${msg}${fields}", flog.GreenBold, flog.Reset),
	})

	logFields := flog.Fields{
		"Workers":   c.options.Workers,
		"NoAck":     c.options.NoAck,
		"ReadCount": c.options.ReadCount,
		"BlockTime": c.options.BlockTime,
		"BatchSize": c.options.BatchSize,
	}
	clog.With(logFields).Infof("")

	if !c.options.NoAck {
		clog.With(flog.Fields{
			"Xpending":   c.options.XpendingInterval,
			"Timeout":    c.options.Timeout,
			"MaxRetries": c.options.MaxRetries,
		}).Infof("")
	}

	for _, h := range c.handlers {
		c.rcli.XGroupCreateMkStream(ctx, h.Stream, c.options.Group, "0")
	}

	chunkHandlers := ChunkArray(c.handlers, c.options.BatchSize)

	slog := flog.New(flog.Config{
		Format: fmt.Sprintf("${time} %s[Streams]%s ${msg}${fields}", flog.GreenBold, flog.Reset),
	})

	for _, handlers := range chunkHandlers {
		ctxCancel, cancel := context.WithCancel(ctx)
		c.cancels = append(c.cancels, cancel)

		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.xread(ctxCancel, handlers)
		}()

		names := []string{}
		for _, h := range c.handlers {
			names = append(names, h.Stream)
		}
		nameStr := strings.Join(names, ",")
		slog.Info(nameStr)

		if !c.options.NoAck {
			c.wg.Add(1)
			go func() {
				defer c.wg.Done()
				c.xpending(ctxCancel, handlers)
			}()
		}
	}

	go func() {
		quit := make(chan os.Signal)

		if len(c.options.Signals) == 0 {
			signal.Notify(quit, syscall.SIGTERM)
		} else {
			signal.Notify(quit, c.options.Signals...)
		}

		<-quit
		clog.Info(flog.YellowBold, "Shutdown...", flog.Reset)
		c.Shutdown()
	}()

	<-c.stop
	clog.Info(flog.GreenBold, "Graceful shutdown success!", flog.Reset)
}

func (c *consumer) Shutdown() {
	defer func() { c.stop <- 1 }()
	defer c.pool.Release()

	for _, cancel := range c.cancels {
		cancel()
	}
	c.wg.Wait()
}

func (c *consumer) xread(ctx context.Context, handlers []*HandlerContext) {
	hmap := make(map[string]Function)
	streams := []string{}

	for _, h := range handlers {
		hmap[h.Stream] = h.Fc
		streams = append(streams, h.Stream)
	}
	for range handlers {
		streams = append(streams, ">")
	}

	var wg sync.WaitGroup

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		default:
			entries, err := c.rcli.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    c.options.Group,
				Consumer: c.options.Name,
				Streams:  streams,
				Count:    c.options.ReadCount,
				Block:    c.options.BlockTime,
				NoAck:    c.options.NoAck,
			}).Result()

			if err != nil {
				if errors.Is(err, redis.Nil) {
					continue
				} else if errors.Is(err, context.Canceled) {
					continue
				} else {
					flog.Error(err)
					time.Sleep(time.Second * 3)
					continue
				}
			}

			if len(entries) == 0 {
				continue
			}
			for _, entry := range entries {
				fc, exists := hmap[entry.Stream]
				if !exists {
					flog.Errorf("Stream %s not found handler", entry.Stream)
					continue
				}

				for _, msg := range entry.Messages {
					hctx := &Context{
						Context: ctx,
						Msg: &Msg{
							ID:      msg.ID,
							Stream:  entry.Stream,
							Payload: msg.Values,
						},
					}

					wg.Add(1)
					_ = c.pool.Submit(func() {
						isAck := true
						defer wg.Done()
						defer func() {
							if r := recover(); r != nil {
								isAck = false
								buf := make([]byte, 1024)
								n := runtime.Stack(buf, false)
								flog.Errorf("%s%v\nStack trace:\n%s%s\n", flog.Red, r, buf[:n], flog.Reset)
							}

							if c.options.MaxRetries <= 0 {
								isAck = true
							}

							if isAck && !c.options.NoAck {
								xerr := c.rcli.XAck(ctx, entry.Stream, c.options.Group, msg.ID).Err()
								if xerr != nil {
									flog.Errorf("error acknowledging after failed XAck for %v stream and %v message",
										entry.Stream, msg.ID)
								}
							}
						}()

						if _er := fc(hctx); _er != nil {
							panic(_er)
						}
					})
				}
			}
		}
	}
}

func (c *consumer) xpending(ctx context.Context, handlers []*HandlerContext) {
	hmap := make(map[string]Function)
	for _, h := range handlers {
		hmap[h.Stream] = h.Fc
	}

	var wg sync.WaitGroup
	ticker := time.NewTicker(c.options.XpendingInterval)

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case <-ticker.C:
			stream_list := []string{}

			cmds, err := c.rcli.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				for _, h := range c.handlers {
					pipe.XPendingExt(ctx, &redis.XPendingExtArgs{
						Stream: h.Stream,
						Group:  c.options.Group,
						Idle:   c.options.Timeout,
						Start:  "0",
						End:    "+",
						Count:  c.options.ReadCount,
					})
					stream_list = append(stream_list, h.Stream)
				}
				return nil
			})
			if err != nil {
				if errors.Is(err, redis.Nil) {
					continue
				} else if errors.Is(err, context.Canceled) {
					continue
				} else {
					time.Sleep(time.Second * 3)
					continue
				}
			}

			xdel_ids := []string{}
			xclaim_ids := []string{}

			for i, cmd := range cmds {
				pcmds, _ := cmd.(*redis.XPendingExtCmd).Result()
				streamName := stream_list[i]

				for _, pcmd := range pcmds {
					if pcmd.RetryCount > c.options.MaxRetries {
						xdel_ids = append(xdel_ids, pcmd.ID)
					} else {
						xclaim_ids = append(xclaim_ids, pcmd.ID)
					}
				}

				if len(xdel_ids) > 0 {
					err = c.rcli.XAck(ctx, streamName, c.options.Group, xdel_ids...).Err()
					if err != nil {
						flog.Errorf("error acknowledging after failed XAck for %v stream and %v message",
							streamName, xdel_ids)
					}
				}

				if len(xclaim_ids) > 0 {
					fc, exists := hmap[streamName]
					if !exists {
						flog.Errorf("Stream %s not found handler", streamName)
						continue
					}

					xmsgs, er := c.rcli.XClaim(ctx, &redis.XClaimArgs{
						Stream:   streamName,
						Group:    c.options.Group,
						Consumer: c.options.Name,
						MinIdle:  c.options.Timeout,
						Messages: xclaim_ids,
					}).Result()

					if er != nil {
						flog.Error("XRangeN Error:", er)
						continue
					}
					for _, msg := range xmsgs {

						hctx := &Context{
							Context: ctx,
							Msg: &Msg{
								ID:      msg.ID,
								Stream:  streamName,
								Payload: msg.Values,
							},
						}

						wg.Add(1)
						_ = c.pool.Submit(func() {
							isAck := true
							defer wg.Done()
							defer func() {
								if r := recover(); r != nil {
									isAck = false
									buf := make([]byte, 1024)
									n := runtime.Stack(buf, false)
									flog.Errorf("%s%v\nStack trace:\n%s%s\n", flog.Red, r, buf[:n], flog.Reset)
								}

								if isAck {
									xerr := c.rcli.XAck(ctx, streamName, c.options.Group, msg.ID).Err()
									if xerr != nil {
										flog.Errorf("error acknowledging after failed XAck for %v stream and %v message",
											streamName, msg.ID)
									}
								}
							}()

							if _er := fc(hctx); _er != nil {
								panic(_er)
							}
						})
					}
				}
			}
		}
	}
}
