package pubsub

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"github.com/panjf2000/ants/v2"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"
)

var clog = log.New(os.Stderr, "[Spin] ", log.Lmsgprefix|log.Ldate|log.Ltime)

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
	ExitWaitTime     time.Duration
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

func WithExitWaitTime(exitWaitTime time.Duration) Option {
	return Option{F: func(o *Options) {
		o.ExitWaitTime = exitWaitTime
	}}
}

type Function func(ctx *Context) error

type handlerFc struct {
	Stream string
	Fc     Function
}

type Consumer interface {
	Handler(stream string, fc Function)
	Spin()
	Shutdown()
}

type consumer struct {
	rcli     *redis.Client
	handlers []*handlerFc
	cancels  []context.CancelFunc
	stop     chan int
	options  *Options
	pool     *ants.Pool
	wg       sync.WaitGroup
}

// NewConsumer 使用 URI 创建 redis.Client
func NewConsumer(uri string, opts ...Option) Consumer {
	return NewConsumerWithClient(NewRedisClient(uri), opts...)
}

// NewConsumerWithClient 使用外部注入的 redis.Client
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
		ExitWaitTime:     time.Second * 10,
	}
	options.Apply(opts)

	p, _ := ants.NewPool(options.Workers)

	return &consumer{
		rcli:     rcli,
		options:  options,
		stop:     make(chan int, 1),
		handlers: make([]*handlerFc, 0),
		cancels:  make([]context.CancelFunc, 0),
		pool:     p,
	}
}

func (c *consumer) Handler(stream string, fc Function) {
	c.handlers = append(c.handlers, &handlerFc{
		Stream: stream,
		Fc:     fc,
	})
}

func (c *consumer) Spin() {
	ctx := context.Background()

	clog.Printf("NoAck=%v Workers=%v ReadCount=%v BlockTime=%v BatchSize=%v",
		c.options.NoAck, c.options.Workers, c.options.ReadCount, c.options.BlockTime, c.options.BatchSize)
	if !c.options.NoAck {
		clog.Printf("XpendingInterval=%v Timeout=%v MaxRetries=%v",
			c.options.XpendingInterval, c.options.Timeout, c.options.MaxRetries)
	}

	// 确保 Stream 和 Group 都已创建
	for _, h := range c.handlers {
		_ = c.rcli.XGroupCreateMkStream(ctx, h.Stream, c.options.Group, "0").Err()
	}

	chunkHandlers := ChunkArray(c.handlers, c.options.BatchSize)
	clog.Printf("Start %v goroutines to perform XRead from Redis...", len(chunkHandlers))
	if !c.options.NoAck {
		clog.Printf("Start %v goroutines to perform XPending from Redis...", len(chunkHandlers))
	}

	// 按批次启动 goroutine
	for _, handlersChunk := range chunkHandlers {
		ctxCancel, cancel := context.WithCancel(ctx)
		c.cancels = append(c.cancels, cancel)

		// XRead
		c.wg.Add(1)
		go func(handlers []*handlerFc) {
			defer c.wg.Done()
			c.xread(ctxCancel, handlers)
		}(handlersChunk)

		// XPending
		if !c.options.NoAck {
			c.wg.Add(1)
			go func(handlers []*handlerFc) {
				defer c.wg.Done()
				c.xpending(ctxCancel, handlers)
			}(handlersChunk)
		}
	}

	// 监听退出信号，进入优雅退出
	go func() {
		quit := make(chan os.Signal, 1)
		if len(c.options.Signals) == 0 {
			signal.Notify(quit, syscall.SIGTERM)
		} else {
			signal.Notify(quit, c.options.Signals...)
		}
		<-quit

		clog.Println("Received signal, start shutdown...")
		c.Shutdown()
	}()

	// 阻塞等待 stop 信号
	<-c.stop
}

// Shutdown 优雅退出
func (c *consumer) Shutdown() {
	defer func() { c.stop <- 1 }()

	// 关闭协程池
	defer c.pool.Release()

	// 取消所有 context
	for _, cancel := range c.cancels {
		cancel()
	}

	// 等待剩余任务处理完毕
	ctx, cancel := context.WithTimeout(context.Background(), c.options.ExitWaitTime)
	defer cancel()

	done := make(chan struct{}, 1)
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		clog.Printf("Graceful shutdown success")
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			clog.Println("Shutdown timeout exceeded")
		}
	}
}

// handleMessage 包装了业务逻辑的执行和异常处理，保证最终根据执行情况进行 XAck
func (c *consumer) handleMessage(hctx *Context, fc Function) {
	defer func() {
		// 默认需要 ack，除非出现 panic
		isAck := true
		if r := recover(); r != nil {
			isAck = false
			buf := make([]byte, 1024)
			n := runtime.Stack(buf, false)
			clog.Printf("\033[31mPanic: %v\nStack trace:\n%s\033[0m", r, string(buf[:n]))
		}

		// 如果 MaxRetries<=0 表示不再进行重试处理，可视为一定 ack
		if c.options.MaxRetries <= 0 {
			isAck = true
		}

		// 如果不是 NoAck，则进行 ack
		if isAck && !c.options.NoAck {
			if err := c.rcli.XAck(hctx, hctx.Stream, c.options.Group, hctx.ID).Err(); err != nil {
				clog.Printf("\033[31mXAck error (stream=%s, id=%s): %v\033[0m",
					hctx.Stream, hctx.ID, err)
			}
		}
	}()

	if err := fc(hctx); err != nil {
		// 让 handleMessage 的 deferred recover 捕获
		panic(err)
	}
}

// xread 循环消费 Redis Stream
func (c *consumer) xread(ctx context.Context, handlers []*handlerFc) {
	hmap := make(map[string]Function, len(handlers))
	streams := make([]string, 0, 2*len(handlers))

	for _, h := range handlers {
		hmap[h.Stream] = h.Fc
		streams = append(streams, h.Stream)
	}
	// xreadgroup 消费 ">" 专门用于读取新消息
	for range handlers {
		streams = append(streams, ">")
	}

	for {
		select {
		case <-ctx.Done():
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

			if shouldContinueOnErr(err) {
				continue
			} else if err != nil {
				clog.Printf("\033[31mXReadGroup error: %v\033[0m", err)
				time.Sleep(time.Second * 3)
				continue
			}

			// 处理读取到的消息
			for _, streamResult := range entries {
				fc, ok := hmap[streamResult.Stream]
				if !ok {
					clog.Printf("\033[31mNo handler found for stream %s\033[0m", streamResult.Stream)
					continue
				}
				for _, msg := range streamResult.Messages {
					hctx := &Context{
						Context: context.Background(),
						Msg: &Msg{
							ID:      msg.ID,
							Stream:  streamResult.Stream,
							Payload: msg.Values,
						},
					}
					c.wg.Add(1)
					if errSubmit := c.pool.Submit(func() {
						defer c.wg.Done()
						c.handleMessage(hctx, fc)
					}); errSubmit != nil {
						// ants pool 已关闭或队列已满
						clog.Printf("\033[31mSubmit to pool error: %v\033[0m", errSubmit)
						c.wg.Done()
					}
				}
			}
		}
	}
}

// xpending 处理挂起的消息 (Retry)
func (c *consumer) xpending(ctx context.Context, handlers []*handlerFc) {
	hmap := make(map[string]Function, len(handlers))
	for _, h := range handlers {
		hmap[h.Stream] = h.Fc
	}

	ticker := time.NewTicker(c.options.XpendingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			streamNames := make([]string, 0, len(handlers))

			// 使用 Pipeline 同时获取多个 Stream 的挂起信息
			cmds, err := c.rcli.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				for _, h := range handlers {
					pipe.XPendingExt(ctx, &redis.XPendingExtArgs{
						Stream: h.Stream,
						Group:  c.options.Group,
						Idle:   c.options.Timeout,
						Start:  "0",
						End:    "+",
						Count:  c.options.ReadCount,
					})
					streamNames = append(streamNames, h.Stream)
				}
				return nil
			})

			if shouldContinueOnErr(err) {
				continue
			} else if err != nil {
				clog.Printf("\033[31mXPending error: %v\033[0m", err)
				time.Sleep(time.Second * 3)
				continue
			}

			// 逐个处理 XPending 结果
			for i, cmd := range cmds {
				pendingEntries, _ := cmd.(*redis.XPendingExtCmd).Result()
				streamName := streamNames[i]

				// 将需要删除的 ID 和需要重新 claim 的 ID 分开
				var toDelete, toClaim []string
				for _, pending := range pendingEntries {
					if pending.RetryCount > c.options.MaxRetries {
						toDelete = append(toDelete, pending.ID)
					} else {
						toClaim = append(toClaim, pending.ID)
					}
				}

				// XAck 删除超出重试次数的消息
				if len(toDelete) > 0 {
					if errAck := c.rcli.XAck(ctx, streamName, c.options.Group, toDelete...).Err(); errAck != nil {
						clog.Printf(
							"\033[31mXAck error after exceeding max retries (stream=%s, ids=%v): %v\033[0m",
							streamName, toDelete, errAck,
						)
					}
				}

				// XClaim 重新获取需要重试的消息
				if len(toClaim) > 0 {
					fc, ok := hmap[streamName]
					if !ok {
						clog.Printf("\033[35mNo handler for stream %s\033[0m", streamName)
						continue
					}
					xmsgs, er := c.rcli.XClaim(ctx, &redis.XClaimArgs{
						Stream:   streamName,
						Group:    c.options.Group,
						Consumer: c.options.Name,
						MinIdle:  c.options.Timeout,
						Messages: toClaim,
					}).Result()
					if er != nil {
						clog.Printf("\033[31mXClaim error (stream=%s): %v\033[0m", streamName, er)
						continue
					}

					// 重新处理拿回来的消息
					for _, msg := range xmsgs {
						hctx := &Context{
							Context: context.Background(),
							Msg: &Msg{
								ID:      msg.ID,
								Stream:  streamName,
								Payload: msg.Values,
							},
						}
						c.wg.Add(1)
						if errSubmit := c.pool.Submit(func() {
							defer c.wg.Done()
							c.handleMessage(hctx, fc)
						}); errSubmit != nil {
							clog.Printf("\033[31mSubmit to pool error: %v\033[0m", errSubmit)
							c.wg.Done()
						}
					}
				}
			}
		}
	}
}

// shouldContinueOnErr 用于判断遇到 redis.Nil / context.Cancelled / context.DeadlineExceeded 等是否继续循环
func shouldContinueOnErr(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, redis.Nil) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}
