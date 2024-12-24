package pubsub

import (
	"context"
	"errors"
	"github.com/panjf2000/ants/v2"
	"github.com/redis/go-redis/v9"
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
	Name               string
	Group              string
	Workers            int
	ReadCount          int64
	BlockTime          time.Duration
	Timeout            time.Duration
	XautoClaimInterval time.Duration
	BatchSize          int
	NoAck              bool
	Signals            []os.Signal
	ExitWaitTime       time.Duration
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

func WithTimeout(timeout time.Duration) Option {
	return Option{F: func(o *Options) {
		o.Timeout = timeout
	}}
}

func WithXautoClaimInterval(xautoClaimInterval time.Duration) Option {
	return Option{F: func(o *Options) {
		o.XautoClaimInterval = xautoClaimInterval
	}}
}

func WithNoAck(noAck bool) Option {
	return Option{F: func(o *Options) {
		o.NoAck = noAck
	}}
}

func WithSignals(sig ...os.Signal) Option {
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
	rcli, er := NewRedisClient(uri)
	if er != nil {
		clog.Fatal(er)
	}
	return NewConsumerWithClient(rcli, opts...)
}

// NewConsumerWithClient 使用外部注入的 redis.Client
func NewConsumerWithClient(rcli *redis.Client, opts ...Option) Consumer {
	options := &Options{
		Name:               "CONSUMER",
		Group:              "CONSUMER-GROUP",
		Workers:            0,
		ReadCount:          10,
		BlockTime:          time.Second * 5,
		Timeout:            time.Second * 300,
		BatchSize:          16,
		XautoClaimInterval: time.Second * 5,
		NoAck:              false,
		ExitWaitTime:       time.Second * 10,
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
		clog.Printf("XautoClaimInterval=%v Timeout=%v",
			c.options.XautoClaimInterval, c.options.Timeout)
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
				c.xautoClaim(ctxCancel, handlers)
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
	defer func() { close(c.stop) }()

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

			if err != nil {
				if errors.Is(err, redis.Nil) {
					continue
				}
				if errors.Is(err, context.Canceled) {
					continue
				}

				clog.Printf("\033[31mXReadGroup error: %v\033[0m", err)
				_ = SleepContext(ctx, time.Second*3)
				break
			}

			// 处理读取到的消息
			for _, streamResult := range entries {
				fc, ok := hmap[streamResult.Stream]
				if !ok {
					clog.Printf("\033[31mNo handler found for stream %s\033[0m", streamResult.Stream)
					continue
				}
				for _, msg := range streamResult.Messages {
					select {
					case <-ctx.Done():
						return
					default:
						// 创建消息上下文
						hctx := &Context{
							Context: context.Background(),
							Msg: &Msg{
								ID:      msg.ID,
								Stream:  streamResult.Stream,
								Payload: msg.Values,
							},
						}
						c.wg.Add(1)
						_ = c.pool.Submit(func() {
							defer c.wg.Done()
							c.handleMessage(hctx, fc)
						})
					}
				}
			}
		}
	}
}

func (c *consumer) xautoClaim(ctx context.Context, handlers []*handlerFc) {
	ticker := time.NewTicker(c.options.XautoClaimInterval)
	defer ticker.Stop()

	// 预构建 stream 到处理函数的映射
	handlerMap := make(map[string]Function)
	for _, handler := range handlers {
		handlerMap[handler.Stream] = handler.Fc
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 循环认领消息
			// 当一轮处理后都没有消息可认领，说明暂时没有更多 pending，退出循环
			offsetMap := make(map[string]string)
			for _, h := range handlers {
				offsetMap[h.Stream] = "0-0"
			}

			for {
				claimCmds, err := c.executeXAutoClaim(ctx, offsetMap)

				if err != nil {
					if errors.Is(err, redis.Nil) {
						continue
					}
					clog.Printf("\033[31mPipeline XAutoClaim Exec error: %v\033[0m\n", err)
					break
				}

				gotMessages := c.processXAutoClaimResults(ctx, claimCmds, handlerMap, offsetMap)

				// 如果没有获取到任何消息，退出内循环
				if !gotMessages || len(offsetMap) == 0 {
					break
				}
			}
		}
	}
}

// executeXAutoClaim 执行 XAutoClaim 的 Pipeline
func (c *consumer) executeXAutoClaim(ctx context.Context, offsetMap map[string]string) (map[string]*redis.XAutoClaimCmd, error) {
	pipe := c.rcli.Pipeline()
	claimCmds := make(map[string]*redis.XAutoClaimCmd)

	for streamName, start := range offsetMap {
		claimCmds[streamName] = pipe.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   streamName,
			Group:    c.options.Group,
			Consumer: c.options.Name,
			MinIdle:  c.options.Timeout,
			Start:    start,
			Count:    c.options.ReadCount,
		})
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	return claimCmds, nil
}

// processXAutoClaimResults 处理 XAutoClaim 的返回结果
func (c *consumer) processXAutoClaimResults(
	ctx context.Context,
	claimCmds map[string]*redis.XAutoClaimCmd,
	handlerMap map[string]Function,
	offsetMap map[string]string,
) bool {
	gotAnyMessage := false

	for stream, cmd := range claimCmds {
		msgs, nextStart, err := cmd.Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			clog.Printf("XAutoClaim error, stream=%s, err=%v\n", stream, err)
			break
		}

		if len(msgs) == 0 {
			continue
		}

		// 更新 offsetMap
		if len(msgs) < int(c.options.ReadCount) || nextStart == "" {
			delete(offsetMap, stream)
		} else {
			offsetMap[stream] = nextStart
		}

		gotAnyMessage = true
		handlerFunc := handlerMap[stream]

		// 异步处理消息
		for _, msg := range msgs {
			if ctx.Err() != nil {
				return gotAnyMessage
			}

			msgContext := &Context{
				Context: context.Background(),
				Msg: &Msg{
					ID:      msg.ID,
					Stream:  stream,
					Payload: msg.Values,
				},
			}

			c.wg.Add(1)
			_ = c.pool.Submit(func() {
				defer c.wg.Done()
				c.handleMessage(msgContext, handlerFunc)
			})
		}
	}

	return gotAnyMessage
}
