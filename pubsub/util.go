package pubsub

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"net/url"
	"strconv"
	"time"
)

const DefaultRedisPoolSize = 1024

type Msg struct {
	ID      string
	Stream  string
	Payload map[string]interface{}
}

type RedisOptions struct {
	Scheme   string
	Addr     string
	Host     string
	Port     int
	Password string
	DB       int
	PoolSize int
}

// ParseRedisURL parses the Redis URI and returns RedisOptions.
// It ensures robust error handling and uses default values where necessary.
func ParseRedisURL(uri string) (*RedisOptions, error) {
	parsedURL, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Redis URL: %w", err)
	}

	scheme := parsedURL.Scheme
	host := parsedURL.Hostname()
	portStr := parsedURL.Port()
	password, _ := parsedURL.User.Password()

	// Default Redis port
	port := 6379
	if portStr != "" {
		port, err = strconv.Atoi(portStr)
		if err != nil {
			return nil, fmt.Errorf("invalid port in Redis URL: %w", err)
		}
	}

	// Parse DB index from the path, defaulting to 0
	db := 0
	if len(parsedURL.Path) > 1 {
		dbStr := parsedURL.Path[1:]
		db, err = strconv.Atoi(dbStr)
		if err != nil {
			return nil, fmt.Errorf("invalid DB index in Redis URL: %w", err)
		}
	}

	addr := fmt.Sprintf("%s:%d", host, port)

	return &RedisOptions{
		Scheme:   scheme,
		Addr:     addr,
		Host:     host,
		Port:     port,
		Password: password,
		DB:       db,
		PoolSize: DefaultRedisPoolSize,
	}, nil
}

// NewRedisClient creates a new Redis client based on the provided URI.
// It returns an error if the URI is invalid or if the connection fails.
func NewRedisClient(uri string) (*redis.Client, error) {
	opt, err := ParseRedisURL(uri)
	if err != nil {
		return nil, fmt.Errorf("error parsing Redis URL: %w", err)
	}

	rcli := redis.NewClient(&redis.Options{
		Addr:     opt.Addr,
		Password: opt.Password,
		DB:       opt.DB,
		PoolSize: opt.PoolSize,
	})

	// Verify the connection by pinging the Redis server
	if err := rcli.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return rcli, nil
}

// ChunkArray splits a slice into smaller chunks of the specified size.
// It preallocates the result slice for improved performance.
func ChunkArray[T any](arr []T, size int) [][]T {
	if size <= 0 {
		size = 1 // Prevent division by zero or negative sizes
	}

	chunkCount := (len(arr) + size - 1) / size
	result := make([][]T, 0, chunkCount)

	for i := 0; i < len(arr); i += size {
		end := i + size
		if end > len(arr) {
			end = len(arr)
		}
		result = append(result, arr[i:end])
	}

	return result
}

func SleepContext(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil // 睡眠完成
	case <-ctx.Done():
		return ctx.Err() // 上下文被取消
	}
}
