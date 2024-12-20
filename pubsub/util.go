package pubsub

import (
	"errors"
	"fmt"
	"github.com/eininst/flog"
	"github.com/go-redis/redis/v8"
	"log"
	"net/url"
	"strconv"
)

var REDIS_POOL_SIZE = 4096

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
}

func ParsedRedisURL(uri string) (*RedisOptions, error) {
	parsedURL, err := url.Parse(uri)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to parse Redis URL: %v", err))
	}
	scheme := parsedURL.Scheme
	host := parsedURL.Hostname()
	port := parsedURL.Port()
	password, _ := parsedURL.User.Password()   // 获取密码
	db, er := strconv.Atoi(parsedURL.Path[1:]) // 去掉前导斜杠并转换为整数
	if er != nil {
		return nil, errors.New(fmt.Sprintf("Failed to parse DB index: %v", err))
	}

	_port := 6379
	if port != "" {
		portInt, _er := strconv.Atoi(port)

		if _er != nil {
			return nil, errors.New(fmt.Sprintf("Failed to parse Port index: %v", _er))
		}
		_port = portInt
	}

	addr := fmt.Sprintf("%v:%v", host, port)

	return &RedisOptions{
		Scheme:   scheme,
		Addr:     addr,
		Host:     host,
		Port:     _port,
		Password: password,
		DB:       db,
	}, nil
}

func NewRedisClient(uri string) *redis.Client {
	opt, er := ParsedRedisURL(uri)
	if er != nil {
		log.Fatalf("%v%v%v", flog.Red, er.Error(), flog.Reset)
	}

	poolSize := REDIS_POOL_SIZE

	rcli := redis.NewClient(&redis.Options{
		Addr:     opt.Addr,
		Password: opt.Password,
		DB:       opt.DB,
		PoolSize: poolSize,
		//IdleTimeout:        -1,
		//IdleCheckFrequency: -1,
	})

	return rcli
}

func ChunkArray[T any](arr []T, size int) [][]T {
	var result [][]T
	for i := 0; i < len(arr); i += size {
		// 计算当前子数组的结束索引
		end := i + size
		// 确保结束索引不超过数组长度
		if end > len(arr) {
			end = len(arr)
		}
		// 将当前子数组添加到结果中
		result = append(result, arr[i:end])
	}
	return result
}
