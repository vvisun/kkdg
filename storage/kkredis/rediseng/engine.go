package rediseng

import (
	"context"
	"errors"
	"sync"

	"github.com/redis/go-redis/v9"
	"github.com/vvisun/kkdg/storage/kkredis"
	"github.com/vvisun/kkdg/utils/kklog"
)

var (
	errNotStarted = errors.New("redis not started")
)

// NewRedisEngine 使用 kkredis.RedisOption 创建单机 Redis 引擎。
func NewRedisEngine(opt kkredis.RedisOption) *RedisEngine {
	return &RedisEngine{opts: opt}
}

// NewRedisEngineFromClient 用于测试或已有 *redis.Client 时包装为 RedisEngine，调用方负责 client 生命周期。
func NewRedisEngineFromClient(client *redis.Client) *RedisEngine {
	return &RedisEngine{client: client}
}

// RedisEngine 封装 go-redis 单机客户端，提供启动/关闭/健康检查与实例获取。
type RedisEngine struct {
	opts   kkredis.RedisOption
	client *redis.Client
	mu     sync.RWMutex
}

// GetInst 返回 go-redis 客户端实例，供业务层或 redisop 使用。未启动时返回 nil。
func (e *RedisEngine) GetInst() *redis.Client {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.client
}

func (e *RedisEngine) initClient() bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.client != nil {
		return true
	}

	if err := kkredis.CheckOption(&e.opts); err != nil {
		kklog.Error("redis option check error: ", err.Error())
		return false
	}

	ropt := &redis.Options{
		Addr:     e.opts.Addr,
		Password: e.opts.Password,
		DB:       e.opts.DB,
	}
	if e.opts.Username != "" {
		ropt.Username = e.opts.Username
	}
	if e.opts.PoolSize > 0 {
		ropt.PoolSize = e.opts.PoolSize
	}
	if e.opts.MinIdleConns > 0 {
		ropt.MinIdleConns = e.opts.MinIdleConns
	}

	client := redis.NewClient(ropt)
	e.client = client
	kklog.Infof("初始化 Redis 客户端: Addr=%s DB=%d PoolSize=%d",
		e.opts.Addr, e.opts.DB, e.opts.PoolSize)
	return true
}

// StartUp 初始化连接并 Ping 校验。
func (e *RedisEngine) StartUp() bool {
	if !e.initClient() {
		kklog.Error("redis start up error: create client fail")
		return false
	}
	if !e.Ping() {
		kklog.Error("redis start up error: ping fail")
		return false
	}
	kklog.Info("redis start up success")
	return true
}

// Ping 检查 Redis 连接是否可用。
func (e *RedisEngine) Ping() bool {
	client := e.GetInst()
	if client == nil {
		kklog.Error("redis ping error: ", errNotStarted.Error())
		return false
	}
	if err := client.Ping(context.Background()).Err(); err != nil {
		kklog.Error("redis ping error: ", err.Error())
		return false
	}
	return true
}

// Close 关闭 Redis 连接。
func (e *RedisEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.client != nil {
		if err := e.client.Close(); err != nil {
			kklog.Error("redis close error: ", err.Error())
			return err
		}
		e.client = nil
		kklog.Info("redis close success")
	}
	return nil
}
