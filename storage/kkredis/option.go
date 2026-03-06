package kkredis

import (
	"errors"

	"github.com/vvisun/kkdg/utils/kklog"
)

// RedisOption 定义 Redis 连接与连接池等配置。
type RedisOption struct {
	Addr         string // 地址，如 "127.0.0.1:6379"
	Password     string // 密码，空表示无
	DB           int    // 数据库编号，默认 0
	PoolSize     int    // 连接池大小，每 CPU 建议 10 * GOMAXPROCS
	MinIdleConns int    // 最小空闲连接数
	Username     string // Redis 6+ ACL 用户名，空则仅用 Password
}

// DefaultRedisOption 返回默认配置，生产环境请用 WithAddr/WithPassword 等覆盖。
func DefaultRedisOption() RedisOption {
	return RedisOption{
		Addr:         "127.0.0.1:6379",
		Password:     "",
		DB:           0,
		PoolSize:     0, // 0 表示使用 go-redis 默认（10 * runtime.GOMAXPROCS）
		MinIdleConns: 0,
		Username:     "",
	}
}

// CheckOption 校验并补全配置，启动前调用。无效项会写默认并打日志。
func CheckOption(opt *RedisOption) error {
	if opt.Addr == "" {
		return errors.New("redis addr is required")
	}
	if opt.PoolSize < 0 {
		opt.PoolSize = 0
		kklog.Warn("redis pool size is negative, set to 0 (use default)")
	}
	if opt.MinIdleConns < 0 {
		opt.MinIdleConns = 0
		kklog.Warn("redis min idle conns is negative, set to 0")
	}
	if opt.DB < 0 {
		opt.DB = 0
		kklog.Warn("redis db is negative, set to 0")
	}
	return nil
}

// ApplyOptions 链式应用多个 With* 选项，得到最终 RedisOption。
func ApplyOptions(opts ...func(*RedisOption)) RedisOption {
	cfg := DefaultRedisOption()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

func WithAddr(addr string) func(*RedisOption) {
	return func(o *RedisOption) {
		o.Addr = addr
	}
}

func WithPassword(password string) func(*RedisOption) {
	return func(o *RedisOption) {
		o.Password = password
	}
}

func WithDB(db int) func(*RedisOption) {
	return func(o *RedisOption) {
		o.DB = db
	}
}

func WithPoolSize(poolSize int) func(*RedisOption) {
	return func(o *RedisOption) {
		o.PoolSize = poolSize
	}
}

func WithMinIdleConns(n int) func(*RedisOption) {
	return func(o *RedisOption) {
		o.MinIdleConns = n
	}
}

func WithUsername(username string) func(*RedisOption) {
	return func(o *RedisOption) {
		o.Username = username
	}
}
