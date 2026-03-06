package redisop

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vvisun/kkdg/utils/kklog"
)

var (
	errNilClient = errors.New("redis client is nil")
)

func checkClient(client *redis.Client) error {
	if client == nil {
		kklog.Error("redisop: client is nil")
		return errNilClient
	}
	return nil
}

// Get 获取字符串 key 的值。key 不存在时返回 redis.Nil。
func Get(ctx context.Context, client *redis.Client, key string) (string, error) {
	if err := checkClient(client); err != nil {
		return "", err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return client.Get(ctx, key).Result()
}

// Set 设置 key = value，expiration 为 0 表示不过期。
func Set(ctx context.Context, client *redis.Client, key string, value interface{}, expiration time.Duration) error {
	if err := checkClient(client); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return client.Set(ctx, key, value, expiration).Err()
}

// SetEx 设置 key = value 并指定过期时间（秒）。
func SetEx(ctx context.Context, client *redis.Client, key string, value interface{}, seconds int) error {
	return Set(ctx, client, key, value, time.Duration(seconds)*time.Second)
}

// Delete 删除一个或多个 key，返回删除的个数。
func Delete(ctx context.Context, client *redis.Client, keys ...string) (int64, error) {
	if err := checkClient(client); err != nil {
		return 0, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return client.Del(ctx, keys...).Result()
}

// Exists 判断 key 是否存在，返回存在的个数（0 或 1 或更多）。
func Exists(ctx context.Context, client *redis.Client, keys ...string) (int64, error) {
	if err := checkClient(client); err != nil {
		return 0, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return client.Exists(ctx, keys...).Result()
}

// ExistsOne 判断单个 key 是否存在。
func ExistsOne(ctx context.Context, client *redis.Client, key string) (bool, error) {
	n, err := Exists(ctx, client, key)
	return n > 0, err
}

// Expire 设置 key 的过期时间。
func Expire(ctx context.Context, client *redis.Client, key string, expiration time.Duration) (bool, error) {
	if err := checkClient(client); err != nil {
		return false, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return client.Expire(ctx, key, expiration).Result()
}

// TTL 获取 key 剩余生存时间，-1 表示无过期，-2 表示 key 不存在。
func TTL(ctx context.Context, client *redis.Client, key string) (time.Duration, error) {
	if err := checkClient(client); err != nil {
		return 0, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return client.TTL(ctx, key).Result()
}
