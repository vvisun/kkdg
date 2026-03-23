package testredis

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vvisun/kkdg/storage/kkredis"
	"github.com/vvisun/kkdg/storage/kkredis/rediseng"
	"github.com/vvisun/kkdg/storage/kkredis/redisop"
)

/**
# 本机默认
go test ./zothers/tests/testredis/... -v

# 指定地址/密码
set REDIS_ADDR=192.168.1.2:6379
set REDIS_PASSWORD=yourpass
go test ./zothers/tests/testredis/... -v
*/

var testRedisEng *rediseng.RedisEngine

func TestMain(m *testing.M) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	opt := kkredis.ApplyOptions(
		kkredis.WithAddr(addr),
		kkredis.WithPassword(os.Getenv("REDIS_PASSWORD")),
		kkredis.WithDB(0),
	)
	eng := rediseng.NewRedisEngine(opt)
	if !eng.StartUp() {
		// Redis 未启动时跳过集成测试，不失败
		testRedisEng = nil
	} else {
		testRedisEng = eng
	}
	code := m.Run()
	if testRedisEng != nil {
		_ = testRedisEng.Close()
	}
	os.Exit(code)
}

func setupRedisTest(t *testing.T) *rediseng.RedisEngine {
	t.Helper()
	if testRedisEng == nil {
		t.Skip("redis not available (start Redis or set REDIS_ADDR)")
	}
	return testRedisEng
}

func TestRedisEng_StartUp_Ping(t *testing.T) {
	eng := setupRedisTest(t)
	if !eng.Ping() {
		t.Fatal("Ping() = false")
	}
	if eng.GetInst() == nil {
		t.Fatal("GetInst() = nil")
	}
}

func TestRedisop_Set_Get_Delete_Exists(t *testing.T) {
	eng := setupRedisTest(t)
	client := eng.GetInst()
	ctx := context.Background()
	key := "testredis:kkdg:set_get_del"

	// 清理可能残留
	_, _ = redisop.Delete(ctx, client, key)

	err := redisop.Set(ctx, client, key, "v1", 0)
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	val, err := redisop.Get(ctx, client, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if val != "v1" {
		t.Errorf("Get = %q, want v1", val)
	}

	exists, err := redisop.ExistsOne(ctx, client, key)
	if err != nil {
		t.Fatalf("ExistsOne: %v", err)
	}
	if !exists {
		t.Error("ExistsOne = false, want true")
	}

	n, err := redisop.Delete(ctx, client, key)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if n != 1 {
		t.Errorf("Delete = %d, want 1", n)
	}

	_, err = redisop.Get(ctx, client, key)
	if err != redis.Nil {
		t.Errorf("Get after Delete: err = %v, want redis.Nil", err)
	}

	exists, err = redisop.ExistsOne(ctx, client, key)
	if err != nil {
		t.Fatalf("ExistsOne after Delete: %v", err)
	}
	if exists {
		t.Error("ExistsOne after Delete = true, want false")
	}
}

func TestRedisop_SetEx_TTL_Expire(t *testing.T) {
	eng := setupRedisTest(t)
	client := eng.GetInst()
	ctx := context.Background()
	key := "testredis:kkdg:setex_ttl"

	_, _ = redisop.Delete(ctx, client, key)

	err := redisop.SetEx(ctx, client, key, "ex", 60)
	if err != nil {
		t.Fatalf("SetEx: %v", err)
	}
	ttl, err := redisop.TTL(ctx, client, key)
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if ttl <= 0 || ttl > 61*time.Second {
		t.Errorf("TTL = %v, want about 60s", ttl)
	}

	ok, err := redisop.Expire(ctx, client, key, 10*time.Second)
	if err != nil {
		t.Fatalf("Expire: %v", err)
	}
	if !ok {
		t.Error("Expire = false, want true")
	}
	ttl, _ = redisop.TTL(ctx, client, key)
	if ttl <= 0 || ttl > 11*time.Second {
		t.Errorf("TTL after Expire = %v, want about 10s", ttl)
	}

	_, _ = redisop.Delete(ctx, client, key)
}

func TestRedisop_Delete_multi(t *testing.T) {
	eng := setupRedisTest(t)
	client := eng.GetInst()
	ctx := context.Background()
	k1, k2 := "testredis:kkdg:del1", "testredis:kkdg:del2"

	_ = redisop.Set(ctx, client, k1, "a", 0)
	_ = redisop.Set(ctx, client, k2, "b", 0)

	n, err := redisop.Delete(ctx, client, k1, k2)
	if err != nil {
		t.Fatalf("Delete multi: %v", err)
	}
	if n != 2 {
		t.Errorf("Delete multi = %d, want 2", n)
	}
}

func TestRedisop_Exists_multi(t *testing.T) {
	eng := setupRedisTest(t)
	client := eng.GetInst()
	ctx := context.Background()
	k1, k2, k3 := "testredis:kkdg:e1", "testredis:kkdg:e2", "testredis:kkdg:e3"

	_, _ = redisop.Delete(ctx, client, k1, k2, k3)
	_ = redisop.Set(ctx, client, k1, "x", 0)
	_ = redisop.Set(ctx, client, k3, "z", 0)

	n, err := redisop.Exists(ctx, client, k1, k2, k3)
	if err != nil {
		t.Fatalf("Exists multi: %v", err)
	}
	if n != 2 {
		t.Errorf("Exists(k1,k2,k3) = %d, want 2", n)
	}

	_, _ = redisop.Delete(ctx, client, k1, k2, k3)
}

func TestRedisEng_NewFromClient(t *testing.T) {
	eng := setupRedisTest(t)
	client := eng.GetInst()
	wrapped := rediseng.NewRedisEngineFromClient(client)
	if wrapped.GetInst() != client {
		t.Error("NewRedisEngineFromClient: GetInst() != original client")
	}
	// 不 Close wrapped，避免关掉共享 client
}
