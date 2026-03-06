// Package rediseng 提供基于 go-redis 的 Redis 单机客户端引擎，配合 storage/kkredis 配置使用。
//
// 用法：
//
//	opt := kkredis.ApplyOptions(
//	    kkredis.WithAddr("127.0.0.1:6379"),
//	    kkredis.WithPassword(""),
//	    kkredis.WithDB(0),
//	)
//	eng := rediseng.NewRedisEngine(opt)
//	if !eng.StartUp() {
//	    // 启动失败
//	}
//	defer eng.Close()
//
//	// 直接使用 go-redis 能力
//	client := eng.GetInst()
//	client.Set(ctx, "k", "v", 0)
//
//	// 或使用 kkredis/redisop 的封装
//	redisop.Set(ctx, eng.GetInst(), "k", "v", 0)
package rediseng
