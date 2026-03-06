// Package kkredis 提供 Redis 客户端配置与引擎抽象，架构与 storage/kkdb 对齐，可扩展、开箱可用。
//
// # 架构
//
//   - 配置：RedisOption 定义地址、密码、DB、连接池等；DefaultRedisOption 返回默认值；
//     ApplyOptions 与 With* 用于链式构造；CheckOption 用于启动前校验。
//
//   - 引擎实现见子包：
//   - rediseng：单机 go-redis 引擎（StartUp/Close/Ping/GetInst）。
//   - redisop：常用键值操作封装（Get/Set/SetEx/Delete/Exists/Expire/TTL），传入 *redis.Client 即可使用。
//
// # 可扩展
//
// 新增集群、Sentinel 等时，可增加子包（如 rediseng_cluster），复用本包 RedisOption 或定义 ClusterOption，
// 对外提供统一的 StartUp/Close/Ping 与 GetInst()，业务层通过接口或工厂切换实现。
//
// # 示例
//
//	opt := kkredis.ApplyOptions(
//	    kkredis.WithAddr("127.0.0.1:6379"),
//	    kkredis.WithPassword(""),
//	    kkredis.WithDB(0),
//	)
//	eng := rediseng.NewRedisEngine(opt)
//	if !eng.StartUp() { return }
//	defer eng.Close()
//
//	client := eng.GetInst()
//	_ = redisop.Set(ctx, client, "k", "v", 0)
//	val, _ := redisop.Get(ctx, client, "k")
package kkredis
