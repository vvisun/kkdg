// Package redisop 提供基于 *redis.Client 的常用键值操作封装，配合 rediseng 使用。
//
// 传入 rediseng.RedisEngine.GetInst() 或任意 *redis.Client 即可：
//
//	client := eng.GetInst()
//	redisop.Set(ctx, client, "key", "value", 0)
//	val, err := redisop.Get(ctx, client, "key")
//	redisop.Delete(ctx, client, "key")
//	ok, err := redisop.Exists(ctx, client, "key")
//
// 需要超时/取消时统一使用 context；无 Ctx 后缀的函数内部使用 context.Background()。
package redisop
