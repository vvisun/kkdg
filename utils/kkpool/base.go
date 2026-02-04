package kkpool

// 假设每个池有1024个对象，每个对象1KB，总共1MB。
// 100个池就是100MB。1000个池就是1GB。
// 所以必须做限制，防止池过大耗尽内存
const max_size_for_pool = 4096

// PoolStats 池统计信息
type PoolStats struct {
	Size int64
}

// BasePoolObject 基础池对象，包含引用计数
type BasePoolObject struct {
	poolRef int64 //原子操作的池对象引用标记。为0表示未被池管理，大于0表示在池中，防止重复放入，导致从池中取对象时，取到重复的对象
}

// getPoolRef 获取池引用计数
func (bpo *BasePoolObject) getPoolRef() *int64 {
	return &bpo.poolRef
}

// OnDtor 析构函数
func (bpo *BasePoolObject) OnDtor() {
	// 默认实现为空，子类可以重写
}
