package kklru

import (
	"sync"
)

// node 双向链表节点
type node[K comparable, V any] struct {
	prev  *node[K, V]
	next  *node[K, V]
	value V
	key   K
}

// LRU 线程安全的 LRU 缓存
// 使用双向链表 + 哈希表实现，保证 O(1) 的 Get 和 Set 操作
type LRU[K comparable, V any] struct {
	mu       sync.RWMutex
	capacity int
	size     int
	cache    map[K]*node[K, V]
	head     *node[K, V] // 虚拟头节点
	tail     *node[K, V] // 虚拟尾节点
	pool     *sync.Pool  // 节点对象池，用于复用节点减少内存分配
}

// NewLRU 创建一个新的 LRU 缓存
// capacity: 缓存容量，必须 > 0
func NewLRU[K comparable, V any](capacity int) *LRU[K, V] {
	if capacity <= 0 {
		panic("kklru: capacity must be greater than 0")
	}
	lru := &LRU[K, V]{
		capacity: capacity,
		cache:    make(map[K]*node[K, V], capacity),
		head:     &node[K, V]{},
		tail:     &node[K, V]{},
		pool: &sync.Pool{
			New: func() interface{} {
				return &node[K, V]{}
			},
		},
	}
	lru.head.next = lru.tail
	lru.tail.prev = lru.head
	return lru
}

// Get 获取指定 key 的值
// 如果 key 存在，将其移动到链表头部（标记为最近使用）并返回值和 true
// 如果 key 不存在，返回零值和 false
func (l *LRU[K, V]) Get(key K) (V, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	n, ok := l.cache[key]
	if !ok {
		var zero V
		return zero, false
	}

	// 将节点移动到头部
	l.moveToFront(n)
	return n.value, true
}

// Set 设置 key-value 对
// 如果 key 已存在，更新值并移动到头部
// 如果 key 不存在，添加新节点到头部，如果超过容量则删除尾部节点
func (l *LRU[K, V]) Set(key K, value V) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 如果 key 已存在，更新值并移动到头部
	if n, ok := l.cache[key]; ok {
		n.value = value
		l.moveToFront(n)
		return
	}

	// 从对象池获取节点（复用）
	n := l.pool.Get().(*node[K, V])
	n.key = key
	n.value = value
	n.prev = nil
	n.next = nil

	// 如果超过容量，删除尾部节点
	if l.size >= l.capacity {
		l.removeTail()
	}

	// 添加到头部
	l.addToFront(n)
	l.cache[key] = n
	l.size++
}

// Delete 删除指定 key
// 如果 key 存在，删除并返回 true，否则返回 false
func (l *LRU[K, V]) Delete(key K) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	n, ok := l.cache[key]
	if !ok {
		return false
	}

	l.removeNode(n)
	delete(l.cache, key)
	l.size--
	// 将节点回收到对象池
	l.pool.Put(n)
	return true
}

// Clear 清空缓存
func (l *LRU[K, V]) Clear() {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 将所有节点回收到对象池
	for n := l.head.next; n != l.tail; n = n.next {
		l.pool.Put(n)
	}

	l.cache = make(map[K]*node[K, V], l.capacity)
	l.head.next = l.tail
	l.tail.prev = l.head
	l.size = 0
}

// Size 返回当前缓存中的元素数量
func (l *LRU[K, V]) Size() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.size
}

// Capacity 返回缓存容量
func (l *LRU[K, V]) Capacity() int {
	return l.capacity
}

// Contains 检查 key 是否存在（不移动节点）
func (l *LRU[K, V]) Contains(key K) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	_, ok := l.cache[key]
	return ok
}

// Peek 获取值但不移动节点（不更新访问顺序）
func (l *LRU[K, V]) Peek(key K) (V, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	n, ok := l.cache[key]
	if !ok {
		var zero V
		return zero, false
	}
	return n.value, true
}

// moveToFront 将节点移动到链表头部
func (l *LRU[K, V]) moveToFront(n *node[K, V]) {
	l.removeNode(n)
	l.addToFront(n)
}

// addToFront 将节点添加到链表头部
func (l *LRU[K, V]) addToFront(n *node[K, V]) {
	n.prev = l.head
	n.next = l.head.next
	l.head.next.prev = n
	l.head.next = n
}

// removeNode 从链表中移除节点
func (l *LRU[K, V]) removeNode(n *node[K, V]) {
	n.prev.next = n.next
	n.next.prev = n.prev
	n.prev = nil
	n.next = nil
}

// removeTail 移除尾部节点（最久未使用的节点）
func (l *LRU[K, V]) removeTail() {
	if l.tail.prev == l.head {
		return // 链表为空
	}
	last := l.tail.prev
	l.removeNode(last)
	delete(l.cache, last.key)
	l.size--
	// 将节点回收到对象池
	l.pool.Put(last)
}
