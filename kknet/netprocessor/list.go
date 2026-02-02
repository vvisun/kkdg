package netprocessor

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

// -------------------------- 无锁链表核心实现 --------------------------
// node 无锁链表节点，存储单条数据
type node[T any] struct {
	val  T
	next *node[T]
}

// lockFreeList 无锁单向链表，作为背压缓冲区核心
type lockFreeList[T any] struct {
	head   *node[T] // 头指针：出队位置（原子操作）
	tail   *node[T] // 尾指针：入队位置（原子操作）
	len    uint64   // 当前队列长度（原子操作，监控用）
	maxLen uint64   // 最大长度（0=无界）
	closed uint32   // 关闭标记（原子操作，0=未关，1=已关）
}

// newLockFreeList 新建无锁链表，maxLen=0则为无界
func newLockFreeList[T any](maxLen uint64) *lockFreeList[T] {
	// 哨兵节点（避免头/尾指针空指针判断）
	sentinel := &node[T]{}
	return &lockFreeList[T]{
		head:   sentinel,
		tail:   sentinel,
		maxLen: maxLen,
	}
}

// Enqueue 无锁入队，返回true=成功，false=队列满/已关闭
func (l *lockFreeList[T]) Enqueue(val T) bool {
	// 快速判断：已关闭/有界且满，直接返回false
	if atomic.LoadUint32(&l.closed) == 1 || (l.maxLen > 0 && atomic.LoadUint64(&l.len) >= l.maxLen) {
		return false
	}

	// 新建节点
	newNode := &node[T]{val: val}
	for {
		tail := (*node[T])(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.tail))))
		next := (*node[T])(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&tail.next))))

		// 尾指针未变，尝试CAS更新尾指针的next
		if tail == (*node[T])(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.tail)))) {
			if next == nil {
				// CAS成功：节点加入链表尾部
				if atomic.CompareAndSwapPointer(
					(*unsafe.Pointer)(unsafe.Pointer(&tail.next)),
					unsafe.Pointer(next),
					unsafe.Pointer(newNode),
				) {
					// 尝试将尾指针移到新节点（允许失败，其他协程会帮忙移动）
					atomic.CompareAndSwapPointer(
						(*unsafe.Pointer)(unsafe.Pointer(&l.tail)),
						unsafe.Pointer(tail),
						unsafe.Pointer(newNode))
					atomic.AddUint64(&l.len, 1) // 长度+1
					return true
				}
			} else {
				// 尾指针滞后，帮忙移动到next
				atomic.CompareAndSwapPointer(
					(*unsafe.Pointer)(unsafe.Pointer(&l.tail)), unsafe.Pointer(tail), unsafe.Pointer(next))
			}
		}
		runtime.Gosched() // 轻量让出CPU，避免自旋过度
	}
}

// Dequeue 无锁出队，返回val=数据，ok=true=成功，ok=false=空/已关闭
func (l *lockFreeList[T]) Dequeue() (val T, ok bool) {
	if atomic.LoadUint32(&l.closed) == 1 {
		return val, false
	}

	for {
		head := (*node[T])(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.head))))
		tail := (*node[T])(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.tail))))
		next := (*node[T])(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&head.next))))

		// 头指针未变，判断队列是否为空
		if head == (*node[T])(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.head)))) {
			if head == tail {
				if next == nil {
					return val, false // 队列为空
				}
				// 尾指针滞后，帮忙移动
				atomic.CompareAndSwapPointer(
					(*unsafe.Pointer)(unsafe.Pointer(&l.tail)),
					unsafe.Pointer(tail),
					unsafe.Pointer(next),
				)
			} else {
				// 尝试CAS更新头指针，取出next节点数据
				if atomic.CompareAndSwapPointer(
					(*unsafe.Pointer)(unsafe.Pointer(&l.head)),
					unsafe.Pointer(head),
					unsafe.Pointer(next),
				) {
					val = next.val
					atomic.AddUint64(&l.len, ^uint64(0)) // 长度-1（原子操作）
					return val, true
				}
			}
		}
		runtime.Gosched()
	}
}

// Len 获取队列当前长度（近似值，高并发下非精确，仅用于监控）
func (l *lockFreeList[T]) Len() uint64 {
	return atomic.LoadUint64(&l.len)
}

// Close 优雅关闭队列，关闭后无法入队，可继续出队
func (l *lockFreeList[T]) Close() {
	atomic.StoreUint32(&l.closed, 1)
}

// Flush 清空队列，返回所有剩余数据（关闭后刷盘用）
func (l *lockFreeList[T]) Flush() []T {
	var res []T
	for {
		val, ok := l.Dequeue()
		if !ok {
			break
		}
		res = append(res, val)
	}
	return res
}
