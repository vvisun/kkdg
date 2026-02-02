package gateway

import (
	"math/rand"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/vvisun/kkdg/kknet/kkgate/gateway/pbgate"
	"google.golang.org/protobuf/proto"
)

// 消息优先级分级
const (
	MsgLevelLow  = 0 // 低优先级：心跳、位置同步（可丢弃）
	MsgLevelHigh = 1 // 高优先级：技能、交易、聊天（必须重试）
	ShardCount   = 8 // 分片数，与pool.go一致
)

// -------------------------- 无锁链表：分片消费的基础 --------------------------
type node[T any] struct {
	val  T
	next *node[T]
}

type lockFreeList[T any] struct {
	head   *node[T]
	tail   *node[T]
	len    uint64
	maxLen uint64
	closed uint32
}

func newLockFreeList[T any](maxLen uint64) *lockFreeList[T] {
	sentinel := &node[T]{}
	return &lockFreeList[T]{
		head:   sentinel,
		tail:   sentinel,
		maxLen: maxLen,
	}
}

func (l *lockFreeList[T]) Enqueue(val T) bool {
	if atomic.LoadUint32(&l.closed) == 1 || (l.maxLen > 0 && atomic.LoadUint64(&l.len) >= l.maxLen) {
		return false
	}
	newNode := &node[T]{val: val}
	for {
		tail := (*node[T])(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.tail))))
		next := (*node[T])(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&tail.next))))
		if tail == (*node[T])(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.tail)))) {
			if next == nil {
				if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&tail.next)), unsafe.Pointer(next), unsafe.Pointer(newNode)) {
					atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&l.tail)), unsafe.Pointer(tail), unsafe.Pointer(newNode))
					atomic.AddUint64(&l.len, 1)
					return true
				}
			} else {
				atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&l.tail)), unsafe.Pointer(tail), unsafe.Pointer(next))
			}
		}
		runtime.Gosched()
	}
}

func (l *lockFreeList[T]) Dequeue() (val T, ok bool) {
	if atomic.LoadUint32(&l.closed) == 1 {
		return val, false
	}
	for {
		head := (*node[T])(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.head))))
		tail := (*node[T])(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.tail))))
		next := (*node[T])(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&head.next))))
		if head == (*node[T])(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.head)))) {
			if head == tail {
				if next == nil {
					return val, false
				}
				atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&l.tail)), unsafe.Pointer(tail), unsafe.Pointer(next))
			} else {
				if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&l.head)), unsafe.Pointer(head), unsafe.Pointer(next)) {
					val = next.val
					atomic.AddUint64(&l.len, ^uint64(0))
					return val, true
				}
			}
		}
		runtime.Gosched()
	}
}

func (l *lockFreeList[T]) Len() uint64 { return atomic.LoadUint64(&l.len) }
func (l *lockFreeList[T]) Close()      { atomic.StoreUint32(&l.closed, 1) }
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

// -------------------------- 网关核心消息结构 --------------------------
type GatewayMsg struct {
	ConnID     uint64        // 客户端连接ID
	LogicID    uint32        // 逻辑服ID
	MsgID      uint32        // 消息ID
	TraceID    []byte        // 16字节TraceID
	PbMsg      proto.Message // Protobuf消息体
	TS         int64         // 接收时间戳（ms）
	RetryCount int32         // 已重试次数
	TimeoutTS  int64         // 超时时间戳（ms）
	MsgLevel   int           // 消息优先级
}

// -------------------------- TraceID生成 --------------------------
var randSource = rand.New(rand.NewSource(time.Now().UnixNano()))

// GenTraceID 生成16字节全局唯一TraceID
func GenTraceID() []byte {
	traceID := make([]byte, pbgate.TraceIDLen)
	randSource.Read(traceID)
	return traceID
}
