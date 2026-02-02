package kkchan

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// -------------------------- 背压Channel核心实现 --------------------------
// LockFreeLinkBackPressureChan 无锁链表背压Channel
// 核心：主Channel（16缓冲区）+ 无锁链表背压 + 自旋/休眠消费协程
type LockFreeLinkBackPressureChan[T any] struct {
	mainChan  chan T           // 主Channel，固定缓冲区16
	backList  *lockFreeList[T] // 无锁链表背压队列
	closeChan chan struct{}    // 关闭信号通道
	wakeCh    chan struct{}    // 唤醒信号：队列从空变为非空时通知消费协程
	wg        sync.WaitGroup   // 等待消费协程退出
	spinTimes int              // 高流量自旋次数（避免休眠带来的延迟）

	idle uint32 // 1=消费协程处于等待(wake)状态
}

// NewLockFreeLinkBackPressureChan 初始化无锁链表背压Channel
// backMaxLen：背压队列最大长度（0=无界）；
// spinTimes：自旋次数（建议1000-5000，游戏服推荐2000）
func NewLockFreeLinkBackPressureChan[T any](backMaxLen uint64, spinTimes int) *LockFreeLinkBackPressureChan[T] {
	if spinTimes <= 0 {
		spinTimes = 2000 // 默认自旋次数，适配游戏服帧同步
	}
	bpc := &LockFreeLinkBackPressureChan[T]{
		mainChan:  make(chan T, 16), // 严格保留16个原生缓冲区
		backList:  newLockFreeList[T](backMaxLen),
		closeChan: make(chan struct{}),
		wakeCh:    make(chan struct{}, 1),
		spinTimes: spinTimes,
	}
	// 启动背压队列消费协程
	bpc.wg.Add(1)
	go bpc.consumeBackList()
	return bpc
}

// Send 对外发送方法：非阻塞，无锁竞争，严格FIFO
// 返回值：0=发送成功（入背压队列，按序转发到主Channel），1=背压队列满，2=已关闭
func (bpc *LockFreeLinkBackPressureChan[T]) Send(data T) int {
	// If closing, reject new sends to avoid data loss.
	select {
	case <-bpc.closeChan:
		return 2
	default:
	}

	// Strict FIFO under multi-producer requires a single enqueue path.
	// Always enqueue into the lock-free FIFO list, and let the consumer goroutine
	// forward to mainChan in order.
	if !bpc.backList.Push(data) {
		return 1
	}
	// If consumer is waiting, nudge it. Non-blocking to avoid producer stalls.
	if atomic.LoadUint32(&bpc.idle) == 1 {
		select {
		case bpc.wakeCh <- struct{}{}:
		default:
		}
	}
	return 0
}

// consumeBackList 消费背压队列：自旋+休眠策略，兼顾低延迟和低CPU
func (bpc *LockFreeLinkBackPressureChan[T]) consumeBackList() {
	defer bpc.wg.Done()
	for {
		// 爆发流量：少量自旋抢延迟
		spinCount := 0
		for spinCount < bpc.spinTimes {
			select {
			case <-bpc.closeChan:
				bpc.flush()
				return
			default:
			}
			if val, ok := bpc.backList.Pop(); ok {
				bpc.mainChan <- val // 阻塞写入主Channel，保证数据不丢
				spinCount = 0
				continue
			}
			spinCount++
			runtime.Gosched()
		}

		// 空闲：不轮询，进入等待；为避免“丢唤醒”，标记 idle 后再二次确认队列。
		atomic.StoreUint32(&bpc.idle, 1)
		if val, ok := bpc.backList.Pop(); ok {
			atomic.StoreUint32(&bpc.idle, 0)
			bpc.mainChan <- val
			continue
		}

		select {
		case <-bpc.closeChan:
			atomic.StoreUint32(&bpc.idle, 0)
			bpc.flush()
			return
		case <-bpc.wakeCh:
			atomic.StoreUint32(&bpc.idle, 0)
			// continue
		}
	}
}

// flush 刷盘：关闭时将背压队列所有剩余数据写入主Channel
func (bpc *LockFreeLinkBackPressureChan[T]) flush() {
	bpc.backList.Close()
	// 批量取出剩余数据，减少原子操作次数
	remaining := bpc.backList.Flush()
	for _, val := range remaining {
		bpc.mainChan <- val
	}

	// drain any remaining that may have been enqueued before Close()
	for {
		val, ok := bpc.backList.Pop()
		if !ok {
			break
		}
		bpc.mainChan <- val
	}
}

// Chan 获取原生只读主Channel，供外部消费（对接Actor单协程消费）
func (bpc *LockFreeLinkBackPressureChan[T]) Chan() <-chan T {
	return bpc.mainChan
}

// BackLen 获取背压队列当前长度（近似值，用于监控告警）
func (bpc *LockFreeLinkBackPressureChan[T]) BackLen() uint64 {
	return bpc.backList.Len()
}

// Close 优雅关闭：保证主Channel+背压队列所有数据都被消费，无丢失
func (bpc *LockFreeLinkBackPressureChan[T]) Close() {
	close(bpc.closeChan)
	bpc.wg.Wait()
	close(bpc.mainChan)
}
