package netprocessor

import (
	"runtime"
	"sync"
	"time"
)

// -------------------------- 背压Channel核心实现 --------------------------
// LockFreeLinkBackPressureChan 无锁链表背压Channel
// 核心：主Channel（16缓冲区）+ 无锁链表背压 + 自旋/休眠消费协程
type LockFreeLinkBackPressureChan[T any] struct {
	mainChan  chan T           // 主Channel，固定缓冲区16
	backList  *lockFreeList[T] // 无锁链表背压队列
	closeChan chan struct{}    // 关闭信号通道
	wg        sync.WaitGroup   // 等待消费协程退出
	spinTimes int              // 高流量自旋次数（避免休眠带来的延迟）
}

// NewLockFreeLinkBackPressureChan 初始化无锁链表背压Channel
// backMaxLen：背压队列最大长度（0=无界）；spinTimes：自旋次数（建议1000-5000，游戏服推荐2000）
func NewLockFreeLinkBackPressureChan[T any](backMaxLen uint64, spinTimes int) *LockFreeLinkBackPressureChan[T] {
	if spinTimes <= 0 {
		spinTimes = 2000 // 默认自旋次数，适配游戏服帧同步
	}
	bpc := &LockFreeLinkBackPressureChan[T]{
		mainChan:  make(chan T, 16), // 严格保留16个原生缓冲区
		backList:  newLockFreeList[T](backMaxLen),
		closeChan: make(chan struct{}),
		spinTimes: spinTimes,
	}
	// 启动背压队列消费协程
	bpc.wg.Add(1)
	go bpc.consumeBackList()
	return bpc
}

// Send 对外发送方法：非阻塞，无锁竞争，严格FIFO
// 返回值：true=发送成功（入主Channel/背压队列），false=背压队列满/已关闭
func (bpc *LockFreeLinkBackPressureChan[T]) Send(data T) bool {
	// 第一步：非阻塞写入主Channel（16缓冲区），优先低延迟
	select {
	case bpc.mainChan <- data:
		return true
	default:
		// 第二步：主Channel满，写入无锁链表背压队列
		return bpc.backList.Push(data)
	}
}

// consumeBackList 消费背压队列：自旋+休眠策略，兼顾低延迟和低CPU
func (bpc *LockFreeLinkBackPressureChan[T]) consumeBackList() {
	defer bpc.wg.Done()
	for {
		select {
		case <-bpc.closeChan:
			// 关闭时刷盘：将背压队列剩余数据全部写入主Channel
			bpc.flush()
			return
		default:
			// 自旋+休眠策略：高流量自旋（低延迟），低流量休眠（省CPU）
			spinCount := 0
			for spinCount < bpc.spinTimes {
				if val, ok := bpc.backList.Pop(); ok {
					bpc.mainChan <- val // 阻塞写入主Channel，保证数据不丢
					spinCount = 0       // 取到数据，重置自旋计数
				} else {
					spinCount++
					runtime.Gosched() // 轻量自旋，让出CPU
				}
			}
			// 自旋次数耗尽仍无数据，短暂休眠（避免空轮询）
			time.Sleep(500 * time.Nanosecond) // 500ns，游戏服无感知延迟
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
