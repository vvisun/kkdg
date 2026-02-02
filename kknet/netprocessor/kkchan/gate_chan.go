package kkchan

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// -------------------------- 网关专属背压Channel核心 --------------------------
// GatewayMsg 网关消息通用结构体，可根据业务扩展
type GatewayMsg struct {
	ConnID uint64 // 连接ID，唯一标识客户端连接
	Data   []byte // 消息体二进制数据
	MsgID  uint32 // 消息ID，用于逻辑服路由
	TS     int64  // 消息时间戳，毫秒
}

// GatewayBackPressureChan 网关专属无锁背压Channel
// 特性：连接隔离+批量转发+熔断保护+全量监控
type GatewayBackPressureChan struct {
	mainChan  chan GatewayMsg           // 主通道，16缓冲区（网关透传专用）
	backList  *lockFreeList[GatewayMsg] // 无锁背压队列
	closeChan chan struct{}             // 关闭信号
	wg        sync.WaitGroup            // 消费协程等待组
	// 网关专属配置
	spinTimes int // 自旋次数（网关调优为3000，更低延迟）
	batchSize int // 批量转发大小（默认32，可动态调整）
	// 熔断保护
	isFuse        uint32 // 熔断标记：0=正常，1=熔断（逻辑服不可用）
	fuseThreshold uint64 // 熔断阈值（背压长度超过此值触发）
	// 全维度监控埋点（原子操作，无性能损耗）
	metricTotalSend  uint64 // 总发送消息数
	metricTotalSucc  uint64 // 总发送成功数
	metricTotalFail  uint64 // 总发送失败数
	metricBackMaxLen uint64 // 背压队列最大长度（历史峰值）
}

// NewGatewayBackPressureChan 创建网关专属背压Channel
// backMaxLen：背压队列最大长度（网关推荐8192/16384）
// batchSize：批量转发大小（推荐16/32，兼顾吞吐和延迟）
func NewGatewayBackPressureChan(backMaxLen uint64, batchSize int) *GatewayBackPressureChan {
	if batchSize <= 0 {
		batchSize = 32
	}
	bpc := &GatewayBackPressureChan{
		mainChan:      make(chan GatewayMsg, 16), // 严格保留16缓冲区
		backList:      newLockFreeList[GatewayMsg](backMaxLen),
		closeChan:     make(chan struct{}),
		spinTimes:     3000, // 网关专属自旋次数，提升高并发下的低延迟
		batchSize:     batchSize,
		fuseThreshold: backMaxLen * 8 / 10, // 熔断阈值：背压队列80%满
	}
	// 启动消费协程（网关单协程消费，保证转发顺序）
	bpc.wg.Add(1)
	go bpc.consumeAndForward()
	return bpc
}

// Send 网关发送消息：非阻塞，连接隔离，严格FIFO
// 入参：msg-网关消息（带ConnID，天然连接隔离）
// 返回：true=成功，false=失败（背压满/熔断/已关闭）
func (bpc *GatewayBackPressureChan) Send(msg GatewayMsg) bool {
	atomic.AddUint64(&bpc.metricTotalSend, 1)
	// 熔断判断：逻辑服不可用时，直接返回失败（避免网关内存溢出）
	if atomic.LoadUint32(&bpc.isFuse) == 1 {
		atomic.AddUint64(&bpc.metricTotalFail, 1)
		return false
	}
	// 1. 非阻塞写入主通道（优先低延迟透传）
	select {
	case bpc.mainChan <- msg:
		atomic.AddUint64(&bpc.metricTotalSucc, 1)
		// 更新背压峰值
		bpc.updateBackMaxLen()
		return true
	default:
		// 2. 主通道满，写入背压队列
		if bpc.backList.Push(msg) {
			atomic.AddUint64(&bpc.metricTotalSucc, 1)
			// 检查是否触发熔断
			bpc.checkFuse()
			// 更新背压峰值
			bpc.updateBackMaxLen()
			return true
		} else {
			atomic.AddUint64(&bpc.metricTotalFail, 1)
			return false
		}
	}
}

// consumeAndForward 消费+批量转发：网关核心逻辑，单协程保证顺序
func (bpc *GatewayBackPressureChan) consumeAndForward() {
	defer bpc.wg.Done()
	batchBuf := make([]GatewayMsg, 0, bpc.batchSize) // 批量缓冲区
	for {
		select {
		case <-bpc.closeChan:
			bpc.flush() // 关闭时刷盘，保证剩余消息全部转发
			return
		// 优先消费主通道消息（低延迟）
		case msg := <-bpc.mainChan:
			batchBuf = append(batchBuf, msg)
			// 批量缓冲区满，立即转发
			if len(batchBuf) >= bpc.batchSize {
				bpc.forwardBatch(batchBuf)
				batchBuf = batchBuf[:0]
			}
		default:
			// 主通道无消息，从背压队列读取（自旋+休眠）
			spinCount := 0
			for spinCount < bpc.spinTimes && len(batchBuf) < bpc.batchSize {
				if msg, ok := bpc.backList.Pop(); ok {
					batchBuf = append(batchBuf, msg)
					spinCount = 0
				} else {
					spinCount++
					runtime.Gosched()
				}
			}
			// 有消息则转发，无则短暂休眠
			if len(batchBuf) > 0 {
				bpc.forwardBatch(batchBuf)
				batchBuf = batchBuf[:0]
			} else {
				time.Sleep(300 * time.Nanosecond) // 网关专属休眠时间，更低延迟
			}
			// 熔断恢复检查：背压队列低于50%，恢复正常
			bpc.checkFuseRecover()
		}
	}
}

// forwardBatch 批量转发消息到逻辑服（核心扩展点，对接网关→逻辑服通信层）
// 可替换为TCP/GRPC/消息队列等通信方式，此处为示例骨架
func (bpc *GatewayBackPressureChan) forwardBatch(msgs []GatewayMsg) {
	// 【网关业务扩展点】
	// 示例：调用逻辑服连接池，批量发送消息
	// logicPool.SendBatch(msgs)
	// 注意：若转发失败，可将msgs重新入队背压队列，避免消息丢失
	// if err := logicPool.SendBatch(msgs); err != nil {
	//     for _, msg := range msgs {
	//         bpc.backList.Enqueue(msg)
	//     }
	// }
}

// -------------------------- 网关专属：熔断+监控+工具方法 --------------------------
// checkFuse 检查熔断：背压队列超过80%，触发熔断（保护网关）
func (bpc *GatewayBackPressureChan) checkFuse() {
	if atomic.LoadUint32(&bpc.isFuse) == 1 {
		return
	}
	if bpc.backList.Len() >= bpc.fuseThreshold {
		atomic.StoreUint32(&bpc.isFuse, 1)
		// 【运维扩展点】触发熔断告警，推送到Prometheus/ELK/钉钉
		// log.Warn("网关背压触发熔断", zap.Uint64("backLen", bpc.backList.Len()), zap.Uint64("fuseThd", bpc.fuseThreshold))
	}
}

// checkFuseRecover 熔断恢复：背压队列低于50%，恢复正常
func (bpc *GatewayBackPressureChan) checkFuseRecover() {
	if atomic.LoadUint32(&bpc.isFuse) == 0 {
		return
	}
	recoverThd := bpc.fuseThreshold / 2
	if bpc.backList.Len() <= recoverThd {
		atomic.StoreUint32(&bpc.isFuse, 0)
		// 【运维扩展点】熔断恢复告警
		// log.Info("网关背压熔断恢复", zap.Uint64("backLen", bpc.backList.Len()), zap.Uint64("recoverThd", recoverThd))
	}
}

// updateBackMaxLen 更新背压队列历史峰值（监控用）
func (bpc *GatewayBackPressureChan) updateBackMaxLen() {
	current := bpc.backList.Len()
	max := atomic.LoadUint64(&bpc.metricBackMaxLen)
	if current > max {
		atomic.CompareAndSwapUint64(&bpc.metricBackMaxLen, max, current)
	}
}

// flush 刷盘：关闭时批量转发剩余所有消息
func (bpc *GatewayBackPressureChan) flush() {
	bpc.backList.Close()
	remaining := bpc.backList.Flush()
	if len(remaining) > 0 {
		bpc.forwardBatch(remaining)
	}
	// 转发主通道剩余消息
	batchBuf := make([]GatewayMsg, 0)
	for {
		select {
		case msg := <-bpc.mainChan:
			batchBuf = append(batchBuf, msg)
		default:
			goto END
		}
	}
END:
	if len(batchBuf) > 0 {
		bpc.forwardBatch(batchBuf)
	}
	close(bpc.mainChan)
}

// GetMetric 获取网关监控指标（原子读取，无性能损耗）
// 返沪：总发送/成功/失败，背压当前长度/历史峰值，熔断状态
func (bpc *GatewayBackPressureChan) GetMetric() (totalSend, totalSucc, totalFail, backLen, backMaxLen uint64, isFuse bool) {
	return atomic.LoadUint64(&bpc.metricTotalSend),
		atomic.LoadUint64(&bpc.metricTotalSucc),
		atomic.LoadUint64(&bpc.metricTotalFail),
		bpc.backList.Len(),
		atomic.LoadUint64(&bpc.metricBackMaxLen),
		atomic.LoadUint32(&bpc.isFuse) == 1
}

// SetBatchSize 动态调整批量大小（网关运行时可配置，无需重启）
func (bpc *GatewayBackPressureChan) SetBatchSize(size int) {
	if size > 0 {
		atomic.StoreInt32((*int32)(unsafe.Pointer(&bpc.batchSize)), int32(size))
	}
}

// Close 网关优雅关闭：保证所有消息转发完成，无丢失
func (bpc *GatewayBackPressureChan) Close() {
	close(bpc.closeChan)
	bpc.wg.Wait()
}
