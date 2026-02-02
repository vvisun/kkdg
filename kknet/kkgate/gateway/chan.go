package gateway

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/vvisun/kkdg/kknet/kkgate/gateway/pbgate"
	"google.golang.org/protobuf/proto"
)

// -------------------------- 工具方法：ConnID哈希取模获取分片ID（保证单ConnID顺序性） --------------------------
func getShardIDByConnID(connID uint64) int {
	return int(connID % uint64(ShardCount))
}

// -------------------------- 分片背压Channel核心结构 --------------------------
type GatewayBackPressureChan struct {
	mainChan      [ShardCount]chan GatewayMsg           // 分片主通道
	backList      [ShardCount]*lockFreeList[GatewayMsg] // 分片背压无锁队列
	closeChan     chan struct{}                         // 全局关闭通道
	wg            sync.WaitGroup                        // 消费协程等待组
	spinTimes     int                                   // 自旋次数
	batchSize     int                                   // 批量转发大小
	backMaxLen    uint64                                // 单个分片背压最大长度
	logicFuseMap  sync.Map                              // 按逻辑服熔断：key=LogicID, val=bool
	fuseThreshold float64                               // 熔断阈值（0.8）
	// 全局监控指标
	metricTotalSend        uint64
	metricTotalSucc        uint64
	metricTotalFail        uint64
	metricBackMaxLen       uint64
	metricHighLevelDiscard uint64
	metricLowLevelDiscard  uint64
	// 依赖注入
	logicConnPool *LogicConnPool
}

// NewGatewayBackPressureChan 创建分片背压Channel
func NewGatewayBackPressureChan(perShardBackMaxLen uint64, batchSize int, pool *LogicConnPool) *GatewayBackPressureChan {
	if batchSize <= 0 {
		batchSize = 32
	}
	bpc := &GatewayBackPressureChan{
		closeChan:     make(chan struct{}),
		spinTimes:     3000,
		batchSize:     batchSize,
		backMaxLen:    perShardBackMaxLen,
		fuseThreshold: 0.8,
		logicConnPool: pool,
	}
	// 初始化分片主通道和背压队列
	for i := 0; i < ShardCount; i++ {
		bpc.mainChan[i] = make(chan GatewayMsg, 16)
		bpc.backList[i] = newLockFreeList[GatewayMsg](perShardBackMaxLen)
	}
	// 启动分片消费协程（与分片数一致，多核并行）
	for i := 0; i < ShardCount; i++ {
		bpc.wg.Add(1)
		go bpc.consumeAndForwardByShard(i)
	}
	return bpc
}

// Send 非阻塞发送消息，按ConnID路由到指定分片
func (bpc *GatewayBackPressureChan) Send(msg GatewayMsg) bool {
	atomic.AddUint64(&bpc.metricTotalSend, 1)
	shardID := getShardIDByConnID(msg.ConnID)

	// 检查逻辑服是否熔断
	if isFuse, ok := bpc.logicFuseMap.Load(msg.LogicID); ok && isFuse.(bool) {
		if msg.MsgLevel == MsgLevelLow {
			atomic.AddUint64(&bpc.metricLowLevelDiscard, 1)
			atomic.AddUint64(&bpc.metricTotalFail, 1)
			pbgate.PutPbObjByMsgID(msg.MsgID, msg.PbMsg)
			return false
		}
	}

	// 优先写入分片主通道
	select {
	case bpc.mainChan[shardID] <- msg:
		atomic.AddUint64(&bpc.metricTotalSucc, 1)
		bpc.updateGlobalBackMaxLen()
		return true
	default:
		// 主通道满，写入分片背压队列
		if bpc.backList[shardID].Enqueue(msg) {
			atomic.AddUint64(&bpc.metricTotalSucc, 1)
			bpc.checkLogicFuse(msg.LogicID)
			bpc.updateGlobalBackMaxLen()
			return true
		} else {
			atomic.AddUint64(&bpc.metricTotalFail, 1)
			if msg.MsgLevel == MsgLevelLow {
				atomic.AddUint64(&bpc.metricLowLevelDiscard, 1)
			} else {
				atomic.AddUint64(&bpc.metricHighLevelDiscard, 1)
			}
			pbgate.PutPbObjByMsgID(msg.MsgID, msg.PbMsg)
			return false
		}
	}
}

// consumeAndForwardByShard 分片独立消费，多核并行无竞争
func (bpc *GatewayBackPressureChan) consumeAndForwardByShard(shardID int) {
	defer bpc.wg.Done()
	batchBuf := make([]GatewayMsg, 0, bpc.batchSize)
	for {
		select {
		case <-bpc.closeChan:
			bpc.flushShard(shardID)
			return
		case msg := <-bpc.mainChan[shardID]:
			batchBuf = append(batchBuf, msg)
			if len(batchBuf) >= bpc.batchSize {
				bpc.forwardBatch(batchBuf, shardID)
				batchBuf = batchBuf[:0]
			}
		default:
			// 自旋读取背压队列
			spinCount := 0
			for spinCount < bpc.spinTimes && len(batchBuf) < bpc.batchSize {
				if msg, ok := bpc.backList[shardID].Dequeue(); ok {
					batchBuf = append(batchBuf, msg)
					spinCount = 0
				} else {
					spinCount++
					runtime.Gosched()
				}
			}
			if len(batchBuf) > 0 {
				bpc.forwardBatch(batchBuf, shardID)
				batchBuf = batchBuf[:0]
			} else {
				time.Sleep(300 * time.Nanosecond)
			}
			bpc.checkLogicFuseRecover()
		}
	}
}

// forwardBatch 批量转发消息，适配分片缓冲区池
func (bpc *GatewayBackPressureChan) forwardBatch(msgs []GatewayMsg, shardID int) {
	// 按逻辑服分组，批量处理
	logicGroup := make(map[uint32][]GatewayMsg)
	for _, msg := range msgs {
		logicGroup[msg.LogicID] = append(logicGroup[msg.LogicID], msg)
	}

	for logicID, groupMsgs := range logicGroup {
		var protoMsgs []proto.Message
		var traceIDs [][]byte
		var msgIDs []uint32
		var validMsgs []GatewayMsg
		now := time.Now().UnixMilli()

		// 过滤超时/重试耗尽的消息
		for _, msg := range groupMsgs {
			if now > msg.TimeoutTS || msg.RetryCount >= 3 {
				atomic.AddUint64(&bpc.metricTotalFail, 1)
				if msg.MsgLevel == MsgLevelHigh {
					atomic.AddUint64(&bpc.metricHighLevelDiscard, 1)
				} else {
					atomic.AddUint64(&bpc.metricLowLevelDiscard, 1)
				}
				pbgate.PutPbObjByMsgID(msg.MsgID, msg.PbMsg)
				continue
			}
			validMsgs = append(validMsgs, msg)
			protoMsgs = append(protoMsgs, msg.PbMsg)
			traceIDs = append(traceIDs, msg.TraceID)
			msgIDs = append(msgIDs, msg.MsgID)
		}
		if len(validMsgs) == 0 {
			continue
		}

		// 获取逻辑服连接
		conn, err := bpc.logicConnPool.Get(logicID)
		if err != nil {
			bpc.retryOrDiscardMsgs(validMsgs)
			continue
		}
		defer bpc.logicConnPool.Put(logicID, conn)

		// 封装批量消息
		var batchProtoMsgs []*pbgate.Message
		for i := range validMsgs {
			batchProtoMsgs = append(batchProtoMsgs, &pbgate.Message{
				MsgID:   msgIDs[i],
				TraceID: traceIDs[i],
				Data:    protoMsgs[i],
			})
		}

		// 分片批量编码
		batchData, err := pbgate.EncodeBatchWithShard(batchProtoMsgs, shardID)
		if err != nil {
			bpc.retryOrDiscardMsgs(validMsgs)
			pbgate.PutShardBuf(shardID, batchData)
			continue
		}
		defer pbgate.PutShardBuf(shardID, batchData)

		// 发送到逻辑服
		if _, err := conn.Write(batchData); err != nil {
			bpc.logicConnPool.MarkFault(logicID)
			bpc.retryOrDiscardMsgs(validMsgs)
			return
		}

		// 发送成功，归还Protobuf对象
		for _, msg := range validMsgs {
			pbgate.PutPbObjByMsgID(msg.MsgID, msg.PbMsg)
		}
	}
}

// retryOrDiscardMsgs 消息重试/丢弃：高优重试，低优丢弃
func (bpc *GatewayBackPressureChan) retryOrDiscardMsgs(msgs []GatewayMsg) {
	for _, msg := range msgs {
		if msg.MsgLevel == MsgLevelHigh {
			msg.RetryCount++
			shardID := getShardIDByConnID(msg.ConnID)
			bpc.backList[shardID].Enqueue(msg)
		} else {
			atomic.AddUint64(&bpc.metricLowLevelDiscard, 1)
			atomic.AddUint64(&bpc.metricTotalFail, 1)
			pbgate.PutPbObjByMsgID(msg.MsgID, msg.PbMsg)
		}
	}
}

// -------------------------- 熔断/恢复/监控相关方法 --------------------------
func (bpc *GatewayBackPressureChan) checkLogicFuse(logicID uint32) {
	if isFuse, ok := bpc.logicFuseMap.Load(logicID); ok && isFuse.(bool) {
		return
	}
	var totalLen uint64
	for i := 0; i < ShardCount; i++ {
		totalLen += bpc.backList[i].Len()
	}
	totalMaxLen := uint64(ShardCount) * bpc.backMaxLen
	if float64(totalLen)/float64(totalMaxLen) >= bpc.fuseThreshold {
		bpc.logicFuseMap.Store(logicID, true)
	}
}

func (bpc *GatewayBackPressureChan) checkLogicFuseRecover() {
	recoverThreshold := bpc.fuseThreshold / 2
	var totalLen uint64
	for i := 0; i < ShardCount; i++ {
		totalLen += bpc.backList[i].Len()
	}
	totalMaxLen := uint64(ShardCount) * bpc.backMaxLen
	if float64(totalLen)/float64(totalMaxLen) < recoverThreshold {
		bpc.logicFuseMap.Range(func(key, value any) bool {
			bpc.logicFuseMap.Store(key.(uint32), false)
			return true
		})
	}
}

func (bpc *GatewayBackPressureChan) updateGlobalBackMaxLen() {
	var totalLen uint64
	for i := 0; i < ShardCount; i++ {
		totalLen += bpc.backList[i].Len()
	}
	currentMax := atomic.LoadUint64(&bpc.metricBackMaxLen)
	if totalLen > currentMax {
		atomic.CompareAndSwapUint64(&bpc.metricBackMaxLen, currentMax, totalLen)
	}
}

func (bpc *GatewayBackPressureChan) flushShard(shardID int) {
	// 刷背压队列
	remaining := bpc.backList[shardID].Flush()
	if len(remaining) > 0 {
		bpc.forwardBatch(remaining, shardID)
	}
	// 刷主通道
	var batchBuf []GatewayMsg
	for {
		select {
		case msg := <-bpc.mainChan[shardID]:
			batchBuf = append(batchBuf, msg)
		default:
			goto END
		}
	}
END:
	if len(batchBuf) > 0 {
		bpc.forwardBatch(batchBuf, shardID)
	}
}

func (bpc *GatewayBackPressureChan) flush() {
	for i := 0; i < ShardCount; i++ {
		bpc.flushShard(i)
	}
}

// GetMetric 获取全局监控指标
func (bpc *GatewayBackPressureChan) GetMetric() (
	totalSend, totalSucc, totalFail, backLen, backMaxLen, highDiscard, lowDiscard uint64,
	logicFuseCount int,
) {
	logicFuseCount = 0
	bpc.logicFuseMap.Range(func(key, value any) bool {
		if value.(bool) {
			logicFuseCount++
		}
		return true
	})
	var currentBackLen uint64
	for i := 0; i < ShardCount; i++ {
		currentBackLen += bpc.backList[i].Len()
	}
	return atomic.LoadUint64(&bpc.metricTotalSend),
		atomic.LoadUint64(&bpc.metricTotalSucc),
		atomic.LoadUint64(&bpc.metricTotalFail),
		currentBackLen,
		atomic.LoadUint64(&bpc.metricBackMaxLen),
		atomic.LoadUint64(&bpc.metricHighLevelDiscard),
		atomic.LoadUint64(&bpc.metricLowLevelDiscard),
		logicFuseCount
}

// SetBatchSize 动态调整批量大小
func (bpc *GatewayBackPressureChan) SetBatchSize(size int) {
	if size > 0 {
		atomic.StoreInt32((*int32)(unsafe.Pointer(&bpc.batchSize)), int32(size))
	}
}

// Close 优雅关闭
func (bpc *GatewayBackPressureChan) Close() {
	close(bpc.closeChan)
	bpc.wg.Wait()
	bpc.logicConnPool.Close()
	bpc.logicFuseMap = sync.Map{}
}
