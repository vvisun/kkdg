package kkchan

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

// -------------------------- 游戏服专属测试示例 --------------------------
// 模拟游戏服高并发场景：10个协程发送帧同步消息（每秒100w+），验证顺序和性能
func Test_LockFreeLinkBackPressureChan(t *testing.T) {
	type msgItem struct {
		prodID int
		seq    int
	}

	// 初始化：背压队列最大长度4096（游戏服推荐），自旋次数2000
	bpc := NewLockFreeLinkBackPressureChan[msgItem](4096*2, 2000)
	// NOTE: we will Close() explicitly after receiving all messages.

	// 模拟：5个生产者协程（网关），每个发送20w条帧同步消息
	producerNum := 5
	msgNumPerProd := 2000000
	var wgProd sync.WaitGroup
	wgProd.Add(producerNum)

	// 计时：统计发送耗时
	start := time.Now()
	for p := 0; p < producerNum; p++ {
		go func(prodID int) {
			defer wgProd.Done()
			for i := 0; i < msgNumPerProd; i++ {
				// 关键消息：循环重试直到发送成功（无丢失）
				for bpc.Send(msgItem{prodID: prodID, seq: i}) == 1 {
					runtime.Gosched()
				}
			}
		}(p)
	}

	// 模拟：Actor单协程消费（游戏服逻辑层），验证顺序性
	recvCnt := 0
	orderErr := false
	lastSeq := make([]int, producerNum)
	for i := range lastSeq {
		lastSeq[i] = -1
	}

	totalMsg := producerNum * msgNumPerProd
	timeout := time.NewTimer(120 * time.Second)
	defer timeout.Stop()

	for recvCnt < totalMsg {
		select {
		case val, ok := <-bpc.Chan():
			if !ok {
				t.Fatalf("channel closed early: recv=%d total=%d", recvCnt, totalMsg)
			}
			recvCnt++

			// 多生产者全局顺序不可定义（并发入队的先后不确定），因此只验证：
			// 1) 每个 producer 自身的发送顺序不乱序（FIFO 对单线程生产者应成立）
			prodID := val.prodID
			seq := val.seq
			if prodID < 0 || prodID >= producerNum {
				t.Fatalf("invalid prodID=%d val=%+v", prodID, val)
			}
			if seq != lastSeq[prodID]+1 && !orderErr {
				orderErr = true
				println("[错误] producer内顺序错乱！prodID：", prodID, "当前seq：", seq, "上一个seq：", lastSeq[prodID])
			}
			lastSeq[prodID] = seq

			// 每10w条打印一次进度
			if recvCnt%100000 == 0 {
				println("[进度] 已接收", recvCnt, "条消息，背压队列长度：", bpc.BackLen())
			}
		case <-timeout.C:
			t.Fatalf("timeout: recv=%d total=%d backLen=%d", recvCnt, totalMsg, bpc.BackLen())
		}
	}
	// 主动关闭，释放后台协程并关闭 mainChan
	bpc.Close()

	// 统计结果
	elapsed := time.Since(start)
	println("==================== 测试结果 ====================")
	println("总发送消息数：", totalMsg)
	println("总接收消息数：", recvCnt)
	println("消息是否丢失：", recvCnt == totalMsg)
	println("消息是否乱序：", orderErr)
	println("总耗时：", elapsed.Seconds(), "秒")
	println("平均吞吐：", float64(totalMsg)/elapsed.Seconds()*1000000, "百万条/秒")
}
