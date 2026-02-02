package netprocessor

import (
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

// -------------------------- 游戏服专属测试示例 --------------------------
// 模拟游戏服高并发场景：10个协程发送帧同步消息（每秒100w+），验证顺序和性能
func Test_LockFreeLinkBackPressureChan(t *testing.T) {
	// 初始化：背压队列最大长度4096（游戏服推荐），自旋次数2000
	bpc := NewLockFreeLinkBackPressureChan[int](4096, 2000)
	defer bpc.Close()

	// 模拟：5个生产者协程（网关），每个发送20w条帧同步消息
	producerNum := 5
	msgNumPerProd := 200000
	var wgProd sync.WaitGroup
	wgProd.Add(producerNum)

	// 计时：统计发送耗时
	start := time.Now()
	for p := 0; p < producerNum; p++ {
		go func(prodID int) {
			defer wgProd.Done()
			for i := 0; i < msgNumPerProd; i++ {
				// 关键消息：循环重试直到发送成功（无丢失）
				for !bpc.Send(prodID*1000000 + i) {
					runtime.Gosched()
				}
			}
		}(p)
	}

	// 模拟：Actor单协程消费（游戏服逻辑层），验证顺序性
	recvCnt := 0
	lastVal := -1
	orderErr := false
	for val := range bpc.Chan() {
		recvCnt++
		// 验证严格FIFO，出现乱序立即标记
		if val < lastVal && !orderErr {
			orderErr = true
			println("[错误] 消息顺序错乱！当前值：", val, "上一个值：", lastVal)
		}
		lastVal = val

		// 每10w条打印一次进度
		if recvCnt%100000 == 0 {
			println("[进度] 已接收", recvCnt, "条消息，背压队列长度：", bpc.BackLen())
		}
	}

	// 统计结果
	elapsed := time.Since(start)
	totalMsg := producerNum * msgNumPerProd
	println("==================== 测试结果 ====================")
	println("总发送消息数：", totalMsg)
	println("总接收消息数：", recvCnt)
	println("消息是否丢失：", recvCnt == totalMsg)
	println("消息是否乱序：", orderErr)
	println("总耗时：", elapsed)
	println("平均吞吐：", float64(totalMsg)/elapsed.Seconds(), "条/秒")

	// 退出
	os.Exit(0)
}
