package kknet

import (
	"runtime/metrics"
	"sync/atomic"

	"github.com/vvisun/kkdg/utils/kklog"
)

// StatsSnapshot is a point-in-time copy of statistics.
type StatsSnapshot struct {
	ActiveConns int64
	TotalConns  int64
	ClosedConns int64
	RecvMsgs    int64
	SentMsgs    int64
	//RecvBytes   uint64
	//SentBytes   uint64
	Errors int64
}

// Stats tracks connection and traffic counters.
type Stats struct {
	activeConns int64 // 当前活跃连接数
	totalConns  int64 // 累计连接数
	closedConns int64 // 累计关闭连接数
	recvMsgs    int64 // 累计接收消息数
	sentMsgs    int64 // 累计发送消息数
	//recvBytes   uint64 // 累计接收字节数
	//sentBytes   uint64 // 累计发送字节数
	errors int64 // 累计错误数
}

// OnConnect updates connection counters.
func (s *Stats) OnConnect() {
	atomic.AddInt64(&s.activeConns, 1)
	atomic.AddInt64(&s.totalConns, 1)
}

// OnClose updates connection counters.
func (s *Stats) OnClose() {
	atomic.AddInt64(&s.activeConns, -1)
	atomic.AddInt64(&s.closedConns, 1)
}

// AddRecv records one received message.
func (s *Stats) AddRecv(n int) {
	if n <= 0 {
		return
	}
	atomic.AddInt64(&s.recvMsgs, 1)
	//atomic.AddUint64(&s.recvBytes, uint64(n))
}

// AddSent records one sent message.
func (s *Stats) AddSent(n int) {
	if n <= 0 {
		return
	}
	atomic.AddInt64(&s.sentMsgs, 1)
	//atomic.AddUint64(&s.sentBytes, uint64(n))
}

// AddError records an error.
func (s *Stats) AddError() {
	atomic.AddInt64(&s.errors, 1)
}

// Snapshot returns a copy of current stats.
func (s *Stats) Snapshot() StatsSnapshot {
	return StatsSnapshot{
		ActiveConns: atomic.LoadInt64(&s.activeConns),
		TotalConns:  atomic.LoadInt64(&s.totalConns),
		ClosedConns: atomic.LoadInt64(&s.closedConns),
		RecvMsgs:    atomic.LoadInt64(&s.recvMsgs),
		SentMsgs:    atomic.LoadInt64(&s.sentMsgs),
		//RecvBytes:   atomic.LoadUint64(&s.recvBytes),
		//SentBytes:   atomic.LoadUint64(&s.sentBytes),
		Errors: atomic.LoadInt64(&s.errors),
	}
}

//----------------------------------------------------------

// 使用 runtime/metrics 读取的指标名称（Go 1.16+，无 stop-the-world）
const metricHeapObjectsBytes = "/memory/classes/heap/objects:bytes"

func PrintStress(stats *StatsSnapshot) {
	heapUsedMB, heapKBPerConn := ReadMetricsStress(stats.ActiveConns)

	kklog.Debugf("=== kknet 指标 ===")
	kklog.Debugf("并发连接数：%d", stats.ActiveConns)
	kklog.Debugf("累计接收消息量：%d", stats.RecvMsgs)
	kklog.Debugf("累计发送消息量：%d", stats.SentMsgs)
	//kklog.Debugf("累计接收字节数：%dMB", stats.RecvBytes/1024/1024)
	//kklog.Debugf("累计发送字节数：%dMB", stats.SentBytes/1024/1024)
	kklog.Debugf("累计错误数：%d", stats.Errors)
	kklog.Debugf("堆内存占用：%d MB", heapUsedMB)
	kklog.Debugf("单连接堆内存：%.2f KB/conn", heapKBPerConn)
	kklog.Debugf("------------------------")
}

// ReadMetricsStress 通过 runtime/metrics 读取堆内存（无 stop-the-world）。
// 返回 (堆对象占用 MB, 单连接堆内存 KB)。
func ReadMetricsStress(activeConns int64) (heapUsedMB uint64, heapKBPerConn float64) {
	samples := []metrics.Sample{
		{Name: metricHeapObjectsBytes},
	}
	metrics.Read(samples)

	var heapBytes uint64
	if samples[0].Value.Kind() == metrics.KindUint64 {
		heapBytes = samples[0].Value.Uint64()
	}

	heapUsedMB = heapBytes / 1024 / 1024
	if activeConns > 0 && heapBytes > 0 {
		heapKBPerConn = float64(heapBytes) / float64(activeConns) / 1024
	}
	return heapUsedMB, heapKBPerConn
}
