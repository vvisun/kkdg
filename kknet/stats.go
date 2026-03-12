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

//----------------------------------------------------------

// MetricsFromSnapshot 将 StatsSnapshot 转换为可用于 metrics 导出的键值对。
//
// - namespace 用于区分不同实例/模块，为空则不加前缀。
// - 返回的 key 采用 "<namespace>.<name>" 或仅 "<name>" 形式，值统一为 float64，方便接入 Prometheus、StatsD 等。
//
// 约定的度量名称（未加 namespace）：
//   - kknet_active_conns
//   - kknet_total_conns
//   - kknet_closed_conns
//   - kknet_recv_msgs_total
//   - kknet_sent_msgs_total
//   - kknet_errors_total
func MetricsFromSnapshot(namespace string, snap StatsSnapshot) map[string]float64 {
	prefix := ""
	if namespace != "" {
		prefix = namespace + "."
	}

	m := make(map[string]float64, 6)
	m[prefix+"kknet_active_conns"] = float64(snap.ActiveConns)
	m[prefix+"kknet_total_conns"] = float64(snap.TotalConns)
	m[prefix+"kknet_closed_conns"] = float64(snap.ClosedConns)
	m[prefix+"kknet_recv_msgs_total"] = float64(snap.RecvMsgs)
	m[prefix+"kknet_sent_msgs_total"] = float64(snap.SentMsgs)
	m[prefix+"kknet_errors_total"] = float64(snap.Errors)

	return m
}
