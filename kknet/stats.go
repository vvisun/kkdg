package kknet

import (
	"runtime"
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
	RecvBytes   int64
	SentBytes   int64
	Errors      int64
}

// Stats tracks connection and traffic counters.
type Stats struct {
	activeConns int64 // 当前活跃连接数
	totalConns  int64 // 累计连接数
	closedConns int64 // 累计关闭连接数
	recvMsgs    int64 // 累计接收消息数
	sentMsgs    int64 // 累计发送消息数
	recvBytes   int64 // 累计接收字节数
	sentBytes   int64 // 累计发送字节数
	errors      int64 // 累计错误数
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
	atomic.AddInt64(&s.recvBytes, int64(n))
}

// AddSent records one sent message.
func (s *Stats) AddSent(n int) {
	if n <= 0 {
		return
	}
	atomic.AddInt64(&s.sentMsgs, 1)
	atomic.AddInt64(&s.sentBytes, int64(n))
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
		RecvBytes:   atomic.LoadInt64(&s.recvBytes),
		SentBytes:   atomic.LoadInt64(&s.sentBytes),
		Errors:      atomic.LoadInt64(&s.errors),
	}
}

func PrintStress(stats *StatsSnapshot) {
	var memStats runtime.MemStats

	// 统计内存（堆分配）
	runtime.ReadMemStats(&memStats)
	heapUsed := memStats.Alloc / 1024 / 1024 // MB
	perConnMem := 0.0
	if stats.ActiveConns > 0 {
		perConnMem = float64(memStats.Mallocs) / float64(stats.ActiveConns) / 1024 // KB/连接
	}

	// 打印指标
	kklog.Debugf("=== kknet 指标 ===")
	kklog.Debugf("并发连接数：%d", stats.ActiveConns)
	kklog.Debugf("累计接收消息量：%d", stats.RecvMsgs)
	kklog.Debugf("累计发送消息量：%d", stats.SentMsgs)
	kklog.Debugf("累计接收字节数：%d", stats.RecvBytes)
	kklog.Debugf("累计发送字节数：%d", stats.SentBytes)
	kklog.Debugf("累计错误数：%d", stats.Errors)
	kklog.Debugf("堆内存占用：%d MB", heapUsed)
	kklog.Debugf("单连接内存：%.2f KB/conn", perConnMem)
	kklog.Debugf("------------------------\n")
}
