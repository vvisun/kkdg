package kkws

import (
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
)

// TestHeartbeat_OnlyPing_NoAppData
// 仅依靠 Ping/Pong 心跳、不发送任何业务数据，验证读超时不会把连接断掉。
func TestHeartbeat_OnlyPing_NoAppData(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heartbeat test in short mode")
	}

	addr := freePort(t)

	// 服务端：读超时 4s，Ping 间隔 1.5s（内部会被钳到 >=3s），只要心跳正常就不会因读超时关闭。
	srvOpts := kknet.ApplyOptions(
		kknet.WithReadTimeout(4*time.Second),
		kknet.WithPingInterval(2*time.Second),
		kknet.WithRawHandler(&noopRawHandler{}),
	)
	s := NewServer(addr, nil, srvOpts)
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop()

	// 客户端同样配置心跳
	cliOpts := kknet.ApplyOptions(
		kknet.WithReadTimeout(4*time.Second),
		kknet.WithPingInterval(2*time.Second),
		kknet.WithRawHandler(&noopRawHandler{}),
	)
	client := NewClient("ws://"+addr+"/ws", nil, cliOpts)
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	// 等待握手完成
	time.Sleep(100 * time.Millisecond)

	// 空闲时间 > 读超时，仅依赖 Ping/Pong。
	time.Sleep(10 * time.Second)

	// 如果心跳失效，readLoop 会因读超时返回、触发 OnClose，ActiveConns 会变为 0。
	stats := s.Stats()
	if stats.ActiveConns != 1 {
		t.Fatalf("heartbeat did not keep connection alive, ActiveConns=%d, want 1", stats.ActiveConns)
	}
}
