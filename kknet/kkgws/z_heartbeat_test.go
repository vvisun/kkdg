package kkgws

import (
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// noopRawHandlerForHeartbeat satisfies RawHandler for heartbeat tests.
type noopRawHandlerForHeartbeat struct{}

func (h *noopRawHandlerForHeartbeat) OnRaw(_ kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	if data != nil {
		kkbuffer.Put(data)
	}
}

// rawRecvHandlerForPingTest receives raw packets into a channel.
type rawRecvHandlerForPingTest struct {
	ch chan []byte
}

func (h *rawRecvHandlerForPingTest) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	if data == nil {
		return
	}
	b := append([]byte(nil), data.Bytes()...)
	select {
	case h.ch <- b:
	default:
	}
	kkbuffer.Put(data)
}

// TestHeartbeat_OnlyPing_NoAppData
// 仅依靠 Ping/Pong 心跳、不发送任何业务数据，验证读超时不会把连接断掉。
func TestHeartbeat_OnlyPing_NoAppData(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heartbeat test in short mode")
	}

	addr := freePortStress(t)

	// 服务端：读超时 4s，Ping 间隔 2s，只要心跳正常就不会因读超时关闭。
	srvOpts := kknet.ApplyOptions(
		kknet.WithReadTimeout(4*time.Second),
		kknet.WithPingInterval(2*time.Second),
		kknet.WithRawHandler(&noopRawHandlerForHeartbeat{}),
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
		kknet.WithRawHandler(&noopRawHandlerForHeartbeat{}),
	)
	client := NewClient("ws://"+addr+"/ws", nil, cliOpts)
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	// 等待握手完成
	time.Sleep(100 * time.Millisecond)

	// 空闲时间 > 读超时，仅依赖 Ping/Pong。
	time.Sleep(12 * time.Second)

	// 如果心跳失效，readLoop 会因读超时返回、触发 OnClose，ActiveConns 会变为 0。
	stats := s.Stats()
	if stats.ActiveConns != 1 {
		t.Fatalf("heartbeat did not keep connection alive, ActiveConns=%d, want 1", stats.ActiveConns)
	}
}

// TestPingPong_Keepalive 演示 Ping/Pong 保活：读超时 4s，Ping 间隔 1.5s；空闲 5s 后仍能收发，说明 Pong 刷新了读超时。
func TestPingPong_Keepalive(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping ping/pong test in short mode")
	}

	addr := freePortStress(t)
	recvCh := make(chan []byte, 4)
	opts := kknet.ApplyOptions(
		kknet.WithReadTimeout(4*time.Second),
		kknet.WithPingInterval(1500*time.Millisecond),
		kknet.WithRawHandler(&rawRecvHandlerForPingTest{ch: recvCh}),
	)
	s := NewServer(addr, nil, opts)
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop()

	clientOpts := kknet.ApplyOptions(
		kknet.WithReadTimeout(4*time.Second),
		kknet.WithPingInterval(1500*time.Millisecond),
		kknet.WithRawHandler(&noopRawHandlerForHeartbeat{}),
	)
	client := NewClient("ws://"+addr+"/ws", nil, clientOpts)
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()
	time.Sleep(100 * time.Millisecond)

	// 空闲超过读超时（4s），仅靠 Ping/Pong 保活
	time.Sleep(5 * time.Second)

	// 仍能正常收发说明连接未因读超时断开
	payload := []byte("alive")
	bb, err := kkpacket.DefaultStreamPacket().Pack(payload)
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	if err := client.SendBuffer(bb); err != nil {
		t.Fatalf("SendBuffer after idle: %v", err)
	}
	select {
	case got := <-recvCh:
		msg, err := kkpacket.DefaultStreamPacket().Unpack(got)
		if err != nil {
			t.Fatalf("Unpack: %v", err)
		}
		if string(msg) != string(payload) {
			t.Errorf("got %q, want %q", msg, payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message after idle (Ping/Pong keepalive may not be active)")
	}
}

