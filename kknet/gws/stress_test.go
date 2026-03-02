package gws

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet/gws/internal"
)

// echoHandler 服务端回显 Handler，用于压测
type echoHandler struct {
	BuiltinEventHandler
}

func (h *echoHandler) OnMessage(socket *Conn, message *Message) {
	defer message.Close()
	_ = socket.WriteMessage(message.Opcode, message.Bytes())
}

// readyListener 在首次 Accept 时发出就绪信号，确保只在本进程已进入 Accept 后才返回
type readyListener struct {
	net.Listener
	ready chan struct{}
	once  sync.Once
}

func (r *readyListener) Accept() (net.Conn, error) {
	r.once.Do(func() { close(r.ready) })
	return r.Listener.Accept()
}

// runEchoServer 在随机端口启动 Echo 服务器，返回 ws 地址与停止函数
func runEchoServer(t testing.TB, opt *ServerOption) (addr string, stop func()) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr = "ws://" + listener.Addr().String()
	ready := make(chan struct{})
	wrapped := &readyListener{Listener: listener, ready: ready}
	srv := NewServer(&echoHandler{}, opt)
	srv.OnError = func(_ net.Conn, _ error) {} // 压测结束时 listener.Close 会触发 accept 错误，忽略日志
	go func() {
		_ = srv.RunListener(wrapped)
	}()
	select {
	case <-ready:
		// 给调度器时间让服务端执行到 r.Listener.Accept()，再让客户端连
		time.Sleep(50 * time.Millisecond)
		return addr, func() { _ = listener.Close() }
	case <-time.After(10 * time.Second):
		_ = listener.Close()
		t.Fatalf("server at %s not ready after 10s", listener.Addr().String())
		return "", nil
	}
}

// stressClient 单个压测客户端：连接后发送 numSend 条消息并等待回显
func stressClient(t testing.TB, wsAddr string, numSend int, payloadSize int) (sent, recv int64, err error) {
	var recvCount int64
	handler := &webSocketMocker{}
	handler.onMessage = func(socket *Conn, message *Message) {
		atomic.AddInt64(&recvCount, 1)
		_ = message.Close()
	}

	var conn *Conn
	for attempt := 0; attempt < 60; attempt++ {
		conn, _, err = NewClient(handler, &ClientOption{Addr: wsAddr})
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		return 0, 0, err
	}
	defer conn.NetConn().Close()

	go conn.ReadLoop()
	time.Sleep(10 * time.Millisecond)

	payload := internal.AlphabetNumeric.Generate(payloadSize)
	for i := 0; i < numSend; i++ {
		if err = conn.WriteMessage(OpcodeText, payload); err != nil {
			return int64(i), atomic.LoadInt64(&recvCount), err
		}
	}
	sent = int64(numSend)

	deadline := time.Now().Add(30 * time.Second)
	for atomic.LoadInt64(&recvCount) < sent && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	return sent, atomic.LoadInt64(&recvCount), nil
}

// TestStress_Echo 压测：多连接 + 每连接多消息回显
func TestStress_Echo(t *testing.T) {
	const (
		numClients  = 22222
		msgsPerConn = 2222
		payloadSize = 555
	)
	addr, stop := runEchoServer(t, nil)
	defer stop()

	var wg sync.WaitGroup
	var totalSent, totalRecv int64
	start := time.Now()
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sent, recv, err := stressClient(t, addr, msgsPerConn, payloadSize)
			if err != nil {
				t.Errorf("stress client: %v", err)
				return
			}
			atomic.AddInt64(&totalSent, sent)
			atomic.AddInt64(&totalRecv, recv)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("clients=%d, msgsPerConn=%d, payload=%d bytes\n", numClients, msgsPerConn, payloadSize)
	fmt.Printf("total sent=%d, recv=%d, elapsed=%v\n", atomic.LoadInt64(&totalSent), atomic.LoadInt64(&totalRecv), elapsed)
	fmt.Printf("connections/sec ≈ %.0f\n", float64(numClients)/elapsed.Seconds())
	fmt.Printf("messages/sec ≈ %.0f\n", float64(atomic.LoadInt64(&totalRecv))/elapsed.Seconds())

	if got := atomic.LoadInt64(&totalRecv); got != atomic.LoadInt64(&totalSent) {
		fmt.Printf("recv %d != sent %d\n", got, atomic.LoadInt64(&totalSent))
	}
}

// TestStress_ManyConnections 压测：大量短连接，每连接发少量消息后断开
func TestStress_ManyConnections(t *testing.T) {
	const (
		numClients  = 50000
		msgsPerConn = 10
		payloadSize = 564
	)
	addr, stop := runEchoServer(t, nil)
	defer stop()

	var wg sync.WaitGroup
	var done int64
	start := time.Now()
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = stressClient(t, addr, msgsPerConn, payloadSize)
			atomic.AddInt64(&done, 1)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("connections=%d, msgsPerConn=%d, completed=%d\n", numClients, msgsPerConn, atomic.LoadInt64(&done))
	fmt.Printf("elapsed=%v, connections/sec ≈ %.0f\n", elapsed, float64(numClients)/elapsed.Seconds())
}

// TestStress_Echo_Compress 开启压缩的回显压测
func TestStress_Echo_Compress(t *testing.T) {
	const numClients, msgsPerConn, payloadSize = 100, 30, 256
	opt := &ServerOption{
		PermessageDeflate: PermessageDeflate{Enabled: true, Threshold: 1},
	}
	addr, stop := runEchoServer(t, opt)
	defer stop()

	var wg sync.WaitGroup
	var totalSent, totalRecv int64
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sent, recv, err := stressClient(t, addr, msgsPerConn, payloadSize)
			if err != nil {
				t.Errorf("stress client: %v", err)
				return
			}
			atomic.AddInt64(&totalSent, sent)
			atomic.AddInt64(&totalRecv, recv)
		}()
	}
	wg.Wait()
	if got := atomic.LoadInt64(&totalRecv); got != atomic.LoadInt64(&totalSent) {
		t.Errorf("recv %d != sent %d", got, atomic.LoadInt64(&totalSent))
	}
	t.Logf("compress: sent=%d recv=%d", atomic.LoadInt64(&totalSent), atomic.LoadInt64(&totalRecv))
}

// BenchmarkStress_Echo 压测基准：每轮一个客户端连接并完成多轮回显
func BenchmarkStress_Echo(b *testing.B) {
	addr, stop := runEchoServer(b, nil)
	defer stop()

	const msgsPerClient = 20
	const payloadSize = 128

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sent, recv, err := stressClient(b, addr, msgsPerClient, payloadSize)
		if err != nil {
			b.Fatal(err)
		}
		if sent != recv {
			b.Errorf("sent %d != recv %d", sent, recv)
		}
	}
}

// BenchmarkStress_Echo_Parallel 多 goroutine 并发压测
func BenchmarkStress_Echo_Parallel(b *testing.B) {
	addr, stop := runEchoServer(b, nil)
	defer stop()

	const msgsPerClient = 20
	const payloadSize = 64

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, _ = stressClient(b, addr, msgsPerClient, payloadSize)
		}
	})
}

// BenchmarkStress_ConnectOnly 仅测连接建立（握手上限）
func BenchmarkStress_ConnectOnly(b *testing.B) {
	addr, stop := runEchoServer(b, nil)
	defer stop()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		conn, _, err := NewClient(&BuiltinEventHandler{}, &ClientOption{Addr: addr})
		if err != nil {
			b.Fatal(err)
		}
		_ = conn.NetConn().Close()
	}
}

// BenchmarkStress_ConnectOnly_Parallel 并发仅连接
func BenchmarkStress_ConnectOnly_Parallel(b *testing.B) {
	addr, stop := runEchoServer(b, nil)
	defer stop()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, _, err := NewClient(&BuiltinEventHandler{}, &ClientOption{Addr: addr})
			if err != nil {
				b.Fatal(err)
			}
			_ = conn.NetConn().Close()
		}
	})
}
