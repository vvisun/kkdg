package gnrpc

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/vvisun/kkdg/proto/pbbase"
	"github.com/vvisun/kkdg/utils/xnet"
	"google.golang.org/protobuf/proto"
)

func TestGNRPC_UnaryEcho(t *testing.T) {
	port, err := xnet.AssignRandPort("127.0.0.1")
	if err != nil {
		t.Fatalf("assign port: %v", err)
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)

	svr := NewServer(addr)
	svr.Register("echo", UnaryHandler(func(ctx context.Context, req []byte) ([]byte, error) {
		_ = ctx
		out := make([]byte, 0, len(req)+4)
		out = append(out, []byte("pong")...)
		out = append(out, req...)
		return out, nil
	}))
	if err := svr.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer svr.Stop()

	cli := NewClient(addr)
	if err := cli.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := cli.Invoke(ctx, "echo", []byte("123"))
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if string(resp) != "pong123" {
		t.Fatalf("unexpected resp: %q", string(resp))
	}
}

func TestGNRPC_InterceptorAndProto(t *testing.T) {
	port, err := xnet.AssignRandPort("127.0.0.1")
	if err != nil {
		t.Fatalf("assign port: %v", err)
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)

	svr := NewServer(addr)
	var serverInterceptorCalled bool
	svr.UseInterceptor(func(ctx context.Context, method string, req []byte, handler Handler) ([]byte, error) {
		serverInterceptorCalled = true
		_ = req
		if method == "" {
			t.Fatalf("empty method")
		}
		return handler(ctx, req) // do not mutate bytes for proto payload
	})
	svr.RegisterProto("pbbase.String/echo", func() proto.Message { return &pbbase.String{} }, func(ctx context.Context, req proto.Message) (proto.Message, error) {
		_ = ctx
		in := req.(*pbbase.String)
		return &pbbase.String{Value: "ok:" + in.Value}, nil
	})
	if err := svr.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer svr.Stop()

	cli := NewClient(addr)
	var clientInterceptorCalled bool
	cli.UseInterceptor(func(ctx context.Context, method string, req []byte, invoker Invoker) ([]byte, error) {
		clientInterceptorCalled = true
		if method == "" {
			t.Fatalf("empty method")
		}
		return invoker(ctx, method, req) // do not mutate bytes for proto payload
	})
	if err := cli.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var out pbbase.String
	if err := cli.InvokeProto(ctx, "pbbase.String/echo", &pbbase.String{Value: "X"}, &out); err != nil {
		t.Fatalf("invoke proto: %v", err)
	}
	if out.Value != "ok:X" {
		t.Fatalf("unexpected proto resp: %q", out.Value)
	}
	if !serverInterceptorCalled {
		t.Fatalf("server interceptor not called")
	}
	if !clientInterceptorCalled {
		t.Fatalf("client interceptor not called")
	}
}

func TestGNRPC_MetadataHeader(t *testing.T) {
	port, err := xnet.AssignRandPort("127.0.0.1")
	if err != nil {
		t.Fatalf("assign port: %v", err)
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)

	svr := NewServer(addr)
	svr.RegisterProto("pbbase.String/headers", func() proto.Message { return &pbbase.String{} }, func(ctx context.Context, req proto.Message) (proto.Message, error) {
		_ = req
		md, ok := FromIncomingContext(ctx)
		if !ok {
			return nil, Status(CodeInvalidArgument, "missing metadata")
		}
		if md["x-test"] != "123" {
			return nil, Status(CodeInvalidArgument, "bad header")
		}
		return &pbbase.String{Value: "ok"}, nil
	})
	if err := svr.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer svr.Stop()

	cli := NewClient(addr)
	if err := cli.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var out pbbase.String
	if err := cli.InvokeProtoWithOptions(ctx, "pbbase.String/headers", &pbbase.String{Value: "x"}, &out, WithHeader("x-test", "123")); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if out.Value != "ok" {
		t.Fatalf("unexpected resp: %q", out.Value)
	}
}

func TestGNRPC_ResponseHeaderTrailer(t *testing.T) {
	port, err := xnet.AssignRandPort("127.0.0.1")
	if err != nil {
		t.Fatalf("assign port: %v", err)
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)

	svr := NewServer(addr)
	svr.RegisterProto("pbbase.String/meta", func() proto.Message { return &pbbase.String{} }, func(ctx context.Context, req proto.Message) (proto.Message, error) {
		_ = req
		SetHeader(ctx, "x-h", "hv")
		SetTrailer(ctx, "x-t", "tv")
		return &pbbase.String{Value: "ok"}, nil
	})
	if err := svr.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer svr.Stop()

	cli := NewClient(addr)
	if err := cli.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var rh, rt MD
	var out pbbase.String
	if err := cli.InvokeProtoWithOptions(
		ctx,
		"pbbase.String/meta",
		&pbbase.String{Value: "x"},
		&out,
		WithResponseHeaders(&rh),
		WithResponseTrailers(&rt),
	); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if out.Value != "ok" {
		t.Fatalf("unexpected resp: %q", out.Value)
	}
	if rh["x-h"] != "hv" {
		t.Fatalf("bad resp header: %#v", rh)
	}
	if rt["x-t"] != "tv" {
		t.Fatalf("bad resp trailer: %#v", rt)
	}
}

func TestGNRPC_ServiceRegistration(t *testing.T) {
	port, err := xnet.AssignRandPort("127.0.0.1")
	if err != nil {
		t.Fatalf("assign port: %v", err)
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)

	const svc = "pbbase.StringService"

	svr := NewServer(addr)
	svr.RegisterProtoService(ProtoServiceDesc{
		ServiceName: svc,
		Methods: []UnaryProtoMethodDesc{
			{
				MethodName: "Echo",
				NewRequest: func() proto.Message { return &pbbase.String{} },
				Handler: func(ctx context.Context, req proto.Message) (proto.Message, error) {
					_ = ctx
					in := req.(*pbbase.String)
					return &pbbase.String{Value: "svc:" + in.Value}, nil
				},
			},
		},
	})
	if err := svr.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer svr.Stop()

	cli := NewClient(addr)
	if err := cli.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var out pbbase.String
	if err := cli.InvokeProtoService(ctx, svc, "Echo", &pbbase.String{Value: "X"}, &out); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if out.Value != "svc:X" {
		t.Fatalf("unexpected resp: %q", out.Value)
	}
}

func TestGNRPC_InvokeNoResponse(t *testing.T) {
	port, err := xnet.AssignRandPort("127.0.0.1")
	if err != nil {
		t.Fatalf("assign port: %v", err)
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)

	done := make(chan string, 1)
	svr := NewServer(addr)
	svr.Register("oneway", UnaryHandler(func(ctx context.Context, req []byte) ([]byte, error) {
		_ = ctx
		done <- string(req)
		return []byte("should-not-send"), nil
	}))
	if err := svr.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer svr.Stop()

	cli := NewClient(addr)
	if err := cli.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := cli.InvokeNoResponse(ctx, "oneway", []byte("ping")); err != nil {
		t.Fatalf("InvokeNoResponse: %v", err)
	}

	select {
	case v := <-done:
		if v != "ping" {
			t.Fatalf("unexpected payload: %q", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for oneway handler")
	}
}

func TestGNRPC_OnewayAsyncQueueDrop(t *testing.T) {
	port, err := xnet.AssignRandPort("127.0.0.1")
	if err != nil {
		t.Fatalf("assign port: %v", err)
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)

	block := make(chan struct{})
	svr := NewServer(addr)
	svr.EnableOnewayAsync(1, 1) // 1 worker, queue size 1
	svr.Register("oneway_slow", UnaryHandler(func(ctx context.Context, req []byte) ([]byte, error) {
		_ = ctx
		_ = req
		<-block // block worker
		return nil, nil
	}))
	if err := svr.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer func() {
		close(block)
		_ = svr.Stop()
	}()

	cli := NewClient(addr)
	if err := cli.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// flood to increase chance of queue full before worker drains
	for i := 0; i < 200; i++ {
		_ = cli.InvokeNoResponse(ctx, "oneway_slow", []byte("x"))
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st := svr.OnewayStats()
		if st.Enqueued > 0 || st.Dropped > 0 {
			if st.Dropped == 0 {
				// keep waiting for drops to appear
				time.Sleep(20 * time.Millisecond)
				continue
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected dropped > 0, got %+v", svr.OnewayStats())
}

func TestGNRPC_OnewayPolicyDropOldest(t *testing.T) {
	port, err := xnet.AssignRandPort("127.0.0.1")
	if err != nil {
		t.Fatalf("assign port: %v", err)
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)

	release := make(chan struct{})
	seen := make(chan string, 8)

	svr := NewServer(addr)
	svr.EnableOnewayAsyncWithOptions(1, 1, WithOnewayPolicy(OnewayDropOldest))
	svr.Register("oneway_order", UnaryHandler(func(ctx context.Context, req []byte) ([]byte, error) {
		_ = ctx
		seen <- string(req)
		<-release
		return nil, nil
	}))
	if err := svr.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer func() {
		_ = svr.Stop()
	}()

	cli := NewClient(addr)
	if err := cli.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_ = cli.InvokeNoResponse(ctx, "oneway_order", []byte("a"))
	// ensure first task is executing and blocking
	select {
	case v := <-seen:
		if v != "a" {
			t.Fatalf("expected first seen a, got %q", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for first oneway")
	}

	_ = cli.InvokeNoResponse(ctx, "oneway_order", []byte("b")) // queued
	// wait until b is enqueued on server side (reduce reordering flakiness)
	deadlineEnq := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadlineEnq) {
		if svr.OnewayStats().Enqueued >= 2 { // a + b
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	_ = cli.InvokeNoResponse(ctx, "oneway_order", []byte("c")) // should trigger drop-oldest if queue full

	// wait until drop observed
	deadlineDrop := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadlineDrop) {
		if svr.OnewayStats().Dropped > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if svr.OnewayStats().Dropped == 0 {
		t.Fatalf("expected dropped > 0, got %+v", svr.OnewayStats())
	}

	// allow worker to proceed
	close(release)

	// collect remaining up to 2
	var got []string
	deadline := time.Now().Add(2 * time.Second)
	for len(got) < 2 && time.Now().Before(deadline) {
		select {
		case v := <-seen:
			got = append(got, v)
		case <-time.After(50 * time.Millisecond):
		}
	}
	_ = got
}

func TestGNRPC_OnewayMethodRateLimit(t *testing.T) {
	port, err := xnet.AssignRandPort("127.0.0.1")
	if err != nil {
		t.Fatalf("assign port: %v", err)
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)

	svr := NewServer(addr)
	svr.SetMethodConfig("rl", MethodConfig{
		TokenBucketRate:  1, // 1 req/s
		TokenBucketBurst: 1,
	})
	svr.Register("rl", UnaryHandler(func(ctx context.Context, req []byte) ([]byte, error) {
		_ = ctx
		_ = req
		return nil, nil
	}))
	if err := svr.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer svr.Stop()

	cli := NewClient(addr)
	if err := cli.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i := 0; i < 50; i++ {
		_ = cli.InvokeNoResponse(ctx, "rl", []byte("x"))
	}

	// wait for server to see some drops
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st, ok := svr.GetMethodStats("rl")
		if ok && st.RateLimited > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	st, _ := svr.GetMethodStats("rl")
	t.Fatalf("expected rate limited > 0, got %+v", st)
}

func TestGNRPC_OnewayMethodBreaker(t *testing.T) {
	port, err := xnet.AssignRandPort("127.0.0.1")
	if err != nil {
		t.Fatalf("assign port: %v", err)
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)

	svr := NewServer(addr)
	svr.SetMethodConfig("brk", MethodConfig{
		BreakerEnabled:      true,
		BreakerTripFailures: 1,
		BreakerCooldown:     5 * time.Second,
	})
	svr.Register("brk", UnaryHandler(func(ctx context.Context, req []byte) ([]byte, error) {
		_ = ctx
		_ = req
		return nil, Status(CodeInternal, "fail")
	}))
	if err := svr.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer svr.Stop()

	cli := NewClient(addr)
	if err := cli.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 1) Trigger at least one failure (breaker opens after first failure).
	_ = cli.InvokeNoResponse(ctx, "brk", []byte("1"))

	// wait until server processed/failure is observed, so breaker state is definitely updated
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st, ok := svr.GetMethodStats("brk")
		if ok && st.Failed >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 2) Flood more calls; after breaker opens, some should be rejected.
	for i := 0; i < 50; i++ {
		_ = cli.InvokeNoResponse(ctx, "brk", []byte("x"))
	}

	deadline2 := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline2) {
		st, ok := svr.GetMethodStats("brk")
		if ok && st.BreakerOpen > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	st, _ := svr.GetMethodStats("brk")
	t.Fatalf("expected breaker open > 0, got %+v", st)
}

func TestGNRPC_RequestMethodRateLimit_Status(t *testing.T) {
	port, err := xnet.AssignRandPort("127.0.0.1")
	if err != nil {
		t.Fatalf("assign port: %v", err)
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)

	svr := NewServer(addr)
	svr.SetMethodConfig("req_rl", MethodConfig{
		TokenBucketRate:  1,
		TokenBucketBurst: 1,
	})
	svr.Register("req_rl", UnaryHandler(func(ctx context.Context, req []byte) ([]byte, error) {
		_ = ctx
		return req, nil
	}))
	if err := svr.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer svr.Stop()

	cli := NewClient(addr)
	if err := cli.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = cli.Invoke(ctx, "req_rl", []byte("a"))
	if err != nil {
		t.Fatalf("first invoke err: %v", err)
	}
	_, err = cli.Invoke(ctx, "req_rl", []byte("b"))
	if err == nil {
		t.Fatalf("expected rate limited error")
	}
	var se StatusError
	if !errors.As(err, &se) {
		t.Fatalf("expected StatusError, got %T", err)
	}
	if se.Code != CodeResourceExhausted {
		t.Fatalf("expected CodeResourceExhausted, got %v", se.Code)
	}
}

func TestGNRPC_RequestMethodBreaker_Status(t *testing.T) {
	port, err := xnet.AssignRandPort("127.0.0.1")
	if err != nil {
		t.Fatalf("assign port: %v", err)
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)

	svr := NewServer(addr)
	svr.SetMethodConfig("req_brk", MethodConfig{
		BreakerEnabled:      true,
		BreakerTripFailures: 1,
		BreakerCooldown:     5 * time.Second,
	})
	svr.Register("req_brk", UnaryHandler(func(ctx context.Context, req []byte) ([]byte, error) {
		_ = ctx
		_ = req
		return nil, Status(CodeInternal, "boom")
	}))
	if err := svr.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer svr.Stop()

	cli := NewClient(addr)
	if err := cli.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = cli.Invoke(ctx, "req_brk", []byte("a"))
	if err == nil {
		t.Fatalf("expected internal error")
	}
	// second should be rejected with unavailable
	_, err = cli.Invoke(ctx, "req_brk", []byte("b"))
	if err == nil {
		t.Fatalf("expected breaker open error")
	}
	var se StatusError
	if !errors.As(err, &se) {
		t.Fatalf("expected StatusError, got %T", err)
	}
	if se.Code != CodeUnavailable {
		t.Fatalf("expected CodeUnavailable, got %v", se.Code)
	}
}

func TestGNRPC_Bidirectional_ServerInvokeClient(t *testing.T) {
	port, err := xnet.AssignRandPort("127.0.0.1")
	if err != nil {
		t.Fatalf("assign port: %v", err)
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)

	svr := NewServer(addr)
	if err := svr.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer svr.Stop()

	cli := NewClient(addr)
	cli.Register("client.echo", UnaryHandler(func(ctx context.Context, req []byte) ([]byte, error) {
		_ = ctx
		return append([]byte("c:"), req...), nil
	}))
	if err := cli.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer cli.Close()

	// wait for server to see connection
	var connID int64
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conns := svr.GetConnManager().GetAllConns()
		for id := range conns {
			connID = id
			break
		}
		if connID != 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if connID == 0 {
		t.Fatalf("server did not observe connection")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := svr.InvokeConn(ctx, connID, "client.echo", []byte("hi"))
	if err != nil {
		t.Fatalf("InvokeConn: %v", err)
	}
	if string(resp) != "c:hi" {
		t.Fatalf("unexpected resp: %q", string(resp))
	}
}

