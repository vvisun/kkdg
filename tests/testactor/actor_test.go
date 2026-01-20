package testactor

import (
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet/kkactor"
)

// 基础的本地 Actor 收发与 Stop 测试
func TestActor_SendAndStop(t *testing.T) {
	sys := kkactor.NewActorSystem()
	defer sys.Stop()

	received := make(chan interface{}, 1)

	pid := sys.Spawn(kkactor.PropsFromFunc(func(ctx kkactor.Context) {
		received <- ctx.Message()
	}))
	if pid == nil {
		t.Fatal("Spawn returned nil PID")
	}

	// 发送一条消息
	msg := []byte("hello")
	pid.Tell(msg)

	select {
	case v := <-received:
		b, ok := v.([]byte)
		if !ok {
			t.Fatalf("expected []byte, got %T", v)
		}
		if string(b) != "hello" {
			t.Fatalf("unexpected payload: %q", string(b))
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for actor message")
	}

	// Stop 可安全重复调用
	sys.Stop()
	sys.Stop()
}

// 本地 Ask/Reply 语义测试
func TestActor_AskLocal(t *testing.T) {
	sys := kkactor.NewActorSystem()
	defer sys.Stop()

	replyPayload := []byte("ok")

	pid := sys.Spawn(kkactor.PropsFromFunc(func(ctx kkactor.Context) {
		switch m := ctx.Message().(type) {
		case []byte:
			if string(m) != "ping" {
				t.Errorf("expected ping, got %q", string(m))
			}
			if ctx.Sender() == nil {
				t.Errorf("expected non-nil sender in Ask")
			}
			// 回复调用方
			ctx.Send(ctx.Sender(), replyPayload)
		}
	}))
	if pid == nil {
		t.Fatal("Spawn returned nil PID")
	}

	root := kkactor.NewRootContext(sys)

	resp, ok := root.Ask(pid, []byte("ping"), time.Second)
	if !ok {
		t.Fatal("expected reply, got timeout")
	}

	b, ok := resp.([]byte)
	if !ok {
		t.Fatalf("expected []byte reply, got %T", resp)
	}
	if string(b) != string(replyPayload) {
		t.Fatalf("unexpected reply: %q, want %q", b, replyPayload)
	}
}

// Timer 相关能力测试（After + Tick + Cancel）
func TestActor_Timers(t *testing.T) {
	sys := kkactor.NewActorSystem()
	defer sys.Stop()

	done := make(chan struct{})
	tickCh := make(chan struct{}, 8)

	var tickID kkactor.TimerID

	pid := sys.Spawn(kkactor.PropsFromFunc(func(ctx kkactor.Context) {
		switch m := ctx.Message().(type) {
		case string:
			switch m {
			case "start":
				// 安排一个一次性延迟停止的定时器和一个周期性 tick
				ctx.After(50*time.Millisecond, "stop")
				tickID = ctx.Tick(10*time.Millisecond, "tick")
			case "tick":
				select {
				case tickCh <- struct{}{}:
				default:
				}
			case "stop":
				ctx.CancelTimer(tickID)
				close(done)
			}
		}
	}))

	if pid == nil {
		t.Fatal("Spawn returned nil PID")
	}

	// 触发定时逻辑
	pid.Tell("start")

	// 至少应收到一个 tick
	select {
	case <-tickCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("did not receive tick event")
	}

	// 最终应收到 stop 信号
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("did not receive stop event from After timer")
	}
}
