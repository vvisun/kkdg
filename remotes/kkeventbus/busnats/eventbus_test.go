package nats

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/remotes/kkeventbus"
)

func testNatsURL(t *testing.T) string {
	t.Helper()
	url := "nats://127.0.0.1:4222"
	nc, err := nats.Connect(url, nats.Timeout(500*time.Millisecond))
	if err != nil {
		t.Skipf("nats not available at %s: %v", url, err)
	}
	nc.Close()
	return url
}

func testSubjectPrefix(t *testing.T) string {
	return "kkt_" + strings.ReplaceAll(t.Name(), "/", "_") + ":"
}

func TestEventbus_Subscribe_nilHandler(t *testing.T) {
	url := testNatsURL(t)
	eb := NewEventbus(WithUrl(url), WithTimeout(500*time.Millisecond), WithPrefix(testSubjectPrefix(t)))
	ctx := context.Background()
	_, err := eb.Subscribe(ctx, "t", nil)
	if !errors.Is(err, kkerrors.ErrInvalidHandler) {
		t.Fatalf("got %v, want ErrInvalidHandler", err)
	}
}

func TestEventbus_PublishSubscribe_roundtrip(t *testing.T) {
	url := testNatsURL(t)
	prefix := testSubjectPrefix(t)
	nc, err := nats.Connect(url, nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })

	eb := NewEventbus(WithConn(nc), WithPrefix(prefix))
	ctx := context.Background()

	var n atomic.Int32
	var gotTopic string
	h := func(e *kkeventbus.Event) {
		gotTopic = e.Topic
		n.Add(1)
	}
	if _, err := eb.Subscribe(ctx, "evt", h); err != nil {
		t.Fatal(err)
	}
	if err := eb.Publish(ctx, "evt", []byte("ping")); err != nil {
		t.Fatal(err)
	}
	waitUntilBus(t, func() bool { return n.Load() == 1 }, 3*time.Second)
	if gotTopic != "evt" {
		t.Fatalf("topic %q", gotTopic)
	}
}

func TestEventbus_Unsubscribe_stopsDelivery(t *testing.T) {
	url := testNatsURL(t)
	nc, err := nats.Connect(url, nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })

	eb := NewEventbus(WithConn(nc), WithPrefix(testSubjectPrefix(t)))
	ctx := context.Background()
	var n atomic.Int32
	h := func(e *kkeventbus.Event) { n.Add(1) }
	if _, err := eb.Subscribe(ctx, "x", h); err != nil {
		t.Fatal(err)
	}
	if err := eb.Unsubscribe(ctx, "x", h); err != nil {
		t.Fatal(err)
	}
	if err := eb.Publish(ctx, "x", []byte("a")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if n.Load() != 0 {
		t.Fatalf("handler ran %d times after unsubscribe", n.Load())
	}
}

func TestEventbus_UnsubscribeByID(t *testing.T) {
	url := testNatsURL(t)
	nc, err := nats.Connect(url, nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })

	eb := NewEventbus(WithConn(nc), WithPrefix(testSubjectPrefix(t)))
	ctx := context.Background()
	var n atomic.Int32
	h := func(e *kkeventbus.Event) { n.Add(1) }
	id, err := eb.Subscribe(ctx, "y", h)
	if err != nil || id == 0 {
		t.Fatalf("Subscribe: id=%d err=%v", id, err)
	}
	if err := eb.UnsubscribeByID(ctx, "y", id); err != nil {
		t.Fatal(err)
	}
	if err := eb.Publish(ctx, "y", []byte("b")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if n.Load() != 0 {
		t.Fatal("handler after UnsubscribeByID")
	}
}

func waitUntilBus(t *testing.T, cond func() bool, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met")
}
