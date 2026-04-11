package buslocal

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/remotes/kkeventbus"
)

func TestEventbus_Subscribe_nilHandler(t *testing.T) {
	eb := NewEventbus()
	ctx := context.Background()
	_, err := eb.Subscribe(ctx, "t", nil)
	if !errors.Is(err, kkerrors.ErrInvalidHandler) {
		t.Fatalf("Subscribe nil: %v, want ErrInvalidHandler", err)
	}
}

func TestEventbus_Unsubscribe_nilHandler(t *testing.T) {
	eb := NewEventbus()
	ctx := context.Background()
	err := eb.Unsubscribe(ctx, "t", nil)
	if !errors.Is(err, kkerrors.ErrInvalidHandler) {
		t.Fatalf("Unsubscribe nil: %v, want ErrInvalidHandler", err)
	}
}

func TestEventbus_Publish_roundtrip(t *testing.T) {
	eb := NewEventbus()
	ctx := context.Background()
	var gotTopic string
	var n atomic.Int32
	h := func(e *kkeventbus.Event) {
		gotTopic = e.Topic
		n.Add(1)
	}
	if _, err := eb.Subscribe(ctx, "my.topic", h); err != nil {
		t.Fatal(err)
	}
	if err := eb.Publish(ctx, "my.topic", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return n.Load() == 1 }, 2*time.Second)
	if gotTopic != "my.topic" {
		t.Fatalf("topic %q", gotTopic)
	}
}

func TestEventbus_Unsubscribe_removesTopic(t *testing.T) {
	eb := NewEventbus()
	ctx := context.Background()
	var n atomic.Int32
	h := func(e *kkeventbus.Event) { n.Add(1) }
	if _, err := eb.Subscribe(ctx, "t", h); err != nil {
		t.Fatal(err)
	}
	if err := eb.Unsubscribe(ctx, "t", h); err != nil {
		t.Fatal(err)
	}
	if err := eb.Publish(ctx, "t", "x"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if n.Load() != 0 {
		t.Fatalf("handler invoked %d times after unsubscribe", n.Load())
	}
}

func TestEventbus_UnsubscribeByID(t *testing.T) {
	eb := NewEventbus()
	ctx := context.Background()
	var n atomic.Int32
	h := func(e *kkeventbus.Event) { n.Add(1) }
	id, err := eb.Subscribe(ctx, "t", h)
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 {
		t.Fatal("subscribe id 0")
	}
	if err := eb.UnsubscribeByID(ctx, "t", id); err != nil {
		t.Fatal(err)
	}
	if err := eb.Publish(ctx, "t", "x"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if n.Load() != 0 {
		t.Fatalf("handler invoked after UnsubscribeByID")
	}
}

func waitUntil(t *testing.T, cond func() bool, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met")
}
