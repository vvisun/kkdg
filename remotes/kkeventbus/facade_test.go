package kkeventbus

import (
	"context"
	"errors"
	"testing"

	"github.com/vvisun/kkdg/kkerrors"
)

func TestPublish_noInstance(t *testing.T) {
	prev := globalEventbus
	t.Cleanup(func() { globalEventbus = prev })
	globalEventbus = nil
	err := Publish(context.Background(), "t", "m")
	if !errors.Is(err, kkerrors.ErrMissingEventbusInstance) {
		t.Fatalf("Publish: %v", err)
	}
}

func TestSubscribe_noInstance(t *testing.T) {
	prev := globalEventbus
	t.Cleanup(func() { globalEventbus = prev })
	globalEventbus = nil
	_, err := Subscribe(context.Background(), "t", func(e *Event) {})
	if !errors.Is(err, kkerrors.ErrMissingEventbusInstance) {
		t.Fatalf("Subscribe: %v", err)
	}
}

func TestUnsubscribeByID_noInstance(t *testing.T) {
	prev := globalEventbus
	t.Cleanup(func() { globalEventbus = prev })
	globalEventbus = nil
	err := UnsubscribeByID(context.Background(), "t", 1)
	if !errors.Is(err, kkerrors.ErrMissingEventbusInstance) {
		t.Fatalf("UnsubscribeByID: %v", err)
	}
}
