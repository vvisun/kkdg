package kkevent

import "testing"

func BenchmarkPublish(b *testing.B) {
	bus := NewEventBus()
	topic := "bench"
	handler := func(a int, b string, c bool) {}
	bus.Subscribe(topic, handler)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Publish(topic, 1, "test", true)
	}
}

func BenchmarkPublishAsync(b *testing.B) {
	bus := NewEventBus()
	topic := "bench-async"
	handler := func() {}
	bus.SubscribeAsync(topic, handler, false)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Publish(topic)
	}
	bus.WaitAsync()
}

