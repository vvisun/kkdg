package kkevent

// defaultBus is a global event bus instance for simple use cases.
var defaultBus = NewEventBus()

// Publish sends an event on the default bus.
func Publish(topic string, args ...interface{}) {
	defaultBus.Publish(topic, args...)
}

// Subscribe registers a handler on the default bus.
func Subscribe(topic string, fn interface{}) error {
	return defaultBus.Subscribe(topic, fn)
}
