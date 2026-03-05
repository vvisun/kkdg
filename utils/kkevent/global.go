package kkevent

var globalEventBus Bus = NewEventBus()

func GetGlobalEventBus() Bus {
	return globalEventBus
}
