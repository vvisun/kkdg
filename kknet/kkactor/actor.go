package kkactor

import (
	"errors"
	"time"
)

// Actor represents a unit that processes messages sequentially.
type Actor interface {
	Receive(ctx Context)
}

// Context exposes actor execution context.
type Context interface {
	Message() any
	Sender() *PID
	Self() *PID
	System() *ActorSystem

	Respond(msg any)
	Send(pid *PID, msg any)
	RequestFuture(pid *PID, msg any, timeout ...time.Duration) *Future
	Spawn(props *Props) *PID
	Stop(pid *PID)
	StopFuture(pid *PID) *Future
}

// Producer creates a new actor instance.
type Producer func() Actor

// Props describes how to create and run an actor.
type Props struct {
	Producer    Producer
	MailboxSize int
}

const defaultMailboxSize = 128

// FromProducer creates props from a producer.
func FromProducer(p Producer) *Props {
	return &Props{
		Producer:    p,
		MailboxSize: defaultMailboxSize,
	}
}

// NewProps is an alias for FromProducer.
func NewProps(p Producer) *Props {
	return FromProducer(p)
}

// WithMailboxSize sets mailbox size.
func (p *Props) WithMailboxSize(size int) *Props {
	if p == nil {
		return p
	}
	if size > 0 {
		p.MailboxSize = size
	}
	return p
}

var (
	ErrNoProducer            = errors.New("kkactor: props producer is nil")
	ErrActorDead             = errors.New("kkactor: actor not found or stopped")
	ErrMailboxFull           = errors.New("kkactor: mailbox is full")
	ErrTimeout               = errors.New("kkactor: request timed out")
	ErrRemoteNotConfigured   = errors.New("kkactor: remote not configured")
	ErrMessageNotRegistered  = errors.New("kkactor: message type not registered")
	ErrRemoteRequestFailed   = errors.New("kkactor: remote request failed")
	ErrRemoteDecodeFailed    = errors.New("kkactor: remote decode failed")
	ErrRemoteEncodeFailed    = errors.New("kkactor: remote encode failed")
	ErrRemoteUnsupportedType = errors.New("kkactor: remote unsupported message type")
)

// ActorEventType represents connection event type.
type ActorEventType int

const (
	ActorEventConnect ActorEventType = iota
	ActorEventMessage
	ActorEventClose
)

// ActorEvent is delivered to actors, typically by adapters.
type ActorEvent struct {
	Type ActorEventType
	Conn any
	Data []byte
	Err  error
}
