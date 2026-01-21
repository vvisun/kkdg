package ccclient

import "time"

// Option configures the client component.
type Option struct {
	TCPAddr      string
	WSURL        string
	Payload      []byte
	SendInterval time.Duration
	SendCount    int
}
