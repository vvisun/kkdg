package kkcluster

import (
	"sync"

	"github.com/nats-io/nats.go"
)

var (
	_msgPool = &sync.Pool{
		New: func() interface{} {
			return &nats.Msg{}
		},
	}
)

func NewNatsMsg() *nats.Msg {
	value := _msgPool.Get()
	msg := value.(*nats.Msg)
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	return msg
}

func FreeNatsMsg(msg *nats.Msg) {
	for key := range msg.Header {
		delete(msg.Header, key)
	}
	msg.Data = nil
	_msgPool.Put(msg)
}
