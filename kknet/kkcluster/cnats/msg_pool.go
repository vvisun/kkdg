package cnats

import (
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

var msgCodec = kkcodec.GetCodec(kkcodec.CodecTypeJson)

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
