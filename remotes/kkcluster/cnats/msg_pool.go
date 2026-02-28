package cnats

import (
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
)

var msgCodec = kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)

func SetMsgCodec(codec kkcodec.ICodec) {
	if codec == nil {
		kklog.Errorf("[kkcluster] SetMsgCodec codec is nil, use default codec")
		codec = kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)
	}
	msgCodec = codec
}

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
