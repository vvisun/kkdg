package msgrouter

import (
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

type MsgSender struct {
	connId kknet.CONN_ID
	codec  kkcodec.ICodec
}

func (s *MsgSender) SendMsg(msg any) error {
	return nil
}
