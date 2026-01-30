package gnrpc

import (
	"fmt"

	"github.com/vvisun/kkdg/utils/kkcodec"
)

type codec struct {
	typ uint8
}

func newCodec(codecType uint8) (*codec, error) {
	if kkcodec.GetCodec(codecType) == nil {
		return nil, fmt.Errorf("gnrpc: invalid codecType=%d", codecType)
	}
	return &codec{typ: codecType}, nil
}

func (c *codec) Marshal(v any) ([]byte, error) {
	cc := kkcodec.GetCodec(c.typ)
	return cc.Marshal(v)
}

func (c *codec) Unmarshal(data []byte, v any) error {
	cc := kkcodec.GetCodec(c.typ)
	return cc.Unmarshal(data, v)
}

