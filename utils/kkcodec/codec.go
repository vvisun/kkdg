package kkcodec

import (
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec/json"
	"github.com/vvisun/kkdg/utils/kkcodec/msgpack"
	"github.com/vvisun/kkdg/utils/kkcodec/proto"
	"github.com/vvisun/kkdg/utils/kkcodec/toml"
	"github.com/vvisun/kkdg/utils/kkcodec/xml"
	"github.com/vvisun/kkdg/utils/kkcodec/yaml"
)

type ICodec interface {
	// Marshal 编码
	Marshal(v any) ([]byte, error)
	// Unmarshal 解码
	Unmarshal(data []byte, v any) error
	// MarshalAppend 编码
	MarshalAppend(v any, offset int) (*kkbuffer.ByteBuffer, error)
}

const (
	CodecTypeJson uint8 = iota
	CodecTypeProtoBuf
	CodecTypeMsgpack
	CodecTypeYaml
	CodecTypeXml
	CodecTypeToml
)

func GetCodec(codecType uint8) ICodec {
	switch codecType {
	case CodecTypeJson:
		return json.DefaultCodec
	case CodecTypeProtoBuf:
		return proto.DefaultCodec
	case CodecTypeMsgpack:
		return msgpack.DefaultCodec
	case CodecTypeYaml:
		return yaml.DefaultCodec
	case CodecTypeXml:
		return xml.DefaultCodec
	case CodecTypeToml:
		return toml.DefaultCodec
	default:
		return nil
	}
}
