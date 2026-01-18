package kkcodec

import (
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
}

const (
	CodecTypeJson     = 0
	CodecTypeProtoBuf = 1
	CodecTypeMsgpack  = 2
	CodecTypeYaml     = 3
	CodecTypeXml      = 4
	CodecTypeToml     = 5
)

func GetCodec(codecType int) ICodec {
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
