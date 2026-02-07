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
	// MarshalAppend 编码, 编码的数据在offset偏移后的位置，少一次拷贝，offset前的数据一般用于存放消息头。
	MarshalAppend(v any, offset int) (*kkbuffer.ByteBuffer, error)
}

type CodecType uint8

const (
	CodecTypeJson CodecType = iota
	CodecTypeProtoBuf
	CodecTypeMsgpack
	CodecTypeYaml
	CodecTypeXml
	CodecTypeToml
)

func GetCodec(codecType CodecType) ICodec {
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
