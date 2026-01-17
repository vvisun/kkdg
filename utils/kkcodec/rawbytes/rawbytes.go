package rawbytes

import "github.com/vvisun/kkdg/kkerrors"

const Name = "rawbytes"

var DefaultCodec = &codec{}

type codec struct{}

// Name 编解码器名称
func (codec) Name() string {
	return Name
}

// Marshal 编码
func (codec) Marshal(v any) ([]byte, error) {
	return v.([]byte), nil
}

// Unmarshal 解码
func (codec) Unmarshal(data []byte, v any) error {
	bytes, ok := v.([]byte)
	if &bytes == &data { //同一个切片，无需解码拷贝，直接返回
		return nil
	}
	if !ok {
		return kkerrors.ErrNotByteSlice
	}
	copy(bytes, data)
	return nil
}

// Marshal 编码
func Marshal(v any) ([]byte, error) {
	return DefaultCodec.Marshal(v)
}

// Unmarshal 解码
func Unmarshal(data []byte, v any) error {
	return DefaultCodec.Unmarshal(data, v)
}
