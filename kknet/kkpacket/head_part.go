package kkpacket

import (
	"encoding/binary"

	"github.com/vvisun/kkdg/kkerrors"
)

// [head]的每个part的接口。用于编码解码[head]部分。
type IHeadPart interface {
	// 将value编码到data中
	Marshal(data []byte, endian binary.ByteOrder, value int) error
	// 从data中解析出value，并返回value
	Unmarshal(data []byte, endian binary.ByteOrder) (int, error)
	// 该part的长度（字节数）
	GetSize() int
}

//----------------------------------------------------

type PartUint16 struct{}

var _ IHeadPart = (*PartUint16)(nil)

func (p *PartUint16) Marshal(data []byte, endian binary.ByteOrder, value int) error {
	if len(data) < 2 {
		return kkerrors.ErrDataTooShortToMarshal
	}
	endian.PutUint16(data[:2], uint16(value))
	return nil
}

func (p *PartUint16) Unmarshal(data []byte, endian binary.ByteOrder) (int, error) {
	if len(data) < 2 {
		return 0, kkerrors.ErrDataTooShortToUnmarshal
	}
	return int(endian.Uint16(data[:2])), nil
}

func (p *PartUint16) GetSize() int {
	return 2
}

//----------------------------------------------------

type PartUint32 struct{}

var _ IHeadPart = (*PartUint32)(nil)

func (p *PartUint32) Marshal(data []byte, endian binary.ByteOrder, value int) error {
	if len(data) < 4 {
		return kkerrors.ErrDataTooShortToMarshal
	}
	endian.PutUint32(data[:4], uint32(value))
	return nil
}

func (p *PartUint32) Unmarshal(data []byte, endian binary.ByteOrder) (int, error) {
	if len(data) < 4 {
		return 0, kkerrors.ErrDataTooShortToUnmarshal
	}
	return int(endian.Uint32(data[:4])), nil
}

func (p *PartUint32) GetSize() int {
	return 4
}

//----------------------------------------------------

type PartUint64 struct{}

var _ IHeadPart = (*PartUint64)(nil)

func (p *PartUint64) Marshal(data []byte, endian binary.ByteOrder, value int) error {
	if len(data) < 8 {
		return kkerrors.ErrDataTooShortToMarshal
	}
	endian.PutUint64(data[:8], uint64(value))
	return nil
}

func (p *PartUint64) Unmarshal(data []byte, endian binary.ByteOrder) (int, error) {
	if len(data) < 8 {
		return 0, kkerrors.ErrDataTooShortToUnmarshal
	}
	return int(endian.Uint64(data[:8])), nil
}

func (p *PartUint64) GetSize() int {
	return 8
}
