package kkpacket

import (
	"encoding/binary"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
)

// [head]的每个part的接口。用于编码解码[head]部分。
type HeadPart interface {
	// 将value编码到data中
	Marshal(data []byte, endian binary.ByteOrder, value int) error
	// 从data中解析出value，并返回value
	Unmarshal(data []byte, endian binary.ByteOrder) (int, error)
	// 该part的长度（字节数）
	GetSize() int
}

//----------------------------------------------------

type PartUint16 struct{}

var _ HeadPart = (*PartUint16)(nil)

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

var _ HeadPart = (*PartUint32)(nil)

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

var _ HeadPart = (*PartUint64)(nil)

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

//----------------------------------------------------

// [head]。用于编码解码[head]部分。
type PacketHead struct {
	partList []HeadPart // [head]的各个部分
	size     int        // [head]的总长度
}

func NewPacketHead(parts ...HeadPart) *PacketHead {
	if len(parts) > maxHeadPathCount {
		kklog.Errorf("[head] parts count is too many, max is %d", maxHeadPathCount)
		panic("[head] parts count is too many")
	}
	size := 0
	for _, part := range parts {
		size += part.GetSize()
	}
	return &PacketHead{partList: parts, size: size}
}

func (h *PacketHead) GetSize() int {
	return h.size
}

func (h *PacketHead) GetPartCount() int {
	return len(h.partList)
}

func (h *PacketHead) Marshal(headBytes []byte, endian binary.ByteOrder, valueList ...int) error {
	if len(headBytes) < h.size {
		return kkerrors.ErrDataTooShortToMarshal
	}
	offset := 0
	for i, part := range h.partList {
		if err := part.Marshal(headBytes[offset:offset+part.GetSize()], endian, valueList[i]); err != nil {
			return err
		}
		offset += part.GetSize()
	}
	return nil
}

func (h *PacketHead) Unmarshal(headBytes []byte, endian binary.ByteOrder) ([]int, error) {
	if len(headBytes) < h.size {
		return nil, kkerrors.ErrDataTooShortToUnmarshal
	}
	valueList := make([]int, len(h.partList))
	offset := 0
	for i, part := range h.partList {
		value, err := part.Unmarshal(headBytes[offset:offset+part.GetSize()], endian)
		if err != nil {
			return nil, err
		}
		valueList[i] = value
		offset += part.GetSize()
	}
	return valueList, nil
}

func (h *PacketHead) UnmarshalTo(headBytes []byte, endian binary.ByteOrder, valueList []int) ([]int, error) {
	if len(headBytes) < h.size || h.GetPartCount() <= 0 {
		return valueList[:0], kkerrors.ErrDataTooShortToUnmarshal
	}
	if len(valueList) < h.GetPartCount() {
		valueList = make([]int, h.GetPartCount())
	}
	offset := 0
	for i, part := range h.partList {
		value, err := part.Unmarshal(headBytes[offset:offset+part.GetSize()], endian)
		if err != nil {
			return valueList[:i], err
		}
		valueList[i] = value
		offset += part.GetSize()
	}
	return valueList[:h.GetPartCount()], nil
}
