package kkpacket

import (
	"encoding/binary"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
)

const max_head_part_count = 4

type HeadPart interface {
	Marshal(data []byte, endian binary.ByteOrder, value int) error
	Unmarshal(data []byte, endian binary.ByteOrder) (int, error)
	GetSize() int // 长度（字节数）
}

//----------------------------------------------------

type PartUint16 struct{}

var _ HeadPart = (*PartUint16)(nil)

func (p *PartUint16) Marshal(data []byte, endian binary.ByteOrder, value int) error {
	endian.PutUint16(data[:2], uint16(value))
	return nil
}

func (p *PartUint16) Unmarshal(data []byte, endian binary.ByteOrder) (int, error) {
	return int(endian.Uint16(data[:2])), nil
}

func (p *PartUint16) GetSize() int {
	return 2
}

//----------------------------------------------------

type PartUint32 struct{}

var _ HeadPart = (*PartUint32)(nil)

func (p *PartUint32) Marshal(data []byte, endian binary.ByteOrder, value int) error {
	endian.PutUint32(data[:4], uint32(value))
	return nil
}

func (p *PartUint32) Unmarshal(data []byte, endian binary.ByteOrder) (int, error) {
	return int(endian.Uint32(data[:4])), nil
}

func (p *PartUint32) GetSize() int {
	return 4
}

//----------------------------------------------------

type PartUint64 struct{}

var _ HeadPart = (*PartUint64)(nil)

func (p *PartUint64) Marshal(data []byte, endian binary.ByteOrder, value int) error {
	endian.PutUint64(data[:8], uint64(value))
	return nil
}

func (p *PartUint64) Unmarshal(data []byte, endian binary.ByteOrder) (int, error) {
	return int(endian.Uint64(data[:8])), nil
}

func (p *PartUint64) GetSize() int {
	return 8
}

//----------------------------------------------------

type PacketHead struct {
	partList []HeadPart // 各个部分
	size     int        // 总长度
}

func NewPacketHead(parts ...HeadPart) *PacketHead {
	if len(parts) > max_head_part_count {
		kklog.Errorf("parts count is too many, max is %d", max_head_part_count)
		panic("parts count is too many")
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

func (h *PacketHead) Marshal(data []byte, endian binary.ByteOrder, valueList ...int) error {
	if len(data) < h.size {
		return kkerrors.ErrDataTooShortToMarshal
	}
	offset := 0
	for i, part := range h.partList {
		if err := part.Marshal(data[offset:offset+part.GetSize()], endian, valueList[i]); err != nil {
			return err
		}
		offset += part.GetSize()
	}
	return nil
}

func (h *PacketHead) Unmarshal(data []byte, endian binary.ByteOrder) ([]int, error) {
	if len(data) < h.size {
		return nil, kkerrors.ErrDataTooShortToUnmarshal
	}
	valueList := make([]int, len(h.partList))
	offset := 0
	for i, part := range h.partList {
		value, err := part.Unmarshal(data[offset:offset+part.GetSize()], endian)
		if err != nil {
			return nil, err
		}
		valueList[i] = value
		offset += part.GetSize()
	}
	return valueList, nil
}

func (h *PacketHead) UnmarshalTo(data []byte, endian binary.ByteOrder, valueList []int) error {
	if len(data) < h.size {
		return kkerrors.ErrDataTooShortToUnmarshal
	}
	offset := 0
	for i, part := range h.partList {
		value, err := part.Unmarshal(data[offset:offset+part.GetSize()], endian)
		if err != nil {
			return err
		}
		valueList[i] = value
		offset += part.GetSize()
	}
	return nil
}
