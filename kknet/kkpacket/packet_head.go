package kkpacket

import (
	"encoding/binary"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
)

// [head] 编码解码器。用于编码解码[head]部分。
type PacketHead struct {
	partList []IHeadPart // [head]的各个部分。长度: 0 ~ maxHeadPathCount
	size     int         // [head]的总字节数
}

func NewPacketHead(parts ...IHeadPart) *PacketHead {
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
	if len(valueList) < len(h.partList) {
		return kkerrors.ErrValueListTooShortToMarshal
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
