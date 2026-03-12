package kkpacket

import (
	"encoding/binary"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
)

const (
	PartNameMsgID = "msgID"
)

// [head] 编码解码器。用于编码解码[head]部分。
type PacketHead struct {
	partList []IHeadPart    // [head]的各个部分。长度: 0 ~ maxHeadPathCount
	nameMap  map[string]int // [head]的各个部分的名称。长度: 0 ~ maxHeadPathCount
	size     int            // [head]的总字节数
}

func NewPacketHead(parts ...IHeadPart) *PacketHead {
	if len(parts) > maxHeadPathCount {
		kklog.Errorf("[head] parts count is too many, max is %d", maxHeadPathCount)
		kklog.PanicLog("[head] parts count is too many")
	}
	size := 0
	for _, part := range parts {
		size += part.GetSize()
	}

	head := &PacketHead{partList: parts, size: size, nameMap: make(map[string]int)}
	if len(parts) > 0 {
		// 默认添加msgID部分
		head.nameMap[PartNameMsgID] = 0
	}
	return head
}

func NewPacketHeadWithNames(parts []IHeadPart, names []string) *PacketHead {
	if len(parts) != len(names) {
		kklog.Errorf("[head] parts count is not equal to names count, parts: %d, names: %d", len(parts), len(names))
		kklog.PanicLog("[head] parts count is not equal to names count")
	}
	if len(parts) > maxHeadPathCount {
		kklog.Errorf("[head] parts count is too many, max is %d", maxHeadPathCount)
		kklog.PanicLog("[head] parts count is too many")
	}
	head := NewPacketHead(parts...)
	if err := head.SetNames(names); err != nil {
		kklog.Errorf("[head] set names failed, err: %v", err)
		kklog.PanicLog("[head] set names failed")
	}
	return head
}

func (h *PacketHead) SetNames(names []string) error {
	if len(names) != len(h.partList) {
		return kkerrors.ErrPktNamesAndPartsLengthNotMatch
	}
	nameMap := make(map[string]int)
	for idx, name := range names {
		if _, ok := nameMap[name]; ok {
			return kkerrors.ErrPktHeadPartNameRepeated
		}
		nameMap[name] = idx
	}
	h.nameMap = nameMap
	return nil
}

func (h *PacketHead) GetSize() int {
	return h.size
}

func (h *PacketHead) GetPartCount() int {
	return len(h.partList)
}

func (h *PacketHead) Marshal(headBytes []byte, endian binary.ByteOrder, valueList ...int) error {
	if len(headBytes) < h.size {
		return kkerrors.ErrPktDataTooShortToMarshal
	}
	if len(valueList) < len(h.partList) {
		return kkerrors.ErrPktValueListTooShortToMarshal
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
		return nil, kkerrors.ErrPktDataTooShortToUnmarshal
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
		return valueList[:0], kkerrors.ErrPktDataTooShortToUnmarshal
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

func (h *PacketHead) ReadValueByName(headBytes []byte, endian binary.ByteOrder, name string) (int, error) {
	idx, ok := h.nameMap[name]
	if !ok {
		return 0, kkerrors.ErrPktHeadPartNameNotFound
	}
	part := h.partList[idx]
	offset := 0
	for i := 0; i < idx; i++ {
		offset += h.partList[i].GetSize()
	}
	return part.Unmarshal(headBytes[offset:offset+part.GetSize()], endian)
}
