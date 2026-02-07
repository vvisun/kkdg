package kkpacket

import "encoding/binary"

type HeadType uint8

const (
	HeadTypeMid HeadType = iota
	HeadTypeMidSeq
)

type IHead interface {
	Marshal(data []byte, endian binary.ByteOrder) error
	Unmarshal(data []byte, endian binary.ByteOrder) error
	GetSize() int
	GetType() HeadType
}

func GetHeadSize(headType HeadType) int {
	switch headType {
	case HeadTypeMid:
		return 4
	case HeadTypeMidSeq:
		return 8
	default:
		return -1
	}
}

type HeadMid struct {
	mid uint32
}

var _ IHead = (*HeadMid)(nil)

func (h *HeadMid) Marshal(data []byte, endian binary.ByteOrder) error {
	endian.PutUint32(data[:4], h.mid)
	return nil
}

func (h *HeadMid) Unmarshal(data []byte, endian binary.ByteOrder) error {
	h.mid = endian.Uint32(data[:4])
	return nil
}

func (h *HeadMid) GetSize() int {
	return 4
}

func (h *HeadMid) GetType() HeadType {
	return HeadTypeMid
}

type HeadMidSeq struct {
	mid uint32
	seq uint32
}

var _ IHead = (*HeadMidSeq)(nil)

func (h *HeadMidSeq) Marshal(data []byte, endian binary.ByteOrder) error {
	endian.PutUint32(data[:4], h.mid)
	endian.PutUint32(data[4:8], h.seq)
	return nil
}

func (h *HeadMidSeq) Unmarshal(data []byte, endian binary.ByteOrder) error {
	h.mid = endian.Uint32(data[:4])
	h.seq = endian.Uint32(data[4:8])
	return nil
}

func (h *HeadMidSeq) GetSize() int {
	return 8
}

func (h *HeadMidSeq) GetType() HeadType {
	return HeadTypeMidSeq
}
