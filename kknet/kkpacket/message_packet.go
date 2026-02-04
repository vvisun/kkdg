package kkpacket

import (
	"encoding/binary"

	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
)

type PacketCodec struct {
	headType  uint8
	codecType uint8
}

type packerKey int

var (
	packerCache = make(map[packerKey]*PacketCodec)
)

func init() {
	//预热packer缓存，消除mutex争抢
	for i := HeadTypeMid; i <= HeadTypeMidSeq; i++ {
		for j := kkcodec.CodecTypeJson; j <= kkcodec.CodecTypeToml; j++ {
			initPacker(i, j)
		}
	}

	defaultStreamPacket = NewLengthFieldStreamPacket(NewPacker(HeadTypeMid, kkcodec.CodecTypeProtoBuf))
}

// key layout: [headType:8 bits][codecType:8 bits]
// headType: bits 16-23 (8 bits, 0-255)
// codecType: bits 8-15 (8 bits, 0-255)
func getKey(headType uint8, codecType uint8) packerKey {
	return packerKey(headType)<<16 | packerKey(codecType)<<8
}

func initPacker(headType uint8, codecType uint8) {
	key := getKey(headType, codecType)
	p := &PacketCodec{
		headType:  headType,
		codecType: codecType,
	}
	packerCache[key] = p
}

func NewPacker(headType uint8, codecType uint8) *PacketCodec {
	key := getKey(headType, codecType)
	if packer, ok := packerCache[key]; ok {
		return packer
	}
	kklog.Panicf("packer not found: headType=%d, codecType=%d", headType, codecType)
	return nil
}

var gByteOrder binary.ByteOrder = binary.BigEndian

func SetByteOrder(order binary.ByteOrder) {
	gByteOrder = order
}

func GetByteOrder() binary.ByteOrder {
	return gByteOrder
}
