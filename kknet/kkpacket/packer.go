package kkpacket

import (
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
)

type packer struct {
	headType       uint8
	codecType      uint8
	isLittleEndian bool
}

type packerKey int

var (
	packerCache = make(map[packerKey]*packer)
)

func init() {
	//预热packer缓存，消除mutex争抢
	for i := HeadTypeMid; i <= HeadTypeMidSeq; i++ {
		for j := kkcodec.CodecTypeJson; j <= kkcodec.CodecTypeToml; j++ {
			initPacker(i, j, false)
			initPacker(i, j, true)
		}
	}
}

func initPacker(headType uint8, codecType uint8, isLittleEndian bool) {
	littleEndian := uint8(0)
	if isLittleEndian {
		littleEndian = uint8(1)
	}
	// key layout: [headType:8 bits][codecType:8 bits][littleEndian:1 bit]
	// headType: bits 16-23 (8 bits, 0-255)
	// codecType: bits 8-15 (8 bits, 0-255)
	// littleEndian: bit 0 (1 bit, 0-1)
	key := packerKey(headType)<<16 | packerKey(codecType)<<8 | packerKey(littleEndian)
	p := &packer{
		headType:       headType,
		codecType:      codecType,
		isLittleEndian: isLittleEndian,
	}
	packerCache[key] = p
}

func NewPacker(headType uint8, codecType uint8, isLittleEndian bool) *packer {
	littleEndian := uint8(0)
	if isLittleEndian {
		littleEndian = uint8(1)
	}
	// key layout: [headType:8 bits][codecType:8 bits][littleEndian:1 bit]
	// headType: bits 16-23 (8 bits, 0-255)
	// codecType: bits 8-15 (8 bits, 0-255)
	// littleEndian: bit 0 (1 bit, 0-1)
	key := packerKey(headType)<<16 | packerKey(codecType)<<8 | packerKey(littleEndian)
	if packer, ok := packerCache[key]; ok {
		return packer
	}
	kklog.Panicf("packer not found: headType=%d, codecType=%d, isLittleEndian=%t", headType, codecType, isLittleEndian)
	return nil
}
