package kkpacket

import (
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
)

type packer struct {
	headType  uint8
	codecType uint8
}

type packerKey int

var (
	packerCache = make(map[packerKey]*packer)
)

func init() {
	//预热packer缓存，消除mutex争抢
	for i := HeadTypeMid; i <= HeadTypeMidSeq; i++ {
		for j := kkcodec.CodecTypeJson; j <= kkcodec.CodecTypeToml; j++ {
			initPacker(i, j)
		}
	}
}

func initPacker(headType uint8, codecType uint8) {
	// key layout: [headType:8 bits][codecType:8 bits]
	// headType: bits 16-23 (8 bits, 0-255)
	// codecType: bits 8-15 (8 bits, 0-255)
	key := packerKey(headType)<<16 | packerKey(codecType)<<8
	p := &packer{
		headType:  headType,
		codecType: codecType,
	}
	packerCache[key] = p
}

func NewPacker(headType uint8, codecType uint8) *packer {
	// key layout: [headType:8 bits][codecType:8 bits]
	// headType: bits 16-23 (8 bits, 0-255)
	// codecType: bits 8-15 (8 bits, 0-255)
	key := packerKey(headType)<<16 | packerKey(codecType)<<8
	if packer, ok := packerCache[key]; ok {
		return packer
	}
	kklog.Panicf("packer not found: headType=%d, codecType=%d", headType, codecType)
	return nil
}
