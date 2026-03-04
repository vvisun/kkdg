package kkbuffer

import "math/bits"

func index(n int) int {
	if n <= 0 {
		return 0
	}
	k := (n - 1) >> minBitSize
	idx := bits.Len(uint(k))
	if idx >= steps {
		return steps - 1
	}
	return idx
}

// indexBS 将给定的 uint32 值转换为 2 的幂的索引(向上取整)
// 2^0=1, 2^1=2, 2^2=4, 2^3=8, 2^4=16, 2^5=32, 2^6=64, 2^7=128, 2^8=256, 2^9=512, 2^10=1024, ...
func indexBS(n uint32) uint32 {
	return uint32(bits.Len32(n - 1))
}

// binaryCeil 将给定的 uint32 值向上取整到最近的 2 的幂
// rounds up the given uint32 value to the nearest power of 2
func binaryCeil(v uint32) uint32 {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v++
	return v
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
