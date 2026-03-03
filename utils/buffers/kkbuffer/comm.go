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
