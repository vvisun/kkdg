package xmath

import (
	"math"
	"math/bits"
)

const (
	bitSize       = 32 << (^uint(0) >> 63)
	maxintHeadBit = 1 << (bitSize - 2)
)

// IsPowerOfTwo reports whether the given n is a power of two.
func IsPowerOfTwo(n int) bool {
	return n > 0 && n&(n-1) == 0
}

// CeilToPowerOfTwo returns n if it is a power-of-two, otherwise the next-highest power-of-two.
func CeilToPowerOfTwo(n int) int {
	if n&maxintHeadBit != 0 && n > maxintHeadBit {
		panic("argument is too large")
	}

	if n <= 2 {
		return 2
	}
	return 1 << bits.Len(uint(n-1))
}

// FloorToPowerOfTwo returns n if it is a power-of-two, otherwise the next-highest power-of-two.
func FloorToPowerOfTwo(n int) int {
	if n <= 2 {
		return n
	}

	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16

	return n - (n >> 1)
}

// ClosestPowerOfTwo returns n if it is a power-of-two, otherwise the closest power-of-two.
func ClosestPowerOfTwo(n int) int {
	next := CeilToPowerOfTwo(n)
	if prev := next / 2; (n - prev) < (next - n) {
		next = prev
	}
	return next
}

// Floor 舍去取整保留n位小数
func Floor(f float64, n ...int) float64 {
	s := float64(1)

	if len(n) > 0 {
		s = math.Pow10(n[0])
	}

	return math.Floor(f*s) / s
}

// Ceil 进一取整保留n位小数
func Ceil(f float64, n ...int) float64 {
	s := float64(1)

	if len(n) > 0 {
		s = math.Pow10(n[0])
	}

	return math.Ceil(f*s) / s
}

// Round 四舍五入保留n位小数
func Round(f float64, n ...int) float64 {
	s := float64(1)

	if len(n) > 0 {
		s = math.Pow10(n[0])
	}

	return math.Round(f*s) / s
}
