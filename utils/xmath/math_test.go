package xmath_test

import (
	"math"
	"testing"

	"github.com/vvisun/kkdg/utils/xmath"
)

func TestIsPowerOfTwo(t *testing.T) {
	for _, n := range []int{1, 2, 4, 8, 16, 32, 64, 1024, 1 << 20} {
		if !xmath.IsPowerOfTwo(n) {
			t.Errorf("IsPowerOfTwo(%d) = false, want true", n)
		}
	}
	for _, n := range []int{0, -1, 3, 5, 6, 7, 9, 10} {
		if xmath.IsPowerOfTwo(n) {
			t.Errorf("IsPowerOfTwo(%d) = true, want false", n)
		}
	}
}

func TestFloor(t *testing.T) {
	f := math.Pi
	if got := xmath.Floor(f); got != 3 {
		t.Errorf("Floor(Pi) = %v, want 3", got)
	}
	if got := xmath.Floor(f, 2); got != 3.14 {
		t.Errorf("Floor(Pi, 2) = %v, want 3.14", got)
	}
	if got := xmath.Floor(3.14159, 3); got != 3.141 {
		t.Errorf("Floor(3.14159, 3) = %v, want 3.141", got)
	}
	if got := xmath.Floor(-1.6); got != -2 {
		t.Errorf("Floor(-1.6) = %v, want -2", got)
	}
}

func TestCeil(t *testing.T) {
	f := math.Pi
	if got := xmath.Ceil(f); got != 4 {
		t.Errorf("Ceil(Pi) = %v, want 4", got)
	}
	if got := xmath.Ceil(f, 2); got != 3.15 {
		t.Errorf("Ceil(Pi, 2) = %v, want 3.15", got)
	}
	if got := xmath.Ceil(3.14159, 3); got != 3.142 {
		t.Errorf("Ceil(3.14159, 3) = %v, want 3.142", got)
	}
	if got := xmath.Ceil(-1.2); got != -1 {
		t.Errorf("Ceil(-1.2) = %v, want -1", got)
	}
}

func TestRound(t *testing.T) {
	f := math.Pi
	if got := xmath.Round(f); got != 3 {
		t.Errorf("Round(Pi) = %v, want 3", got)
	}
	if got := xmath.Round(f, 2); got != 3.14 {
		t.Errorf("Round(Pi, 2) = %v, want 3.14", got)
	}
	if got := xmath.Round(3.145, 2); got != 3.15 {
		t.Errorf("Round(3.145, 2) = %v, want 3.15", got)
	}
	if got := xmath.Round(-1.5); got != -2 {
		t.Errorf("Round(-1.5) = %v, want -2", got)
	}
}

func TestFloorToPowerOfTwo(t *testing.T) {
	tests := []struct {
		n    int
		want int
	}{
		{0, 0}, {1, 1}, {2, 2},
		{3, 2}, {4, 4}, {5, 4}, {7, 4}, {8, 8},
		{9, 8}, {15, 8}, {16, 16},
		{17, 16}, {31, 16}, {33, 32}, {63, 32}, {65, 64},
		{100, 64}, {128, 128}, {129, 128}, {255, 128}, {256, 256},
		{1000, 512}, {1024, 1024}, {1025, 1024},
	}
	for _, tt := range tests {
		if got := xmath.FloorToPowerOfTwo(tt.n); got != tt.want {
			t.Errorf("FloorToPowerOfTwo(%d) = %d, want %d", tt.n, got, tt.want)
		}
	}
}

func TestClosestPowerOfTwo(t *testing.T) {
	tests := []struct {
		n    int
		want int
	}{
		{0, 1}, {1, 1}, {2, 2},
		{3, 4},   // 3 is closer to 4 than to 2 (dist 1 vs 1, ceil wins or implementation detail)
		{4, 4},
		{5, 4},   // 5: dist to 4=1, to 8=3, so 4
		{6, 8},   // 6: dist to 4=2, to 8=2, implementation may pick one
		{7, 8},   // 7: dist to 4=3, to 8=1, so 8
		{8, 8},
		{9, 8},   // 9: dist to 8=1, to 16=7, so 8
		{10, 8},  // 10: dist to 8=2, to 16=6, so 8
		{11, 8},  // 11: dist to 8=3, to 16=5, so 8
		{12, 16}, // 12: dist to 8=4, to 16=4, ceil may win
		{15, 16},
		{16, 16},
	}
	for _, tt := range tests {
		if got := xmath.ClosestPowerOfTwo(tt.n); got != tt.want {
			t.Errorf("ClosestPowerOfTwo(%d) = %d, want %d", tt.n, got, tt.want)
		}
	}
}

func TestCeilToPowerOfTwo(t *testing.T) {
	type args struct {
		n int
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{name: "zero", args: args{n: 0}, want: 2},
		{name: "one", args: args{n: 1}, want: 2},
		{name: "two", args: args{n: 2}, want: 2},
		{name: "three", args: args{n: 3}, want: 4},
		{name: "four", args: args{n: 4}, want: 4},
		{name: "five", args: args{n: 5}, want: 8},
		{name: "eight", args: args{n: 8}, want: 8},
		{name: "nine", args: args{n: 9}, want: 16},
		{name: "power_16", args: args{n: 16}, want: 16},
		{name: "power_1024", args: args{n: 1024}, want: 1024},
		{name: "near_1024", args: args{n: 1025}, want: 2048},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := xmath.CeilToPowerOfTwo(tt.args.n); got != tt.want {
				t.Errorf("CeilToPowerOfTwo() = %v, want %v", got, tt.want)
			}
		})
	}
}
