package kkbuffer

import "testing"

func TestIndex(t *testing.T) {
	cases := []struct {
		n    int
		want int
	}{
		{0, 0},
		{-1, 0},
		{1, 0},                  // still in first bucket (64 bytes)
		{minItemSize - 1, 0},    // 1..63 -> idx 0
		{minItemSize, 0},        // 64 -> idx 0
		{minItemSize + 1, 1},    // 65 -> next bucket
		{minItemSize * 2, 1},    // 128 -> idx 1
		{minItemSize*2 + 1, 2},  // 129 -> idx 2
		{maxItemSize, steps - 1}, // largest bucket
	}

	for _, tc := range cases {
		if got := index(tc.n); got != tc.want {
			t.Errorf("index(%d) = %d, want %d", tc.n, got, tc.want)
		}
	}
}

func TestIndexBS(t *testing.T) {
	cases := []struct {
		n    uint32
		want uint32
	}{
		{1, 0},
		{2, 1},
		{3, 2},
		{4, 2},
		{5, 3},
		{8, 3},
		{9, 4},
		{16, 4},
		{17, 5},
	}

	for _, tc := range cases {
		if got := indexBS(tc.n); got != tc.want {
			t.Errorf("indexBS(%d) = %d, want %d", tc.n, got, tc.want)
		}
	}
}

func TestBinaryCeil(t *testing.T) {
	cases := []struct {
		in   uint32
		want uint32
	}{
		{1, 1},
		{2, 2},
		{3, 4},
		{4, 4},
		{5, 8},
		{7, 8},
		{8, 8},
		{9, 16},
		{15, 16},
		{16, 16},
		{17, 32},
		{255, 256},
		{256, 256},
		{257, 512},
		{1024, 1024},
		{1025, 2048},
	}

	for _, tc := range cases {
		if got := binaryCeil(tc.in); got != tc.want {
			t.Errorf("binaryCeil(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestMinMax(t *testing.T) {
	if got := Min(1, 2); got != 1 {
		t.Errorf("Min(1,2) = %d, want 1", got)
	}
	if got := Min(2, 1); got != 1 {
		t.Errorf("Min(2,1) = %d, want 1", got)
	}
	if got := Min(-1, 0); got != -1 {
		t.Errorf("Min(-1,0) = %d, want -1", got)
	}

	if got := Max(1, 2); got != 2 {
		t.Errorf("Max(1,2) = %d, want 2", got)
	}
	if got := Max(2, 1); got != 2 {
		t.Errorf("Max(2,1) = %d, want 2", got)
	}
	if got := Max(-1, 0); got != 0 {
		t.Errorf("Max(-1,0) = %d, want 0", got)
	}
}

