package xconv_test

import (
	"math"
	"math/cmplx"
	"testing"
	"time"

	"github.com/vvisun/kkdg/utils/xconv"
)

func TestInt(t *testing.T) {
	if got := xconv.Int(42); got != 42 {
		t.Errorf("Int(42) = %d, want 42", got)
	}
	if got := xconv.Int("123"); got != 123 {
		t.Errorf("Int(123) = %d, want 123", got)
	}
	if got := xconv.Int(1.9); got != 1 {
		t.Errorf("Int(1.9) = %d, want 1", got)
	}
	if got := xconv.Int(nil); got != 0 {
		t.Errorf("Int(nil) = %d, want 0", got)
	}
}

func TestInt64(t *testing.T) {
	if got := xconv.Int64(int64(9876543210)); got != 9876543210 {
		t.Errorf("Int64(9876543210) = %d, want 9876543210", got)
	}
	a := cmplx.Exp(1i*math.Pi) + 20
	_ = xconv.Int64(&a) // 不 panic 即可
}

func TestUint64(t *testing.T) {
	if got := xconv.Uint64(uint64(100)); got != 100 {
		t.Errorf("Uint64(100) = %d, want 100", got)
	}
	if got := xconv.Uint64("999"); got != 999 {
		t.Errorf("Uint64(999) = %d, want 999", got)
	}
}

func TestFloat64(t *testing.T) {
	if got := xconv.Float64(3.14); got != 3.14 {
		t.Errorf("Float64(3.14) = %v, want 3.14", got)
	}
	if got := xconv.Float64("1.5"); got != 1.5 {
		t.Errorf("Float64(1.5) = %v, want 1.5", got)
	}
}

func TestString(t *testing.T) {
	if got := xconv.String(42); got != "42" {
		t.Errorf("String(42) = %q, want 42", got)
	}
	if got := xconv.String(1.1); got != "1.1" {
		t.Errorf("String(1.1) = %q, want 1.1", got)
	}
	a := int64(1)
	if got := xconv.String(&a); got != "1" {
		t.Errorf("String(&int64(1)) = %q, want 1", got)
	}
}

func TestBool(t *testing.T) {
	if !xconv.Bool(true) {
		t.Error("Bool(true) = false, want true")
	}
	if xconv.Bool(false) {
		t.Error("Bool(false) = true, want false")
	}
	if !xconv.Bool(1) {
		t.Error("Bool(1) = false, want true")
	}
	if xconv.Bool(0) {
		t.Error("Bool(0) = true, want false")
	}
	a := float32(0)
	if xconv.Bool(&a) {
		t.Error("Bool(&0) = true, want false")
	}
	if xconv.Bool(nil) {
		t.Error("Bool(nil) = true, want false")
	}
}

func TestDuration(t *testing.T) {
	d := xconv.Duration("1h")
	if d != time.Hour {
		t.Errorf("Duration(1h) = %v, want 1h", d)
	}
	d2 := xconv.Duration("3d5m4h")
	if d2 <= 0 {
		t.Errorf("Duration(3d5m4h) = %v, want positive", d2)
	}
	if got := xconv.Duration(int64(1000)); got != 1000 {
		t.Errorf("Duration(1000) = %v, want 1000", got)
	}
}

func TestBytes(t *testing.T) {
	if got := xconv.Bytes("hello"); string(got) != "hello" {
		t.Errorf("Bytes(hello) = %q, want hello", got)
	}
	if got := xconv.Int(xconv.String(xconv.Bytes("123"))); got != 123 {
		t.Errorf("roundtrip Bytes->String->Int = %d, want 123", got)
	}
	if got := xconv.Bytes(uint8(255)); len(got) != 1 || got[0] != 255 {
		t.Errorf("Bytes(uint8(255)) = %v, want [255]", got)
	}
}

func TestStrings(t *testing.T) {
	got := xconv.Strings([]int64{1, 2, 3, 4})
	if len(got) != 4 || got[0] != "1" || got[3] != "4" {
		t.Errorf("Strings([1,2,3,4]) = %v", got)
	}
}

func TestAnys(t *testing.T) {
	got := xconv.Anys([]int64{1, 2, 3, 4})
	if len(got) != 4 {
		t.Errorf("Anys len = %d, want 4", len(got))
	}
	if xconv.Anys(nil) != nil {
		t.Error("Anys(nil) = non-nil, want nil")
	}
}

func TestJson(t *testing.T) {
	if got := xconv.Json("{}"); got != "{}" {
		t.Errorf("Json({}) = %q, want {}", got)
	}
	if got := xconv.Json(map[string]any{"id": 1, "name": "fuxiao"}); got == "" || got == "{}" {
		t.Errorf("Json(map) = %q", got)
	}
}

func TestInts(t *testing.T) {
	got := xconv.Ints([]int64{1, 2, 3})
	if len(got) != 3 || got[0] != 1 || got[2] != 3 {
		t.Errorf("Ints([1,2,3]) = %v", got)
	}
}

func TestFloat64s(t *testing.T) {
	got := xconv.Float64s([]float64{1.1, 2.2})
	if len(got) != 2 || math.Abs(got[0]-1.1) > 1e-9 || math.Abs(got[1]-2.2) > 1e-9 {
		t.Errorf("Float64s([1.1,2.2]) = %v", got)
	}
}

func TestB(t *testing.T) {
	if got := xconv.B(1024); got != 1024 {
		t.Errorf("B(1024) = %v, want 1024", got)
	}
	if got := xconv.B("1K"); got != 1024 {
		t.Errorf("B(1K) = %v, want 1024", got)
	}
	if got := xconv.B("1M"); got != 1024*1024 {
		t.Errorf("B(1M) = %v, want 1048576", got)
	}
	if got := xconv.B("1G"); got != 1024*1024*1024 {
		t.Errorf("B(1G) = %v, want 1073741824", got)
	}
}

func TestRune(t *testing.T) {
	if got := xconv.Rune('A'); got != 'A' {
		t.Errorf("Rune('A') = %v, want 65", got)
	}
	if got := xconv.Rune(65); got != 'A' {
		t.Errorf("Rune(65) = %v, want 65", got)
	}
}

func TestRunes(t *testing.T) {
	got := xconv.Runes([]int32{'a', 'b', 'c'})
	if len(got) != 3 || got[0] != 'a' || got[2] != 'c' {
		t.Errorf("Runes([]int32) = %v", got)
	}
}

func TestByte(t *testing.T) {
	if got := xconv.Byte(255); got != 255 {
		t.Errorf("Byte(255) = %v, want 255", got)
	}
	if got := xconv.Byte("123"); got != 123 {
		t.Errorf("Byte(123) = %v, want 123", got)
	}
}

func TestDurations(t *testing.T) {
	got := xconv.Durations([]string{"1s", "2m"})
	if len(got) != 2 || got[0] != time.Second || got[1] != 2*time.Minute {
		t.Errorf("Durations([1s,2m]) = %v", got)
	}
}
