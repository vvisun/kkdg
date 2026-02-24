package xvalue

import (
	"testing"
	"time"
)

func TestNewValue(t *testing.T) {
	v := NewValue()
	if v.Value() != nil {
		t.Errorf("NewValue() = %v, want nil", v.Value())
	}

	v2 := NewValue(42)
	if v2.Value() != 42 {
		t.Errorf("NewValue(42) = %v, want 42", v2.Value())
	}

	v3 := NewValue("hello", "ignored")
	if v3.Value() != "hello" {
		t.Errorf("NewValue(hello, ignored) = %v, want hello", v3.Value())
	}
}

func TestValue_Int(t *testing.T) {
	tests := []struct {
		in   any
		want int
	}{
		{42, 42},
		{-1, -1},
		{int8(127), 127},
		{int64(999), 999},
		{uint(100), 100},
		{"123", 123},
		{1.9, 1},
		{true, 1},
		{false, 0},
	}
	for _, tt := range tests {
		v := NewValue(tt.in)
		if got := v.Int(); got != tt.want {
			t.Errorf("NewValue(%v).Int() = %d, want %d", tt.in, got, tt.want)
		}
	}
}

func TestValue_Int64(t *testing.T) {
	v := NewValue(int64(9876543210))
	if got := v.Int64(); got != 9876543210 {
		t.Errorf("Int64() = %d, want 9876543210", got)
	}
}

func TestValue_Uint64(t *testing.T) {
	v := NewValue(uint64(18446744073709551615))
	if got := v.Uint64(); got != 18446744073709551615 {
		t.Errorf("Uint64() = %d, want 18446744073709551615", got)
	}
}

func TestValue_Float64(t *testing.T) {
	v := NewValue(3.14)
	if got := v.Float64(); got != 3.14 {
		t.Errorf("Float64() = %v, want 3.14", got)
	}
}

func TestValue_Bool(t *testing.T) {
	for _, in := range []any{true, "true", 1, "1"} {
		v := NewValue(in)
		if !v.Bool() {
			t.Errorf("NewValue(%v).Bool() = false, want true", in)
		}
	}
	for _, in := range []any{false, "false", 0, ""} {
		v := NewValue(in)
		if v.Bool() {
			t.Errorf("NewValue(%v).Bool() = true, want false", in)
		}
	}
}

func TestValue_String(t *testing.T) {
	v := NewValue("hello")
	if got := v.String(); got != "hello" {
		t.Errorf("String() = %q, want hello", got)
	}
	v2 := NewValue(42)
	if got := v2.String(); got != "42" {
		t.Errorf("String() from 42 = %q, want 42", got)
	}
}

func TestValue_Duration(t *testing.T) {
	v := NewValue("1h30m")
	d := v.Duration()
	if d != 90*time.Minute {
		t.Errorf("Duration() = %v, want 1h30m", d)
	}
}

func TestValue_Ints(t *testing.T) {
	v := NewValue([]int{1, 2, 3})
	got := v.Ints()
	if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Errorf("Ints() = %v, want [1,2,3]", got)
	}
}

func TestValue_Strings(t *testing.T) {
	v := NewValue([]string{"a", "b", "c"})
	got := v.Strings()
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Errorf("Strings() = %v, want [a,b,c]", got)
	}
}

func TestValue_Bytes(t *testing.T) {
	v := NewValue([]byte("hello"))
	got := v.Bytes()
	if string(got) != "hello" {
		t.Errorf("Bytes() = %q, want hello", got)
	}
}

func TestValue_Slice(t *testing.T) {
	v := NewValue([]int{1, 2, 3})
	got := v.Slice()
	if len(got) != 3 {
		t.Errorf("Slice() len = %d, want 3", len(got))
	}
}

func TestValue_Map(t *testing.T) {
	v := NewValue(map[string]any{"a": 1, "b": "x"})
	got := v.Map()
	if got == nil {
		t.Fatal("Map() = nil")
	}
	if got["a"] != float64(1) && got["a"] != 1 {
		t.Errorf("Map()[a] = %v", got["a"])
	}
	if got["b"] != "x" {
		t.Errorf("Map()[b] = %v, want x", got["b"])
	}
}

func TestValue_Scan(t *testing.T) {
	var i int
	v := NewValue(42)
	if err := v.Scan(&i); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if i != 42 {
		t.Errorf("Scan int = %d, want 42", i)
	}

	var s string
	v2 := NewValue("world")
	if err := v2.Scan(&s); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if s != "world" {
		t.Errorf("Scan string = %q, want world", s)
	}

	var b bool
	v3 := NewValue(true)
	if err := v3.Scan(&b); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !b {
		t.Errorf("Scan bool = false, want true")
	}
}

func TestValue_Kind(t *testing.T) {
	v := NewValue(42)
	if k := v.Kind(); k.String() != "int" {
		t.Errorf("Kind() = %v, want int", k)
	}
	v2 := NewValue("x")
	if k := v2.Kind(); k.String() != "string" {
		t.Errorf("Kind() = %v, want string", k)
	}
}

func TestValue_IsBool(t *testing.T) {
	if !NewValue(true).IsBool() {
		t.Error("IsBool() on true = false, want true")
	}
	if NewValue(42).IsBool() {
		t.Error("IsBool() on 42 = true, want false")
	}
}

func TestValue_IsString(t *testing.T) {
	if !NewValue("x").IsString() {
		t.Error("IsString() on string = false, want true")
	}
	if NewValue(42).IsString() {
		t.Error("IsString() on int = true, want false")
	}
}

func TestValue_IsNumber(t *testing.T) {
	for _, in := range []any{1, int64(1), uint(1), 1.0} {
		if !NewValue(in).IsNumber() {
			t.Errorf("IsNumber() on %v = false, want true", in)
		}
	}
	if NewValue("1").IsNumber() {
		t.Error("IsNumber() on string = true, want false")
	}
}

func TestValue_IsMap(t *testing.T) {
	if !NewValue(map[string]int{}).IsMap() {
		t.Error("IsMap() on map = false, want true")
	}
	if NewValue([]int{}).IsMap() {
		t.Error("IsMap() on slice = true, want false")
	}
}

func TestValue_IsSlice(t *testing.T) {
	if !NewValue([]int{}).IsSlice() {
		t.Error("IsSlice() on slice = false, want true")
	}
	if NewValue(map[string]int{}).IsSlice() {
		t.Error("IsSlice() on map = true, want false")
	}
}

func TestValue_Nil(t *testing.T) {
	v := NewValue(nil)
	if v.Int() != 0 {
		t.Errorf("nil Int() = %d, want 0", v.Int())
	}
	if v.String() != "" {
		t.Errorf("nil String() = %q, want empty", v.String())
	}
	if v.Bool() {
		t.Error("nil Bool() = true, want false")
	}
}
