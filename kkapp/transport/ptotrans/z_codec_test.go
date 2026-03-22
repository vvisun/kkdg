package ptotrans

import (
	"bytes"
	"testing"

	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

func TestStructInfo_Roundtrip_AllFieldKinds(t *testing.T) {
	var si structInfo
	si.AddField(dataTypeUint16, "u16")
	si.AddField(dataTypeUint32, "u32")
	si.AddField(dataTypeUint64, "u64")
	si.AddField(dataTypeUint8, "u8")
	si.AddField(dataTypeBool, "flag")
	si.AddField(dataTypeString, "s")
	si.AddField(dataTypeBytes, "b")
	si.AddField(dataTypeStringList, "sl")
	si.AddField(dataTypeBytesList, "bl")

	values := []any{
		uint16(0x1234),
		uint32(0x89abcdef),
		uint64(0x1122334455667788),
		uint8(0xfe),
		true,
		"hello 世界",
		[]byte{1, 2, 3, 0xff},
		[]string{"", "a", "bc"},
		[][]byte{nil, {}, {9, 9}},
	}

	bb, err := si.Marshal(values, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer kkbuffer.Put(bb)

	got, err := si.Unmarshal(bb.B)
	if err != nil {
		t.Fatal(err)
	}
	assertValuesEqual(t, values, got)
}

func TestStructInfo_Roundtrip_WithOffset(t *testing.T) {
	var si structInfo
	si.AddField(dataTypeString, "s")
	offset := 16
	wantPrefix := make([]byte, offset)

	bb, err := si.Marshal([]any{"payload"}, offset)
	if err != nil {
		t.Fatal(err)
	}
	defer kkbuffer.Put(bb)

	if len(bb.B) < offset || !bytes.Equal(bb.B[:offset], wantPrefix) {
		t.Fatalf("prefix: got %d bytes prefix %x want %d zero bytes", len(bb.B), bb.B[:min(len(bb.B), offset)], offset)
	}

	got, err := si.Unmarshal(bb.B[offset:])
	if err != nil {
		t.Fatal(err)
	}
	assertValuesEqual(t, []any{"payload"}, got)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func TestStructInfo_Marshal_OffsetNegative(t *testing.T) {
	var si structInfo
	si.AddField(dataTypeUint16, "x")
	_, err := si.Marshal([]any{uint16(1)}, -1)
	if err == nil {
		t.Fatal("expected error for negative offset")
	}
}

func TestStructInfo_Marshal_ValueListLengthMismatch(t *testing.T) {
	var si structInfo
	si.AddField(dataTypeUint16, "a")
	si.AddField(dataTypeUint16, "b")
	_, err := si.Marshal([]any{uint16(1)}, 0)
	if err == nil {
		t.Fatal("expected length mismatch error")
	}
}

func TestStructInfo_Marshal_WrongTypeError(t *testing.T) {
	var si structInfo
	si.AddField(dataTypeUint16, "a")
	_, err := si.Marshal([]any{"not-a-uint16"}, 0)
	if err == nil {
		t.Fatal("expected type error")
	}
}

func TestStructInfo_Marshal_BoolWrongTypeError(t *testing.T) {
	var si structInfo
	si.AddField(dataTypeBool, "b")
	_, err := si.Marshal([]any{uint8(1)}, 0)
	if err == nil {
		t.Fatal("expected type error for bool field")
	}
}

func TestStructInfo_Marshal_Uint8WrongTypeError(t *testing.T) {
	var si structInfo
	si.AddField(dataTypeUint8, "u8")
	_, err := si.Marshal([]any{uint16(1)}, 0)
	if err == nil {
		t.Fatal("expected type error for uint8 field")
	}
}

func TestStructInfo_Roundtrip_Uint8(t *testing.T) {
	var si structInfo
	si.AddField(dataTypeUint8, "x")
	bb, err := si.Marshal([]any{uint8(0)}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer kkbuffer.Put(bb)
	got, err := si.Unmarshal(bb.B)
	if err != nil {
		t.Fatal(err)
	}
	assertValuesEqual(t, []any{uint8(0)}, got)
}

func TestStructInfo_Unmarshal_Uint8Truncated(t *testing.T) {
	var si structInfo
	si.AddField(dataTypeUint8, "u8")
	_, err := si.Unmarshal(nil)
	if err == nil {
		t.Fatal("expected truncated error for uint8")
	}
}

func TestStructInfo_Roundtrip_BoolFalse(t *testing.T) {
	var si structInfo
	si.AddField(dataTypeBool, "f")
	bb, err := si.Marshal([]any{false}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer kkbuffer.Put(bb)
	got, err := si.Unmarshal(bb.B)
	if err != nil {
		t.Fatal(err)
	}
	assertValuesEqual(t, []any{false}, got)
}

// 解码端：非 0 字节均视为 true（与 unmarshal 中 != 0 一致）。
func TestStructInfo_Unmarshal_BoolNonZeroAsTrue(t *testing.T) {
	var si structInfo
	si.AddField(dataTypeBool, "b")
	got, err := si.Unmarshal([]byte{7})
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := got[0].(bool); !ok || !v {
		t.Fatalf("got %v (%T) want true", got[0], got[0])
	}
}

func TestStructInfo_Unmarshal_Truncated(t *testing.T) {
	var si structInfo
	si.AddField(dataTypeUint32, "u32")
	_, err := si.Unmarshal([]byte{1, 2}) // need 4 bytes
	if err == nil {
		t.Fatal("expected truncated error")
	}
}

func TestStructInfo_Unmarshal_BoolTruncated(t *testing.T) {
	var si structInfo
	si.AddField(dataTypeBool, "b")
	_, err := si.Unmarshal(nil)
	if err == nil {
		t.Fatal("expected truncated error for bool")
	}
}

func assertValuesEqual(t *testing.T, want, got []any) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("len %d != %d", len(got), len(want))
	}
	for i := range want {
		switch w := want[i].(type) {
		case uint16:
			g, ok := got[i].(int16)
			if !ok || int16(w) != g {
				t.Fatalf("[%d] uint16: got %v (%T) want %v", i, got[i], got[i], w)
			}
		case uint32:
			g, ok := got[i].(int32)
			if !ok || int32(w) != g {
				t.Fatalf("[%d] uint32: got %v (%T) want %v", i, got[i], got[i], w)
			}
		case uint64:
			g, ok := got[i].(int64)
			if !ok || int64(w) != g {
				t.Fatalf("[%d] uint64: got %v (%T) want %v", i, got[i], got[i], w)
			}
		case uint8:
			g, ok := got[i].(uint8)
			if !ok || g != w {
				t.Fatalf("[%d] uint8: got %v (%T) want %v", i, got[i], got[i], w)
			}
		case bool:
			g, ok := got[i].(bool)
			if !ok || g != w {
				t.Fatalf("[%d] bool: got %v (%T) want %v", i, got[i], got[i], w)
			}
		case string:
			g, ok := got[i].(string)
			if !ok || g != w {
				t.Fatalf("[%d] string: got %q want %q", i, got[i], w)
			}
		case []byte:
			g, ok := got[i].([]byte)
			if !ok || !bytes.Equal(w, g) {
				t.Fatalf("[%d] []byte: got %v want %v", i, got[i], w)
			}
		case []string:
			g, ok := got[i].([]string)
			if !ok || len(g) != len(w) {
				t.Fatalf("[%d] []string: got %v want %v", i, got[i], w)
			}
			for j := range w {
				if g[j] != w[j] {
					t.Fatalf("[%d][%d] string elem %q != %q", i, j, g[j], w[j])
				}
			}
		case [][]byte:
			g, ok := got[i].([][]byte)
			if !ok || len(g) != len(w) {
				t.Fatalf("[%d] [][]byte: got %v want %v", i, got[i], w)
			}
			for j := range w {
				if !bytes.Equal(w[j], g[j]) {
					t.Fatalf("[%d][%d] bytes %v != %v", i, j, g[j], w[j])
				}
			}
		default:
			t.Fatalf("[%d] unsupported want type %T", i, w)
		}
	}
}
