package xstring_test

import (
	"testing"

	"github.com/vvisun/kkdg/utils/xstring"
)

func TestBytesToString(t *testing.T) {
	b := []byte("hello")
	s := xstring.BytesToString(b)
	if s != "hello" {
		t.Errorf("BytesToString(%q) = %q, want hello", b, s)
	}
	if len(s) != len(b) {
		t.Errorf("BytesToString len = %d, want %d", len(s), len(b))
	}

	empty := []byte{}
	if got := xstring.BytesToString(empty); got != "" {
		t.Errorf("BytesToString([]) = %q, want empty", got)
	}
}

func TestStringToBytes(t *testing.T) {
	s := "world"
	b := xstring.StringToBytes(s)
	if string(b) != "world" {
		t.Errorf("StringToBytes(%q) = %q, want world", s, b)
	}
	if len(b) != len(s) {
		t.Errorf("StringToBytes len = %d, want %d", len(b), len(s))
	}

	if got := xstring.StringToBytes(""); len(got) != 0 {
		t.Errorf("StringToBytes(\"\") len = %d, want 0", len(got))
	}
}

func TestFirstCharacterIsUpper(t *testing.T) {
	if !xstring.FirstCharacterIsUpper("Hello") {
		t.Error("FirstCharacterIsUpper(Hello) = false, want true")
	}
	if !xstring.FirstCharacterIsUpper("A") {
		t.Error("FirstCharacterIsUpper(A) = false, want true")
	}
	if xstring.FirstCharacterIsUpper("hello") {
		t.Error("FirstCharacterIsUpper(hello) = true, want false")
	}
	if xstring.FirstCharacterIsUpper("") {
		t.Error("FirstCharacterIsUpper(\"\") = true, want false")
	}
	if xstring.FirstCharacterIsUpper("123") {
		t.Error("FirstCharacterIsUpper(123) = true, want false")
	}
}

func TestFirstCharacterIsLower(t *testing.T) {
	if !xstring.FirstCharacterIsLower("hello") {
		t.Error("FirstCharacterIsLower(hello) = false, want true")
	}
	if xstring.FirstCharacterIsLower("Hello") {
		t.Error("FirstCharacterIsLower(Hello) = true, want false")
	}
	if xstring.FirstCharacterIsLower("") {
		t.Error("FirstCharacterIsLower(\"\") = true, want false")
	}
}

func TestFirstCharacterIsNumber(t *testing.T) {
	if !xstring.FirstCharacterIsNumber("123") {
		t.Error("FirstCharacterIsNumber(123) = false, want true")
	}
	if !xstring.FirstCharacterIsNumber("0abc") {
		t.Error("FirstCharacterIsNumber(0abc) = false, want true")
	}
	if xstring.FirstCharacterIsNumber("abc") {
		t.Error("FirstCharacterIsNumber(abc) = true, want false")
	}
	if xstring.FirstCharacterIsNumber("") {
		t.Error("FirstCharacterIsNumber(\"\") = true, want false")
	}
}

func TestFirstCharacterIsSymbol(t *testing.T) {
	if !xstring.FirstCharacterIsSymbol("€100") {
		t.Error("FirstCharacterIsSymbol(€100) = false, want true")
	}
	if xstring.FirstCharacterIsSymbol("abc") {
		t.Error("FirstCharacterIsSymbol(abc) = true, want false")
	}
	if xstring.FirstCharacterIsSymbol("") {
		t.Error("FirstCharacterIsSymbol(\"\") = true, want false")
	}
}

func TestLength(t *testing.T) {
	if got := xstring.Length("hello"); got != 5 {
		t.Errorf("Length(hello) = %d, want 5", got)
	}
	if got := xstring.Length(""); got != 0 {
		t.Errorf("Length(\"\") = %d, want 0", got)
	}
	// 中文等宽字符：每字符 1 个 rune
	if got := xstring.Length("你好"); got != 2 {
		t.Errorf("Length(你好) = %d, want 2", got)
	}
	if got := xstring.Length("a€b"); got != 3 {
		t.Errorf("Length(a€b) = %d, want 3", got)
	}
}

func TestPaddingPrefix(t *testing.T) {
	if got := xstring.PaddingPrefix("1", "0", 3); got != "001" {
		t.Errorf("PaddingPrefix(1,0,3) = %q, want 001", got)
	}
	if got := xstring.PaddingPrefix("001", "0", 3); got != "001" {
		t.Errorf("PaddingPrefix(001,0,3) = %q, want 001", got)
	}
	if got := xstring.PaddingPrefix("0001", "0", 3); got != "0001" {
		t.Errorf("PaddingPrefix(0001,0,3) = %q, want 0001 (no shrink)", got)
	}
	if got := xstring.PaddingPrefix("1", "00", 3); got != "001" {
		t.Errorf("PaddingPrefix(1,00,3) = %q, want 001", got)
	}
	if got := xstring.PaddingPrefix("ab", "0", 2); got != "ab" {
		t.Errorf("PaddingPrefix(ab,0,2) = %q, want ab", got)
	}
	if got := xstring.PaddingPrefix("abc", "0", 2); got != "abc" {
		t.Errorf("PaddingPrefix(abc,0,2) = %q, want abc", got)
	}
}

func TestPaddingSuffix(t *testing.T) {
	if got := xstring.PaddingSuffix("1", "0", 3); got != "100" {
		t.Errorf("PaddingSuffix(1,0,3) = %q, want 100", got)
	}
	if got := xstring.PaddingSuffix("100", "0", 3); got != "100" {
		t.Errorf("PaddingSuffix(100,0,3) = %q, want 100", got)
	}
	if got := xstring.PaddingSuffix("1", "00", 3); got != "100" {
		t.Errorf("PaddingSuffix(1,00,3) = %q, want 100", got)
	}
	if got := xstring.PaddingSuffix("ab", "0", 2); got != "ab" {
		t.Errorf("PaddingSuffix(ab,0,2) = %q, want ab", got)
	}
}

func TestReplace(t *testing.T) {
	if got := xstring.Replace("hello", 1, 3, "X"); got != "hXXXo" {
		t.Errorf("Replace(hello,1,3,X) = %q, want hXXXo", got)
	}
	if got := xstring.Replace("hello", 0, 2, "ab"); got != "ababllo" {
		t.Errorf("Replace(hello,0,2,ab) = %q, want ababllo", got)
	}
	if got := xstring.Replace("hello", 5, 1, "x"); got != "hello" {
		t.Errorf("Replace(hello,5,1,x) = %q, want hello (start>=len)", got)
	}
	if got := xstring.Replace("hello", 0, 10, "X"); got != "XXXXX" {
		t.Errorf("Replace(hello,0,10,X) = %q, want XXXXX", got)
	}
	if got := xstring.Replace("hello", 1, -1, "Y"); got != "hYYYY" {
		t.Errorf("Replace(hello,1,-1,Y) = %q, want hYYYY", got)
	}
	// 按 rune 计算
	if got := xstring.Replace("你好世界", 1, 2, "x"); got != "你xx界" {
		t.Errorf("Replace(你好世界,1,2,x) = %q, want 你xx界", got)
	}
}
