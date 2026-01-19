package xreflect_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/vvisun/kkdg/utils/xreflect"
)

func TestValue(t *testing.T) {
	// 测试基本类型
	var i int = 42
	kind, val := xreflect.Value(i)
	if kind != reflect.Int {
		t.Errorf("Expected Int, got %v", kind)
	}
	if val.Int() != 42 {
		t.Errorf("Expected 42, got %v", val.Int())
	}

	// 测试指针类型
	var pi *int = &i
	kind, val = xreflect.Value(pi)
	if kind != reflect.Int {
		t.Errorf("Expected Int, got %v", kind)
	}
	if val.Int() != 42 {
		t.Errorf("Expected 42, got %v", val.Int())
	}

	// 测试字符串
	s := "hello"
	kind, val = xreflect.Value(s)
	if kind != reflect.String {
		t.Errorf("Expected String, got %v", kind)
	}
	if val.String() != "hello" {
		t.Errorf("Expected 'hello', got %v", val.String())
	}

	// 测试双指针
	var ppi **int
	var p = &i
	ppi = &p
	kind, val = xreflect.Value(ppi)
	if kind != reflect.Int {
		t.Errorf("Expected Int, got %v", kind)
	}
	if val.Int() != 42 {
		t.Errorf("Expected 42, got %v", val.Int())
	}
}

func TestIsNil(t *testing.T) {
	// 测试 nil
	if !xreflect.IsNil(nil) {
		t.Error("Expected nil to be nil")
	}

	// 测试基本类型（非nil）
	var b1 bool
	if xreflect.IsNil(b1) {
		t.Error("Expected bool false to not be nil")
	}
	if xreflect.IsNil(&b1) {
		t.Error("Expected pointer to bool to not be nil")
	}

	// 测试 nil 指针
	var b2 *bool
	if !xreflect.IsNil(b2) {
		t.Error("Expected nil pointer to be nil")
	}
	if xreflect.IsNil(&b2) {
		t.Error("Expected pointer to nil pointer to not be nil")
	}

	// 测试非 nil 指针
	var b3 bool = true
	var pb3 *bool = &b3
	if xreflect.IsNil(pb3) {
		t.Error("Expected non-nil pointer to not be nil")
	}

	// 测试 slice
	var s1 []int
	if !xreflect.IsNil(s1) {
		t.Error("Expected nil slice to be nil")
	}
	s2 := []int{1, 2, 3}
	if xreflect.IsNil(s2) {
		t.Error("Expected non-nil slice to not be nil")
	}

	// 测试 map
	var m1 map[string]int
	if !xreflect.IsNil(m1) {
		t.Error("Expected nil map to be nil")
	}
	m2 := make(map[string]int)
	if xreflect.IsNil(m2) {
		t.Error("Expected non-nil map to not be nil")
	}

	// 测试 channel
	var ch1 chan int
	if !xreflect.IsNil(ch1) {
		t.Error("Expected nil channel to be nil")
	}
	ch2 := make(chan int)
	if xreflect.IsNil(ch2) {
		t.Error("Expected non-nil channel to not be nil")
	}

	// 测试 interface
	var i1 interface{}
	if !xreflect.IsNil(i1) {
		t.Error("Expected nil interface to be nil")
	}
	var i2 interface{} = 42
	if xreflect.IsNil(i2) {
		t.Error("Expected non-nil interface to not be nil")
	}
}

func TestGetStructName(t *testing.T) {
	type TestStruct struct {
		Name string
	}

	// 测试指针
	ts := &TestStruct{}
	name := xreflect.GetStructName(ts)
	if name != "TestStruct" {
		t.Errorf("Expected 'TestStruct', got '%s'", name)
	}

	// 测试非指针
	ts2 := TestStruct{}
	name = xreflect.GetStructName(ts2)
	if name != "TestStruct" {
		t.Errorf("Expected 'TestStruct', got '%s'", name)
	}

	// 测试 nil
	name = xreflect.GetStructName(nil)
	if name != "" {
		t.Errorf("Expected empty string, got '%s'", name)
	}

	// 测试基本类型
	var i int
	name = xreflect.GetStructName(i)
	if name != "int" {
		t.Errorf("Expected 'int', got '%s'", name)
	}
}

func TestIsDoublePointer(t *testing.T) {
	var i int = 42
	var pi *int = &i
	var ppi **int = &pi

	// 测试双指针
	if !xreflect.IsDoublePointer(ppi) {
		t.Error("Expected double pointer to be true")
	}

	// 测试单指针
	if xreflect.IsDoublePointer(pi) {
		t.Error("Expected single pointer to be false")
	}

	// 测试非指针
	if xreflect.IsDoublePointer(i) {
		t.Error("Expected non-pointer to be false")
	}

	// 测试 nil
	if xreflect.IsDoublePointer(nil) {
		t.Error("Expected nil to be false")
	}

	// 测试三指针
	var pppi ***int
	var ppi2 **int = &pi
	pppi = &ppi2
	if !xreflect.IsDoublePointer(pppi) {
		t.Error("Expected triple pointer (which is also double pointer) to be true")
	}
}

func TestIsPointer(t *testing.T) {
	var i int = 42
	var pi *int = &i

	// 测试指针
	if !xreflect.IsPointer(pi) {
		t.Error("Expected pointer to be true")
	}

	// 测试非指针
	if xreflect.IsPointer(i) {
		t.Error("Expected non-pointer to be false")
	}

	// 测试字符串
	s := "hello"
	if xreflect.IsPointer(s) {
		t.Error("Expected string to be false")
	}

	// 测试 nil
	if xreflect.IsPointer(nil) {
		t.Error("Expected nil to be false")
	}
}

func TestTypeName(t *testing.T) {
	type TestStruct struct {
		Name string
	}

	// 测试结构体类型
	ts := TestStruct{}
	typ := reflect.TypeOf(ts)
	name := xreflect.TypeName(typ)
	if name == "" {
		t.Error("Expected non-empty type name")
	}
	t.Logf("TypeName for TestStruct: %s", name)

	// 测试指针类型
	pts := &TestStruct{}
	ptyp := reflect.TypeOf(pts)
	pname := xreflect.TypeName(ptyp)
	if pname == "" {
		t.Error("Expected non-empty type name for pointer")
	}
	if !strings.HasPrefix(pname, "*") {
		t.Errorf("Expected pointer type name to start with '*', got '%s'", pname)
	}
	t.Logf("TypeName for *TestStruct: %s", pname)

	// 测试基本类型
	ityp := reflect.TypeOf(42)
	iname := xreflect.TypeName(ityp)
	if iname == "" {
		t.Error("Expected non-empty type name for int")
	}
	t.Logf("TypeName for int: %s", iname)
}
