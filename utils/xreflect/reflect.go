package xreflect

import (
	"fmt"
	"reflect"
	"strings"
)

// Value 获取值的反射类型和值
func Value(val any) (reflect.Kind, reflect.Value) {
	var (
		rv = reflect.ValueOf(val)
		rk = rv.Kind()
	)

	for rk == reflect.Ptr {
		rv = rv.Elem()
		rk = rv.Kind()
	}

	return rk, rv
}

// IsNil 检测值是否为nil
func IsNil(val any) bool {
	if val == nil {
		return true
	}

	rv := reflect.ValueOf(val)
	rk := rv.Kind()

	switch rk {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.UnsafePointer, reflect.Interface, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

// 获取结构体名字
func GetStructName(ptr interface{}) string {
	if ptr == nil {
		return ""
	}
	if t := reflect.TypeOf(ptr); t.Kind() == reflect.Ptr {
		return t.Elem().Name()
	} else {
		return t.Name()
	}
}

// 判断是否是双指针
func IsDoublePointer(v interface{}) bool {
	if v == nil {
		return false
	}
	t := reflect.TypeOf(v)
	k := t.Kind()
	if k == reflect.Ptr {
		t = t.Elem()
		k = t.Kind()
		if k == reflect.Ptr {
			return true
		}
	}
	return false
}

// 判断是否是指针
func IsPointer(v interface{}) bool {
	return reflect.ValueOf(v).Kind() == reflect.Ptr
}

// TypeName 获取类型名称
func TypeName(T reflect.Type) string {
	pkgPath := ""
	typeName := ""
	isPtr := false
	if T.Kind() == reflect.Ptr {
		isPtr = true
		pkgPath = fmt.Sprintf("%s", T.Elem().PkgPath())
		typeName = fmt.Sprintf("%s", T.Elem().Name())
	} else {
		pkgPath = fmt.Sprintf("%s", T.PkgPath())
		typeName = fmt.Sprintf("%s", T.Name())
	}
	pkgPath = strings.TrimPrefix(pkgPath, "vendor/")
	rtn := fmt.Sprintf("%s.%s", pkgPath, typeName)
	if isPtr {
		rtn = "*" + rtn
	}
	return rtn
}
