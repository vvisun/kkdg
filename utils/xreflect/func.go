package xreflect

import (
	"reflect"

	"github.com/vvisun/kkdg/kkerrors"
)

var (
	nilFuncInfo = FuncInfo{}
)

type FuncInfo struct {
	Type       reflect.Type
	Value      reflect.Value
	InArgs     []reflect.Type
	InArgsLen  int
	OutArgs    []reflect.Type
	OutArgsLen int
}

func GetFuncInfo(fn interface{}) (FuncInfo, error) {
	if fn == nil {
		return nilFuncInfo, kkerrors.ErrXreflectFuncIsNil
	}

	typ := reflect.TypeOf(fn)

	if typ.Kind() != reflect.Func {
		return nilFuncInfo, kkerrors.ErrXreflectFuncTypeError
	}

	var inArgs []reflect.Type
	for i := 0; i < typ.NumIn(); i++ {
		t := typ.In(i)
		inArgs = append(inArgs, t)
	}

	var outArgs []reflect.Type
	for i := 0; i < typ.NumOut(); i++ {
		t := typ.Out(i)
		outArgs = append(outArgs, t)
	}

	funcInfo := FuncInfo{
		Type:       typ,
		Value:      reflect.ValueOf(fn),
		InArgs:     inArgs,
		InArgsLen:  typ.NumIn(),
		OutArgs:    outArgs,
		OutArgsLen: typ.NumOut(),
	}

	return funcInfo, nil
}

// IsSameFunc 判断两个函数是否是同一个函数。
func IsSameFunc(fn1, fn2 interface{}) bool {
	info1, err := GetFuncInfo(fn1)
	if err != nil {
		return false
	}
	info2, err := GetFuncInfo(fn2)
	if err != nil {
		return false
	}
	return info1.Type == info2.Type && info1.Value.Pointer() == info2.Value.Pointer()
}
