package gormop

import "reflect"

func getStructName(ptr interface{}) string {
	t := reflect.TypeOf(ptr)
	if t.Kind() == reflect.Ptr {
		return t.Elem().Name()
	}
	return t.Name()
}

func isNil(comp interface{}) bool {
	if comp == nil {
		return true
	}
	v := reflect.ValueOf(comp)
	k := v.Kind()
	switch k {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func isDoublePointer(v interface{}) bool {
	t := reflect.TypeOf(v)
	if t.Kind() != reflect.Ptr {
		return false
	}
	return t.Elem().Kind() == reflect.Ptr
}
