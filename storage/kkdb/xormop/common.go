package xormop

import (
	"database/sql"
	"reflect"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
)

// 获取结构体名字
func getStructName(ptr interface{}) string {
	if t := reflect.TypeOf(ptr); t.Kind() == reflect.Ptr {
		return t.Elem().Name()
	} else {
		return t.Name()
	}
}

// 检查指针是否是空值
func isNil(comp interface{}) bool {
	if comp == nil {
		return true
	}
	v := reflect.ValueOf(comp)
	k := v.Kind()
	// 检查可以为nil的类型
	switch k {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

// 判断是否是双指针
func isDoublePointer(v interface{}) bool {
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
func isPointer(v interface{}) bool {
	return reflect.ValueOf(v).Kind() == reflect.Ptr
}

func isNumberOrString(i interface{}) bool {
	switch i.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, string:
		return true
	}
	return false
}

func checkUpdateResult(result sql.Result, err error, wishRowCnt int64) error {
	if nil != err {
		kklog.Debug("更新失败：", err.Error())
		return kkerrors.ErrDatabaseUpdateFailed
	}
	if nil == result {
		kklog.Debug("更新失败 result is nil")
		return kkerrors.ErrDatabaseUpdateFailed
	}
	rcnt, err0 := result.RowsAffected()
	if err0 != nil {
		kklog.Debug("更新失败： %v", err0.Error())
		return kkerrors.ErrDatabaseUpdateFailed
	}
	if wishRowCnt > 0 && rcnt != wishRowCnt {
		kklog.Debug("更新失败，受影响行数不对 count: %v , wish: %v", rcnt, wishRowCnt)
		return kkerrors.ErrDatabaseUpdateFailed
	}
	return nil
}
