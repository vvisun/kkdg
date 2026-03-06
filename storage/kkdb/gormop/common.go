package gormop

import (
	"reflect"

	"gorm.io/gorm"
)

// getPrimaryKeyColumn 从模型的 GORM Schema 取主键列名，若无则返回 "id"。
func getPrimaryKeyColumn(db *gorm.DB, model interface{}) string {
	if db == nil || model == nil {
		return "id"
	}
	stmt := db.Model(model).Statement
	if stmt.Schema != nil && len(stmt.Schema.PrimaryFields) > 0 {
		return stmt.Schema.PrimaryFields[0].DBName
	}
	return "id"
}

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
