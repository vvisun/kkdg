package gormop

import (
	"reflect"
	"strings"

	"gorm.io/gorm"
)

// getPrimaryKeyColumn 从模型的 GORM Schema 或 struct tag 取主键列名，若无则返回 "id"。
func getPrimaryKeyColumn(db *gorm.DB, model interface{}) string {
	if model == nil {
		return "id"
	}
	if db != nil {
		stmt := db.Model(model).Statement
		if stmt.Schema != nil && len(stmt.Schema.PrimaryFields) > 0 {
			return stmt.Schema.PrimaryFields[0].DBName
		}
	}
	// 回退：从 struct tag 查找含 primaryKey 的字段，取 column 或转 snake_case
	if col := getPrimaryKeyColumnFromStruct(model); col != "" {
		return col
	}
	return "id"
}

func getPrimaryKeyColumnFromStruct(model interface{}) string {
	t := reflect.TypeOf(model)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return ""
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := f.Tag.Get("gorm")
		if tag == "" {
			continue
		}
		if !strings.Contains(tag, "primaryKey") {
			continue
		}
		// 解析 column:xxx
		for _, part := range strings.Split(tag, ";") {
			part = strings.TrimSpace(part)
			if strings.HasPrefix(part, "column:") {
				return strings.TrimSpace(strings.TrimPrefix(part, "column:"))
			}
		}
		// 无 column 则用字段名转 snake_case
		return toSnakeCase(f.Name)
	}
	return ""
}

func toSnakeCase(s string) string {
	var b strings.Builder
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			b.WriteByte('_')
		}
		b.WriteRune(r)
	}
	return strings.ToLower(b.String())
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
