package fbs2struct

// fbs 类型到 Go 结构体类型的映射
var fbsToGoType = map[string]string{
	"bytes":   "[]byte", // [ubyte] 映射
	"byte":    "int8",
	"ubyte":   "uint8",
	"int8":    "int8",
	"uint8":   "uint8",
	"short":   "int16",
	"ushort":  "uint16",
	"int16":   "int16",
	"uint16":  "uint16",
	"int":     "int32",
	"uint":    "uint32",
	"int32":   "int32",
	"uint32":  "uint32",
	"long":    "int64",
	"ulong":   "uint64",
	"int64":   "int64",
	"uint64":  "uint64",
	"float":   "float32",
	"float32": "float32",
	"double":  "float64",
	"float64": "float64",
	"bool":    "bool",
	"string":  "string",
}
