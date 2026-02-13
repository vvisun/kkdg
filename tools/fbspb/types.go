package fbspb

// FBS 与 Proto 共用的 AST 节点

// Schema 表示完整的 schema 定义
type Schema struct {
	Package   string   // namespace / package
	GoPackage string   // proto option go_package
	Includes  []string  // include / import 路径
	Enums     []Enum
	Messages  []Message // table/struct/message
	RootType  string    // root_type (仅 fbs)
}

// Enum 枚举定义
type Enum struct {
	Name    string
	Type    string   // 底层类型 byte/int 等，proto 无则空
	Values  []EnumValue
	Comment string
}

// EnumValue 枚举值
type EnumValue struct {
	Name    string
	Number  int
	Comment string
}

// Message 消息/表/结构体定义
type Message struct {
	Name     string
	IsStruct bool   // true=struct(全required), false=table/message
	Fields   []Field
	Comment  string
}

// Field 字段定义
type Field struct {
	Name       string // 字段名
	Type       string // 基础类型或消息名
	Repeated   bool   // [T] 或 repeated
	MapKey     string // map 的 key 类型，非 map 时为空
	MapValue   string // map 的 value 类型，非 map 时为空
	Default    string // 默认值
	Required   bool   // required 属性
	Deprecated bool   // deprecated 属性
	Number     int    // proto field number
	Comment    string
}
