package fbspb

import (
	"bufio"
	"io"
	"regexp"
	"strings"
)

// proto 到 fbs 类型映射
var protoToFBSType = map[string]string{
	"int32":   "int",
	"uint32":  "uint",
	"sint32":  "int",
	"fixed32": "uint",
	"sfixed32": "int32",
	"int64":   "long",
	"uint64":  "ulong",
	"sint64":  "long",
	"fixed64": "ulong",
	"sfixed64": "int64",
	"float":   "float",
	"double":  "double",
	"bool":    "bool",
	"string":  "string",
	"bytes":   "[ubyte]",
}

// ParseProto 解析 .proto 文件为 Schema
func ParseProto(r io.Reader) (*Schema, error) {
	sc := &Schema{}
	scanner := bufio.NewScanner(r)
	var commentBuf strings.Builder

	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		if trimmed == "" {
			commentBuf.Reset()
			continue
		}

		// 注释
		if strings.HasPrefix(trimmed, "//") {
			commentBuf.WriteString(strings.TrimSpace(strings.TrimPrefix(trimmed, "//")))
			commentBuf.Reset()
			continue
		}

		comment := strings.TrimSpace(commentBuf.String())
		commentBuf.Reset()

		// syntax
		if strings.HasPrefix(trimmed, "syntax ") {
			continue
		}

		// package
		if strings.HasPrefix(trimmed, "package ") {
			sc.Package = parseProtoIdent(strings.TrimSuffix(strings.TrimSpace(trimmed[8:]), ";"))
			continue
		}

		// option
		if strings.HasPrefix(trimmed, "option ") {
			if strings.Contains(trimmed, "go_package") {
				sc.GoPackage = parseProtoOptionGoPackage(trimmed)
			}
			continue
		}

		// import
		if strings.HasPrefix(trimmed, "import ") {
			imp := parseProtoImport(trimmed)
			if imp != "" {
				imp = strings.TrimSuffix(imp, ".proto") + ".fbs"
				sc.Includes = append(sc.Includes, imp)
			}
			continue
		}

		// enum
		if strings.HasPrefix(trimmed, "enum ") {
			name := extractName(trimmed[5:], "{")
			enum := Enum{Name: strings.TrimSpace(name), Comment: comment}
			enum.Values, _ = parseProtoEnumBody(scanner)
			sc.Enums = append(sc.Enums, enum)
			continue
		}

		// message
		if strings.HasPrefix(trimmed, "message ") {
			name := extractName(trimmed[8:], "{")
			msg := Message{Name: strings.TrimSpace(name), IsStruct: false, Comment: comment}
			var nested []Message
			msg.Fields, _ = parseProtoMessageBodyImpl(scanner, 1, &nested)
			sc.Messages = append(sc.Messages, nested...)
			sc.Messages = append(sc.Messages, msg)
			continue
		}

		// service - 忽略
		if strings.HasPrefix(trimmed, "service ") {
			parseProtoBlock(scanner)
			continue
		}
	}

	return sc, scanner.Err()
}

func parseProtoIdent(s string) string {
	return strings.Trim(s, `";`)
}

func parseProtoOptionGoPackage(line string) string {
	idx := strings.Index(line, "=")
	if idx < 0 {
		return ""
	}
	val := strings.TrimSpace(line[idx+1:])
	val = strings.TrimSuffix(val, ";")
	return strings.Trim(val, `"`)
}

func parseProtoImport(line string) string {
	line = strings.TrimSuffix(line, ";")
	idx := strings.Index(line, "\"")
	if idx < 0 {
		return ""
	}
	end := strings.Index(line[idx+1:], "\"")
	if end < 0 {
		return ""
	}
	return line[idx+1 : idx+1+end]
}

func parseProtoEnumBody(scanner *bufio.Scanner) ([]EnumValue, error) {
	var values []EnumValue
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if trimmed == "}" {
			break
		}
		trimmed = strings.TrimSuffix(trimmed, ";")
		if trimmed == "" || strings.HasPrefix(trimmed, "//") {
			continue
		}
		// NAME = N;
		re := regexp.MustCompile(`^(\w+)\s*=\s*(-?\d+)`)
		if m := re.FindStringSubmatch(trimmed); len(m) >= 3 {
			num, _ := parseInt(m[2])
			values = append(values, EnumValue{Name: m[1], Number: num})
		}
	}
	return values, nil
}

func parseProtoMessageBody(scanner *bufio.Scanner, startNum int) ([]Field, int) {
	return parseProtoMessageBodyImpl(scanner, startNum, nil)
}

func parseProtoMessageBodyImpl(scanner *bufio.Scanner, startNum int, nestedOut *[]Message) ([]Field, int) {
	var fields []Field
	fieldNum := startNum
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if trimmed == "}" {
			break
		}
		if trimmed == "" || strings.HasPrefix(trimmed, "//") {
			continue
		}

		// 嵌套 enum - 跳过
		if strings.HasPrefix(trimmed, "enum ") {
			parseProtoBlock(scanner)
			continue
		}

		// 嵌套 message
		if strings.HasPrefix(trimmed, "message ") {
			name := extractName(trimmed[8:], "{")
			msg := Message{Name: strings.TrimSpace(name), IsStruct: false}
			msg.Fields, fieldNum = parseProtoMessageBodyImpl(scanner, fieldNum, nestedOut)
			if nestedOut != nil {
				*nestedOut = append(*nestedOut, msg)
			}
			continue
		}

		// oneof - 跳过
		if strings.HasPrefix(trimmed, "oneof ") {
			parseProtoBlock(scanner)
			continue
		}

		// 字段: [repeated] type name = num [options];
		field := parseProtoField(trimmed, fieldNum)
		if field.Name != "" {
			fields = append(fields, field)
			fieldNum++
		}
	}
	return fields, fieldNum
}

func parseProtoField(line string, num int) Field {
	line = strings.TrimSuffix(line, ";")
	// 去掉 [deprecated=true] 等选项
	if idx := strings.Index(line, "["); idx >= 0 {
		line = strings.TrimSpace(line[:idx])
	}
	// repeated type name = num 或 type name = num
	repeated := false
	if strings.HasPrefix(line, "repeated ") {
		repeated = true
		line = strings.TrimSpace(line[9:])
	}
	// map<K,V> name = num
	if strings.HasPrefix(line, "map<") {
		end := strings.Index(line, ">")
		if end < 0 {
			return Field{Number: num}
		}
		kv := strings.Split(line[4:end], ",")
		keyType := ""
		valType := ""
		if len(kv) >= 1 {
			keyType = strings.TrimSpace(kv[0])
		}
		if len(kv) >= 2 {
			valType = strings.TrimSpace(kv[1])
		}
		rest := strings.TrimSpace(line[end+1:])
		parts := strings.SplitN(rest, "=", 2)
		name := strings.TrimSpace(parts[0])
		if name == "" {
			return Field{Number: num}
		}
		// map 转为 table MapXxxEntry { key: K; value: V; }
		entryName := "Map" + strings.ToUpper(name[:1]) + name[1:] + "Entry"
		return Field{
			Name:     name,
			Type:     entryName,
			Repeated: true,
			MapKey:   keyType,
			MapValue: valType,
			Number:   num,
		}
	}
	// 匹配 type name = num
	re := regexp.MustCompile(`^(\w+(?:\.\w+)*)\s+(\w+)\s*=\s*(-?\d+)`)
	m := re.FindStringSubmatch(line)
	if len(m) < 4 {
		return Field{Number: num}
	}
	typeStr := m[1]
	name := m[2]
	n, _ := parseInt(m[3])
	typeStr = mapProtoType(typeStr)
	return Field{
		Name:     name,
		Type:     typeStr,
		Repeated: repeated,
		Number:   n,
	}
}

func mapProtoType(t string) string {
	if mapped, ok := protoToFBSType[t]; ok {
		return mapped
	}
	return t // 自定义类型保持
}

func parseProtoBlock(scanner *bufio.Scanner) {
	depth := 1
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		for _, c := range trimmed {
			if c == '{' {
				depth++
			} else if c == '}' {
				depth--
				if depth == 0 {
					return
				}
			}
		}
	}
}
