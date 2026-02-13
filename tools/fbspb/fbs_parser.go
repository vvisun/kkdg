package fbspb

import (
	"bufio"
	"io"
	"strings"
	"unicode"
)

// 类型映射 fbs -> proto
var fbsToProtoType = map[string]string{
	"byte":   "int32",
	"ubyte":  "uint32",
	"int8":   "int32",
	"uint8":  "uint32",
	"short":  "int32",
	"ushort": "uint32",
	"int16":  "int32",
	"uint16": "uint32",
	"int":    "int32",
	"uint":   "uint32",
	"int32":  "int32",
	"uint32": "uint32",
	"long":   "int64",
	"ulong":  "uint64",
	"int64":  "int64",
	"uint64": "uint64",
	"float":  "float",
	"float32": "float",
	"double": "double",
	"float64": "double",
	"bool":   "bool",
	"string": "string",
}

// ParseFBS 解析 .fbs 文件为 Schema
func ParseFBS(r io.Reader) (*Schema, error) {
	sc := &Schema{}
	scanner := bufio.NewScanner(r)
	var commentBuf strings.Builder

	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		// 空行
		if trimmed == "" {
			commentBuf.Reset()
			continue
		}

		// 单行注释
		if strings.HasPrefix(trimmed, "//") {
			commentBuf.WriteString(strings.TrimPrefix(trimmed, "//"))
			commentBuf.WriteByte('\n')
			continue
		}

		// 文档注释
		if strings.HasPrefix(trimmed, "///") {
			commentBuf.WriteString(strings.TrimSpace(strings.TrimPrefix(trimmed, "///")))
			commentBuf.WriteByte('\n')
			continue
		}

		comment := strings.TrimSpace(commentBuf.String())
		commentBuf.Reset()

		// namespace
		if strings.HasPrefix(trimmed, "namespace ") {
			sc.Package = strings.TrimSuffix(strings.TrimSpace(trimmed[9:]), ";")
			sc.Package = strings.TrimSpace(sc.Package)
			continue
		}

		// include
		if strings.HasPrefix(trimmed, "include ") {
			inc := parseQuoted(strings.TrimSpace(trimmed[8:]))
			if inc != "" {
				sc.Includes = append(sc.Includes, inc)
			}
			continue
		}

		// root_type
		if strings.HasPrefix(trimmed, "root_type ") {
			sc.RootType = strings.TrimSuffix(strings.TrimSpace(trimmed[9:]), ";")
			continue
		}

		// attribute, file_identifier, file_extension 等忽略
		if strings.HasPrefix(trimmed, "attribute ") ||
			strings.HasPrefix(trimmed, "file_identifier ") ||
			strings.HasPrefix(trimmed, "file_extension ") {
			continue
		}

		// enum
		if strings.HasPrefix(trimmed, "enum ") {
			rest := strings.TrimSpace(trimmed[5:])
			braceIdx := strings.Index(rest, "{")
			nameAndType := rest
			inner := ""
			if braceIdx >= 0 {
				nameAndType = strings.TrimSpace(rest[:braceIdx])
				inner = strings.TrimSpace(rest[braceIdx+1:])
			}
			parts := strings.SplitN(nameAndType, ":", 2)
			name := strings.TrimSpace(parts[0])
			baseType := "int32"
			if len(parts) > 1 {
				baseType = strings.TrimSpace(parts[1])
				if t, ok := fbsToProtoType[baseType]; ok {
					baseType = t
				}
			}
			enum := Enum{Name: name, Type: baseType, Comment: comment}
			enum.Values, _ = parseFBSEnumBody(scanner, inner)
			sc.Enums = append(sc.Enums, enum)
			continue
		}

		// union - 转为注释或跳过，proto 用 oneof 近似
		if strings.HasPrefix(trimmed, "union ") {
			parseFBSUnionBody(scanner)
			continue
		}

		// struct
		if strings.HasPrefix(trimmed, "struct ") {
			name := extractName(trimmed[7:], "{")
			msg := Message{Name: name, IsStruct: true, Comment: comment}
			var nested []Message
			msg.Fields, _ = parseFBSMessageBodyImpl(scanner, true, 1, &nested)
			sc.Messages = append(sc.Messages, nested...) // 嵌套的在前
			sc.Messages = append(sc.Messages, msg)
			continue
		}

		// table
		if strings.HasPrefix(trimmed, "table ") {
			name := extractName(trimmed[6:], "{")
			msg := Message{Name: name, IsStruct: false, Comment: comment}
			var nested []Message
			msg.Fields, _ = parseFBSMessageBodyImpl(scanner, false, 1, &nested)
			sc.Messages = append(sc.Messages, nested...)
			sc.Messages = append(sc.Messages, msg)
			continue
		}

		// rpc_service 忽略
		if strings.HasPrefix(trimmed, "rpc_service ") {
			parseFBSBlock(scanner)
			continue
		}
	}

	return sc, scanner.Err()
}

func parseQuoted(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimSuffix(s, ";")
	if len(s) >= 2 && (s[0] == '"' || s[0] == '\'') {
		return strings.Trim(s, `"'`)
	}
	return s
}

func extractName(s string, endChar string) string {
	s = strings.TrimSpace(s)
	if idx := strings.Index(s, endChar); idx >= 0 {
		return strings.TrimSpace(s[:idx])
	}
	return s
}

func parseFBSEnumBody(scanner *bufio.Scanner, firstLine string) ([]EnumValue, error) {
	var values []EnumValue
	num := 0
	parseEnumLine := func(line string) (done bool) {
		line = strings.TrimSpace(line)
		for line != "" {
			if strings.HasPrefix(line, "}") {
				return true
			}
			comma := strings.Index(line, ",")
			end := strings.Index(line, "}")
			tok := line
			if comma >= 0 && (end < 0 || comma < end) {
				tok = strings.TrimSpace(line[:comma])
				line = strings.TrimSpace(line[comma+1:])
			} else if end >= 0 {
				tok = strings.TrimSpace(line[:end])
				line = "}"
			} else {
				line = ""
			}
			tok = strings.TrimSuffix(tok, ",")
			if tok == "" || tok == "}" {
				if tok == "}" {
					return true
				}
				continue
			}
			parts := strings.SplitN(tok, "=", 2)
			name := strings.TrimSpace(parts[0])
			if len(parts) > 1 {
				n, _ := parseInt(strings.TrimSpace(parts[1]))
				num = n
			}
			values = append(values, EnumValue{Name: name, Number: num})
			num++
		}
		return false
	}
	if parseEnumLine(firstLine) {
		return values, nil
	}
	for scanner.Scan() {
		if parseEnumLine(scanner.Text()) {
			break
		}
	}
	return values, nil
}

func parseFBSUnionBody(scanner *bufio.Scanner) {
	parseFBSBlock(scanner)
}

func parseFBSBlock(scanner *bufio.Scanner) {
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

func parseFBSMessageBody(scanner *bufio.Scanner, isStruct bool, startNum int) ([]Field, int) {
	return parseFBSMessageBodyImpl(scanner, isStruct, startNum, nil)
}

func parseFBSMessageBodyImpl(scanner *bufio.Scanner, isStruct bool, startNum int, nestedOut *[]Message) ([]Field, int) {
	var fields []Field
	fieldNum := startNum
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if trimmed == "}" {
			break
		}
		if trimmed == "" || strings.HasPrefix(trimmed, "//") || strings.HasPrefix(trimmed, "///") {
			continue
		}

		// 嵌套 table/struct
		if strings.HasPrefix(trimmed, "table ") || strings.HasPrefix(trimmed, "struct ") {
			prefix := "table "
			if strings.HasPrefix(trimmed, "struct ") {
				prefix = "struct "
			}
			name := extractName(trimmed[len(prefix):], "{")
			msg := Message{Name: name, IsStruct: strings.HasPrefix(trimmed, "struct ")}
			msg.Fields, fieldNum = parseFBSMessageBodyImpl(scanner, msg.IsStruct, fieldNum, nestedOut)
			if nestedOut != nil {
				*nestedOut = append(*nestedOut, msg)
			}
			continue
		}

		// 字段:  name : type = default (attrs);
		field := parseFBSField(trimmed, fieldNum, isStruct)
		if field.Name != "" {
			fields = append(fields, field)
			fieldNum++
		}
	}
	return fields, fieldNum
}

func parseFBSField(line string, num int, isStruct bool) Field {
	line = strings.TrimSuffix(line, ";")
	idx := strings.Index(line, ":")
	if idx < 0 {
		return Field{Number: num}
	}
	name := strings.TrimSpace(line[:idx])
	typePart := strings.TrimSpace(line[idx+1:])

	// 解析 (required), (deprecated), default
	var defaultVal string
	var required, deprecated bool
	if p := strings.Index(typePart, "("); p >= 0 {
		q := strings.Index(typePart, ")")
		if q > p {
			attrs := strings.ToLower(typePart[p+1 : q])
			typePart = strings.TrimSpace(typePart[:p]) + strings.TrimSpace(typePart[q+1:])
			if strings.Contains(attrs, "required") {
				required = true
			}
			if strings.Contains(attrs, "deprecated") {
				deprecated = true
			}
		}
	}
	if eq := strings.Index(typePart, "="); eq >= 0 {
		defaultVal = strings.TrimSpace(typePart[eq+1:])
		typePart = strings.TrimSpace(typePart[:eq])
	}
	// [T] -> repeated
	repeated := false
	if len(typePart) >= 2 && typePart[0] == '[' && typePart[len(typePart)-1] == ']' {
		repeated = true
		inner := strings.TrimSpace(typePart[1 : len(typePart)-1])
		if inner == "ubyte" || inner == "byte" {
			typePart = "bytes" // [ubyte] -> bytes
			repeated = false
		} else {
			typePart = inner
		}
	}
	// 映射类型
	baseType := typePart
	if typePart != "bytes" {
		if t, ok := fbsToProtoType[typePart]; ok {
			baseType = t
		}
	}

	return Field{
		Name:       name,
		Type:       baseType,
		Repeated:   repeated,
		Default:    defaultVal,
		Required:   required || isStruct,
		Deprecated: deprecated,
		Number:     num,
	}
}

func parseInt(s string) (int, bool) {
	var n int
	for _, c := range s {
		if !unicode.IsDigit(c) && c != '-' && c != '+' {
			break
		}
		if c >= '0' && c <= '9' {
			n = n*10 + int(c-'0')
		}
	}
	return n, true
}
