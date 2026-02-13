package fbspb

import (
	"fmt"
	"io"
	"strings"
)

// fbNamespace 将 proto package 转为 flatbuffer namespace，pbXXX -> fbXXX
func fbNamespace(pkg string) string {
	if strings.HasPrefix(pkg, "pb") && len(pkg) > 2 {
		return "fb" + pkg[2:]
	}
	return pkg
}

// Proto2FBS 将 .proto Schema 转换为 .fbs 并写入 w
func Proto2FBS(sc *Schema, w io.Writer) error {
	if sc.Package != "" {
		ns := fbNamespace(sc.Package)
		_, err := fmt.Fprintf(w, "namespace %s;\n\n", ns)
		if err != nil {
			return err
		}
	}
	if len(sc.Includes) > 0 {
		for _, inc := range sc.Includes {
			_, err := fmt.Fprintf(w, "include %q;\n", inc)
			if err != nil {
				return err
			}
		}
		_, err := io.WriteString(w, "\n")
		if err != nil {
			return err
		}
	}

	// 收集需要生成的 MapEntry 表
	mapEntries := make(map[string]struct{})
	for _, m := range sc.Messages {
		for _, f := range m.Fields {
			if f.MapKey != "" && f.MapValue != "" {
				mapEntries[f.Type] = struct{}{}
			}
		}
	}

	// 先输出 MapEntry 表
	for _, m := range sc.Messages {
		for _, f := range m.Fields {
			if f.MapKey != "" && f.MapValue != "" {
				if _, ok := mapEntries[f.Type]; ok {
					keyType := mapProtoToFBSType(f.MapKey)
					valType := mapProtoToFBSType(f.MapValue)
					_, err := fmt.Fprintf(w, "table %s {\n  key: %s;\n  value: %s;\n}\n\n", f.Type, keyType, valType)
					if err != nil {
						return err
					}
					delete(mapEntries, f.Type)
				}
			}
		}
	}

	for _, e := range sc.Enums {
		if err := writeFBSEnum(w, &e); err != nil {
			return err
		}
	}

	for _, m := range sc.Messages {
		if err := writeFBSMessage(w, &m); err != nil {
			return err
		}
	}

	return nil
}

func mapProtoToFBSType(t string) string {
	if mapped, ok := protoToFBSType[t]; ok {
		return mapped
	}
	return t
}

func writeFBSEnum(w io.Writer, e *Enum) error {
	if e.Comment != "" {
		for _, line := range strings.Split(e.Comment, "\n") {
			if line = strings.TrimSpace(line); line != "" {
				if _, err := fmt.Fprintf(w, "// %s\n", line); err != nil {
					return err
				}
			}
		}
	}
	baseType := "int"
	if e.Type != "" {
		baseType = protoToFBSType[e.Type]
		if baseType == "" {
			baseType = e.Type
		}
	}
	_, err := fmt.Fprintf(w, "enum %s : %s {\n", e.Name, baseType)
	if err != nil {
		return err
	}
	for i, v := range e.Values {
		suffix := ","
		if i == len(e.Values)-1 {
			suffix = ""
		}
		if _, err := fmt.Fprintf(w, "  %s = %d%s\n", v.Name, v.Number, suffix); err != nil {
			return err
		}
	}
	_, err = io.WriteString(w, "}\n\n")
	return err
}

func writeFBSMessage(w io.Writer, m *Message) error {
	if m.Comment != "" {
		for _, line := range strings.Split(m.Comment, "\n") {
			if line = strings.TrimSpace(line); line != "" {
				if _, err := fmt.Fprintf(w, "// %s\n", line); err != nil {
					return err
				}
			}
		}
	}
	kind := "table"
	if m.IsStruct {
		kind = "struct"
	}
	_, err := fmt.Fprintf(w, "%s %s {\n", kind, m.Name)
	if err != nil {
		return err
	}
	for _, f := range m.Fields {
		if f.Comment != "" {
			for _, line := range strings.Split(f.Comment, "\n") {
				if line = strings.TrimSpace(line); line != "" {
					if _, err := fmt.Fprintf(w, "  // %s\n", line); err != nil {
						return err
					}
				}
			}
		}
		typeStr := f.Type
		if f.MapKey != "" && f.MapValue != "" {
			// map 转为 [MapXxxEntry]
			typeStr = "[" + f.Type + "]"
		} else if f.Repeated {
			if typeStr == "[ubyte]" || typeStr == "bytes" {
				typeStr = "[ubyte]"
			} else {
				typeStr = "[" + mapProtoToFBSType(typeStr) + "]"
			}
		} else {
			typeStr = mapProtoToFBSType(typeStr)
		}
		attrs := ""
		if f.Required && !m.IsStruct {
			attrs = " (required)"
		}
		if f.Deprecated {
			if attrs != "" {
				attrs += ", "
			}
			attrs += "deprecated"
		}
		if f.Default != "" && f.Default != "null" {
			if _, err := fmt.Fprintf(w, "  %s: %s = %s%s;\n", f.Name, typeStr, f.Default, attrs); err != nil {
				return err
			}
		} else {
			if _, err := fmt.Fprintf(w, "  %s: %s%s;\n", f.Name, typeStr, attrs); err != nil {
				return err
			}
		}
	}
	_, err = io.WriteString(w, "}\n\n")
	return err
}

// Proto2FBSFile 将 .proto 文件内容转换为 .fbs 格式并写入 w
func Proto2FBSFile(r io.Reader, w io.Writer) error {
	sc, err := ParseProto(r)
	if err != nil {
		return err
	}
	return Proto2FBS(sc, w)
}
