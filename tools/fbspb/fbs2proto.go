package fbspb

import (
	"fmt"
	"io"
	"strings"
)

// FBS2Proto 将 .fbs Schema 转换为 .proto 并写入 w
func FBS2Proto(sc *Schema, w io.Writer) error {
	_, err := io.WriteString(w, "syntax = \"proto3\";\n")
	if err != nil {
		return err
	}
	if sc.Package != "" {
		_, err = fmt.Fprintf(w, "package %s;\n", sc.Package)
		if err != nil {
			return err
		}
	}
	if sc.GoPackage != "" {
		_, err = fmt.Fprintf(w, "option go_package = %q;\n", sc.GoPackage)
		if err != nil {
			return err
		}
	}
	if len(sc.Includes) > 0 {
		for _, inc := range sc.Includes {
			protoPath := strings.TrimSuffix(inc, ".fbs") + ".proto"
			_, err = fmt.Fprintf(w, "import %q;\n", protoPath)
			if err != nil {
				return err
			}
		}
	}
	_, err = io.WriteString(w, "\n")
	if err != nil {
		return err
	}

	for _, e := range sc.Enums {
		if err := writeProtoEnum(w, &e); err != nil {
			return err
		}
	}

	for _, m := range sc.Messages {
		if err := writeProtoMessage(w, &m, false); err != nil {
			return err
		}
	}

	return nil
}

func writeProtoEnum(w io.Writer, e *Enum) error {
	if e.Comment != "" {
		for _, line := range strings.Split(e.Comment, "\n") {
			if line = strings.TrimSpace(line); line != "" {
				if _, err := fmt.Fprintf(w, "// %s\n", line); err != nil {
					return err
				}
			}
		}
	}
	_, err := fmt.Fprintf(w, "enum %s {\n", e.Name)
	if err != nil {
		return err
	}
	for _, v := range e.Values {
		opt := fmt.Sprintf(" = %d;", v.Number)
		if v.Comment != "" {
			if _, err := fmt.Fprintf(w, "  // %s\n", v.Comment); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(w, "  %s%s\n", v.Name, opt); err != nil {
			return err
		}
	}
	_, err = io.WriteString(w, "}\n\n")
	return err
}

func writeProtoMessage(w io.Writer, m *Message, nested bool) error {
	if m.Comment != "" {
		for _, line := range strings.Split(m.Comment, "\n") {
			if line = strings.TrimSpace(line); line != "" {
				if _, err := fmt.Fprintf(w, "// %s\n", line); err != nil {
					return err
				}
			}
		}
	}
	indent := ""
	if nested {
		indent = "  "
	}
	if _, err := fmt.Fprintf(w, "%smessage %s {\n", indent, m.Name); err != nil {
		return err
	}
	for i, f := range m.Fields {
		if f.Comment != "" {
			for _, line := range strings.Split(f.Comment, "\n") {
				if line = strings.TrimSpace(line); line != "" {
					if _, err := fmt.Fprintf(w, "%s  // %s\n", indent, line); err != nil {
						return err
					}
				}
			}
		}
		typeStr := f.Type
		if f.Repeated {
			typeStr = "repeated " + typeStr
		}
		opt := ""
		if f.Deprecated {
			opt = " [deprecated = true]"
		}
		if _, err := fmt.Fprintf(w, "%s  %s %s = %d%s;\n", indent, typeStr, f.Name, f.Number, opt); err != nil {
			return err
		}
		if i < len(m.Fields)-1 {
			if _, err := io.WriteString(w, "\n"); err != nil {
				return err
			}
		}
	}
	if _, err := fmt.Fprintf(w, "%s}\n\n", indent); err != nil {
		return err
	}
	return nil
}

// FBS2ProtoFile 将 .fbs 文件内容转换为 .proto 格式并写入 w
func FBS2ProtoFile(r io.Reader, w io.Writer) error {
	sc, err := ParseFBS(r)
	if err != nil {
		return err
	}
	return FBS2Proto(sc, w)
}
