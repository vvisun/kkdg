package main

// 该工具用于生成消息ID与消息类型的映射关系。
// 读取指定 proto 目录下的所有 .proto 文件，根据 route_cfg.json 文件生成消息ID与消息类型的映射关系，输出到指定文件。
// 用法示例:
//   msdid ./zothers/examples/examapp/ptoexam
//
// 在 ptoexam 目录中:
//   - route_cfg.json:
//       {
//         "test.proto":  {"route":"logic",  "fromId": 1},
//         "test1.proto": {"route":"combat", "fromId": 1000}
//       }
//   - test.proto / test1.proto: 定义若干 message
// 生成:
//   - init.go:
//       func InitMsgs(router *kkpacket.MsgRouter) {
//           // test.proto
//           router.Register(1,   &Msg1Req{},        "logic")
//           router.Register(2,   &Msg1Resp{},       "logic")
//           ...
//           // test1.proto
//           router.Register(1000, &AaaaReq{},       "combat")
//           ...
//       }
//
// 规则:
//   - 每个 proto 文件在 route_cfg.json 中配置一个起始 ID(fromId) 和 route。
//   - 该 proto 文件中按 message 出现顺序依次分配 ID: fromId, fromId+1, fromId+2, ...
//   - message 名称直接使用 Go 生成的类型名（与 proto 定义一致）。

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type routeCfg struct {
	Route  string `json:"route"`
	FromID uint32 `json:"fromId"`
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}
	dir := os.Args[1]
	if err := run(dir); err != nil {
		fmt.Fprintln(os.Stderr, "msdid:", err)
		os.Exit(1)
	}
}

func printUsage() {
	exe := "msdid"
	if len(os.Args) > 0 {
		exe = filepath.Base(os.Args[0])
	}
	fmt.Fprintf(os.Stderr, "用法: %s <proto目录>\n", exe)
	fmt.Fprintln(os.Stderr, "示例:")
	fmt.Fprintf(os.Stderr, "  %s ./zothers/examples/examapp/ptoexam\n", exe)
}

func run(dir string) error {
	info, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("stat dir %s: %w", dir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s 不是目录", dir)
	}

	cfgPath := filepath.Join(dir, "route_cfg.json")
	cfgMap, err := loadRouteCfg(cfgPath)
	if err != nil {
		return err
	}
	if len(cfgMap) == 0 {
		return fmt.Errorf("route_cfg.json 中没有配置任何条目")
	}

	// 确定包名: 优先从 proto 的 go_package / package 读取，失败则退回目录名。
	pkgName, err := detectGoPackage(dir, cfgMap)
	if err != nil {
		return err
	}

	// 按文件名排序，保证 init.go 生成顺序稳定。
	fileNames := make([]string, 0, len(cfgMap))
	for name := range cfgMap {
		fileNames = append(fileNames, name)
	}
	sort.Strings(fileNames)

	var buf bytes.Buffer
	fmt.Fprintln(&buf, "// 该文件自动生成，无需手动调整")
	fmt.Fprintf(&buf, "package %s\n\n", pkgName)
	fmt.Fprintln(&buf, "import \"github.com/vvisun/kkdg/kknet/kkpacket\"")
	fmt.Fprintln(&buf)
	fmt.Fprintln(&buf, "func InitMsgs(router *kkpacket.MsgRouter) {")

	for _, protoFile := range fileNames {
		cfg := cfgMap[protoFile]
		fullProtoPath := filepath.Join(dir, protoFile)

		msgNames, err := parseProtoMessages(fullProtoPath)
		if err != nil {
			return fmt.Errorf("解析 proto 文件 %s 失败: %w", fullProtoPath, err)
		}
		if len(msgNames) == 0 {
			continue
		}

		fmt.Fprintf(&buf, "\t// %s\n", protoFile)
		for i, msgName := range msgNames {
			id := cfg.FromID + uint32(i)
			fmt.Fprintf(&buf, "\trouter.Register(%d, &%s{}, %q)\n", id, msgName, cfg.Route)
		}
	}
	fmt.Fprintln(&buf, "}")

	outPath := filepath.Join(dir, "init.go")
	tmpPath := outPath + ".tmp"
	if err := os.WriteFile(tmpPath, buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("写入临时文件 %s 失败: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, outPath); err != nil {
		return fmt.Errorf("重命名 %s -> %s 失败: %w", tmpPath, outPath, err)
	}
	fmt.Println("generated:", outPath)
	return nil
}

func loadRouteCfg(path string) (map[string]routeCfg, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取 route_cfg.json 失败: %w", err)
	}
	cfgMap := make(map[string]routeCfg)
	if err := json.Unmarshal(data, &cfgMap); err != nil {
		return nil, fmt.Errorf("解析 route_cfg.json 失败: %w", err)
	}
	return cfgMap, nil
}

// detectGoPackage 尝试从配置的 proto 文件中推断 Go 包名:
//  1. option go_package = "xxx;pkg" -> 取分号后的 pkg。
//  2. package ptoexam;              -> 取该值。
//  3. 以上都没有时，使用目录名。
func detectGoPackage(dir string, cfgMap map[string]routeCfg) (string, error) {
	for protoFile := range cfgMap {
		full := filepath.Join(dir, protoFile)
		f, err := os.Open(full)
		if err != nil {
			continue
		}
		sc := bufio.NewScanner(f)
		var pkg string
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if strings.HasPrefix(line, "option go_package") {
				// 形如: option go_package = ".;ptoexam";
				if idx := strings.Index(line, "="); idx >= 0 {
					val := strings.TrimSpace(line[idx+1:])
					val = strings.Trim(val, `";`+" ")
					// 查找分号后的部分
					if semi := strings.LastIndex(val, ";"); semi >= 0 && semi+1 < len(val) {
						pkg = val[semi+1:]
					} else if val != "" {
						pkg = val
					}
				}
			} else if strings.HasPrefix(line, "package ") && pkg == "" {
				// 形如: package ptoexam;
				rest := strings.TrimSpace(strings.TrimPrefix(line, "package"))
				rest = strings.Trim(rest, "; ")
				if rest != "" {
					pkg = rest
				}
			}
			if pkg != "" {
				break
			}
		}
		f.Close()
		if err := sc.Err(); err != nil {
			continue
		}
		if pkg != "" {
			return pkg, nil
		}
	}
	// 回退: 目录名
	base := filepath.Base(dir)
	if base == "." || base == "/" || base == string(filepath.Separator) {
		return "", fmt.Errorf("无法从 proto 中推断包名, 且目录名非法: %s", dir)
	}
	return base, nil
}

// parseProtoMessages 解析 proto 文件中的 message 名称，按出现顺序返回。
// 仅做简单行级解析: 识别形如 `message MsgName {` 的行，忽略注释行。
func parseProtoMessages(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var msgs []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "//") {
			continue
		}
		if !strings.HasPrefix(line, "message ") {
			continue
		}
		// 去掉前缀与后面的 '{'、注释等
		rest := strings.TrimSpace(strings.TrimPrefix(line, "message"))
		// rest 类似 "Msg1Req {" 或 "Msg1Req{"
		rest = strings.TrimLeft(rest, " \t")
		// 截断到第一个空白或 '{'
		end := len(rest)
		for i, ch := range rest {
			if ch == '{' || ch == ' ' || ch == '\t' {
				end = i
				break
			}
		}
		if end > 0 {
			name := rest[:end]
			if name != "" {
				msgs = append(msgs, name)
			}
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return msgs, nil
}
