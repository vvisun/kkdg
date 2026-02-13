// fbspb 实现 .fbs 与 .proto 文件互转
//
// 用法:
//
//	fbspb fbs2proto <file.fbs|dir>    # fbs 转 proto
//	fbspb proto2fbs <file.proto|dir>  # proto 转 fbs
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/vvisun/kkdg/tools/fbspb"
)

func main() {
	if len(os.Args) < 3 {
		printUsage()
		os.Exit(1)
	}
	cmd := strings.ToLower(os.Args[1])
	path := os.Args[2]

	switch cmd {
	case "fbs2proto":
		runFBS2Proto(path)
	case "proto2fbs":
		runProto2FBS(path)
	default:
		fmt.Fprintf(os.Stderr, "未知命令: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func runFBS2Proto(path string) {
	info, err := os.Stat(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if info.IsDir() {
		converted, err := fbspb.ConvertDirFBS2Proto(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		for _, p := range converted {
			fmt.Println("converted:", p)
		}
	} else {
		if !strings.HasSuffix(path, ".fbs") {
			fmt.Fprintln(os.Stderr, "请输入 .fbs 文件")
			os.Exit(1)
		}
		out, err := fbspb.FBSFileToProto(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println("converted:", out)
	}
}

func runProto2FBS(path string) {
	info, err := os.Stat(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if info.IsDir() {
		converted, err := fbspb.ConvertDirProto2FBS(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		for _, p := range converted {
			fmt.Println("converted:", p)
		}
	} else {
		if !strings.HasSuffix(path, ".proto") {
			fmt.Fprintln(os.Stderr, "请输入 .proto 文件")
			os.Exit(1)
		}
		out, err := fbspb.ProtoFileToFBS(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println("converted:", out)
	}
}

func printUsage() {
	exe := "fbspb"
	if len(os.Args) > 0 {
		exe = filepath.Base(os.Args[0])
	}
	fmt.Fprintf(os.Stderr, `用法:
  %s fbs2proto <file.fbs|目录>    # .fbs 转 .proto
  %s proto2fbs <file.proto|目录>  # .proto 转 .fbs
`, exe, exe)
}
