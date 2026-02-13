// fbs2struct 从 .fbs 文件生成 Go struct 及 Pack/UnmarshalFlatBuffer 方法
//
// 用法:
//
//	fbs2struct -o ./proto/pbrpc/fbrpc -pkg fbrpc ./proto/pbrpc/rpc.fbs
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/vvisun/kkdg/tools/fbspb"
	"github.com/vvisun/kkdg/tools/fbs2struct"
)

func main() {
	outDir := flag.String("o", ".", "输出目录")
	pkg := flag.String("pkg", "", "Go 包名，默认取 fbs namespace")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "用法: fbs2struct [-o dir] [-pkg pkg] file.fbs [file2.fbs ...]")
		os.Exit(1)
	}
	for _, fbsPath := range args {
		if err := processFile(fbsPath, *outDir, *pkg); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}

func processFile(fbsPath, outDir, pkgOverride string) error {
	f, err := os.Open(fbsPath)
	if err != nil {
		return err
	}
	defer f.Close()

	sc, err := fbspb.ParseFBS(f)
	if err != nil {
		return fmt.Errorf("parse %s: %w", fbsPath, err)
	}

	pkg := pkgOverride
	if pkg == "" {
		pkg = sc.Package
	}
	if pkg == "" {
		pkg = "main"
	}

	baseName := strings.TrimSuffix(filepath.Base(fbsPath), ".fbs")
	outPath := filepath.Join(outDir, baseName+"_struct_gen.go")
	outFile, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("create %s: %w", outPath, err)
	}
	defer outFile.Close()

	opts := fbs2struct.GenOptions{Package: pkg}
	if err := fbs2struct.Generate(sc, outFile, opts); err != nil {
		return fmt.Errorf("generate: %w", err)
	}
	fmt.Println("generated:", outPath)
	return nil
}
