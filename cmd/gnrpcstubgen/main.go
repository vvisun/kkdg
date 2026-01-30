package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/vvisun/kkdg/kknet/rpc/gnrpc/stubgen"
)

func main() {
	var in string
	var out string
	var outDir string
	var stdout bool

	flag.StringVar(&in, "in", "", "path to spec.json")
	flag.StringVar(&out, "out", "", "output .go file (single spec only; optional when -stdout)")
	flag.StringVar(&outDir, "outdir", "", "output directory (for directory input or default naming)")
	flag.BoolVar(&stdout, "stdout", false, "write to stdout")
	flag.Parse()

	if in == "" {
		_, _ = fmt.Fprintln(os.Stderr, "missing -in spec.json")
		os.Exit(2)
	}

	fi, err := os.Stat(in)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "stat -in:", err)
		os.Exit(2)
	}

	if fi.IsDir() {
		// batch mode
		if stdout {
			_, _ = fmt.Fprintln(os.Stderr, "-stdout is not supported with directory input")
			os.Exit(2)
		}
		if out != "" {
			_, _ = fmt.Fprintln(os.Stderr, "-out is not supported with directory input (use -outdir)")
			os.Exit(2)
		}
		if outDir == "" {
			outDir = in
		}
		if err := os.MkdirAll(outDir, 0o755); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "mkdir -outdir:", err)
			os.Exit(2)
		}

		entries, err := os.ReadDir(in)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "readdir:", err)
			os.Exit(2)
		}

		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			if !strings.HasSuffix(strings.ToLower(name), ".json") {
				continue
			}
			specPath := filepath.Join(in, name)
			spec, err := readSpec(specPath)
			if err != nil {
				_, _ = fmt.Fprintln(os.Stderr, "read spec:", specPath, err)
				os.Exit(2)
			}
			code, err := stubgen.Generate(spec)
			if err != nil {
				_, _ = fmt.Fprintln(os.Stderr, "generate:", specPath, err)
				os.Exit(2)
			}
			outPath := filepath.Join(outDir, stubgen.DefaultOutputFilename(spec))
			if err := os.WriteFile(outPath, code, 0o644); err != nil {
				_, _ = fmt.Fprintln(os.Stderr, "write output:", outPath, err)
				os.Exit(2)
			}
		}
		return
	}

	// single spec mode
	spec, err := readSpec(in)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "read spec:", err)
		os.Exit(2)
	}
	code, err := stubgen.Generate(spec)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "generate:", err)
		os.Exit(2)
	}
	if stdout {
		_, _ = os.Stdout.Write(code)
		return
	}
	if out == "" {
		fn := stubgen.DefaultOutputFilename(spec)
		if outDir != "" {
			if err := os.MkdirAll(outDir, 0o755); err != nil {
				_, _ = fmt.Fprintln(os.Stderr, "mkdir -outdir:", err)
				os.Exit(2)
			}
			out = filepath.Join(outDir, fn)
		} else {
			out = fn
		}
	}
	if err := os.WriteFile(out, code, 0o644); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "write output:", err)
		os.Exit(2)
	}
}

func readSpec(path string) (stubgen.Spec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return stubgen.Spec{}, err
	}
	var spec stubgen.Spec
	if err := json.Unmarshal(data, &spec); err != nil {
		return stubgen.Spec{}, err
	}
	return spec, nil
}

