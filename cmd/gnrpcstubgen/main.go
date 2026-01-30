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

type bundleSpec struct {
	Package  string        `json:"package"`
	OutputDir string       `json:"outputDir,omitempty"`
	Services []stubgen.Spec `json:"services"`
}

func main() {
	var in string
	var out string
	var outDir string
	var recursive bool
	var stdout bool

	flag.StringVar(&in, "in", "", "path to spec.json")
	flag.StringVar(&out, "out", "", "output .go file (single spec only; optional when -stdout)")
	flag.StringVar(&outDir, "outdir", "", "output directory (for directory input or default naming)")
	flag.BoolVar(&recursive, "r", false, "recursive scan when -in is a directory")
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

		specFiles, err := listSpecFiles(in, recursive)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "list specs:", err)
			os.Exit(2)
		}
		for _, specPath := range specFiles {
			specs, bOutDir, err := readSpecs(specPath)
			if err != nil {
				_, _ = fmt.Fprintln(os.Stderr, "read spec:", specPath, err)
				os.Exit(2)
			}
			targetDir := outDir
			if bOutDir != "" && outDir == in {
				// If caller didn't override outdir and spec bundle provides one, honor it.
				targetDir = bOutDir
				if !filepath.IsAbs(targetDir) {
					targetDir = filepath.Join(in, targetDir)
				}
				if err := os.MkdirAll(targetDir, 0o755); err != nil {
					_, _ = fmt.Fprintln(os.Stderr, "mkdir outputDir:", err)
					os.Exit(2)
				}
			}
			for _, spec := range specs {
				code, err := stubgen.Generate(spec)
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, "generate:", specPath, err)
					os.Exit(2)
				}
				outPath := filepath.Join(targetDir, stubgen.DefaultOutputFilename(spec))
				if err := os.WriteFile(outPath, code, 0o644); err != nil {
					_, _ = fmt.Fprintln(os.Stderr, "write output:", outPath, err)
					os.Exit(2)
				}
			}
		}
		return
	}

	// single spec mode
	specs, bundleOutDir, err := readSpecs(in)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "read spec:", err)
		os.Exit(2)
	}
	if len(specs) != 1 && out != "" {
		_, _ = fmt.Fprintln(os.Stderr, "-out is only valid when the input contains exactly 1 service")
		os.Exit(2)
	}
	if stdout && len(specs) != 1 {
		_, _ = fmt.Fprintln(os.Stderr, "-stdout is only supported when the input contains exactly 1 service")
		os.Exit(2)
	}
	if outDir == "" && bundleOutDir != "" {
		outDir = bundleOutDir
	}

	for i, spec := range specs {
		code, err := stubgen.Generate(spec)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "generate:", err)
			os.Exit(2)
		}
		if stdout {
			_, _ = os.Stdout.Write(code)
			return
		}
		target := out
		if target == "" {
			fn := stubgen.DefaultOutputFilename(spec)
			if outDir != "" {
				if err := os.MkdirAll(outDir, 0o755); err != nil {
					_, _ = fmt.Fprintln(os.Stderr, "mkdir -outdir:", err)
					os.Exit(2)
				}
				target = filepath.Join(outDir, fn)
			} else {
				target = fn
			}
		} else if len(specs) == 1 && i == 0 {
			// keep -out
		}
		if err := os.WriteFile(target, code, 0o644); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "write output:", err)
			os.Exit(2)
		}
	}
}

func readSpecs(path string) ([]stubgen.Spec, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}

	// 1) bundle format: {package, outputDir, services:[...]}
	var bundle bundleSpec
	if err := json.Unmarshal(data, &bundle); err == nil && len(bundle.Services) > 0 {
		for i := range bundle.Services {
			if bundle.Services[i].Package == "" {
				bundle.Services[i].Package = bundle.Package
			}
		}
		return bundle.Services, strings.TrimSpace(bundle.OutputDir), nil
	}

	// 2) array format: [...]
	var specs []stubgen.Spec
	if err := json.Unmarshal(data, &specs); err == nil && len(specs) > 0 {
		return specs, "", nil
	}

	// 3) single format: {...}
	var spec stubgen.Spec
	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, "", err
	}
	return []stubgen.Spec{spec}, "", nil
}

func listSpecFiles(root string, recursive bool) ([]string, error) {
	var out []string
	if !recursive {
		entries, err := os.ReadDir(root)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			if strings.HasSuffix(strings.ToLower(name), ".json") {
				out = append(out, filepath.Join(root, name))
			}
		}
		return out, nil
	}

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(strings.ToLower(d.Name()), ".json") {
			out = append(out, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

