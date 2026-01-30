package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/vvisun/kkdg/kknet/rpc/gnrpc/stubgen"
)

func main() {
	var in string
	var out string
	var stdout bool

	flag.StringVar(&in, "in", "", "path to spec.json")
	flag.StringVar(&out, "out", "", "output .go file (optional when -stdout)")
	flag.BoolVar(&stdout, "stdout", false, "write to stdout")
	flag.Parse()

	if in == "" {
		_, _ = fmt.Fprintln(os.Stderr, "missing -in spec.json")
		os.Exit(2)
	}

	data, err := os.ReadFile(in)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "read spec:", err)
		os.Exit(2)
	}

	var spec stubgen.Spec
	if err := json.Unmarshal(data, &spec); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "parse spec:", err)
		os.Exit(2)
	}

	code, err := stubgen.Generate(spec)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "generate:", err)
		os.Exit(2)
	}

	if stdout || out == "" {
		_, _ = os.Stdout.Write(code)
		return
	}
	if err := os.WriteFile(out, code, 0o644); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "write output:", err)
		os.Exit(2)
	}
}

