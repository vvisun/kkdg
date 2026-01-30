package stubgen

import (
	"path/filepath"
	"regexp"
	"strings"
)

var nonIdentRe = regexp.MustCompile(`[^A-Za-z0-9_]+`)

// DefaultOutputFilename returns a stable output filename for the given spec.
func DefaultOutputFilename(spec Spec) string {
	if strings.TrimSpace(spec.OutputFile) != "" {
		return spec.OutputFile
	}
	goSvc := strings.TrimSpace(spec.GoServiceName)
	if goSvc == "" {
		goSvc = deriveGoServiceName(spec.ServiceName)
	}
	base := goSvc
	if base == "" {
		base = spec.ServiceName
	}
	base = strings.Trim(base, "/")
	base = strings.ReplaceAll(base, ".", "_")
	base = strings.ReplaceAll(base, "/", "_")
	base = nonIdentRe.ReplaceAllString(base, "_")
	base = strings.Trim(base, "_")
	if base == "" {
		base = "service"
	}
	return filepath.Base(base + "_gnrpc.pb.go")
}

