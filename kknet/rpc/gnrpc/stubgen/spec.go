package stubgen

// Spec describes a service stub to generate.
type Spec struct {
	// Package is the Go package name for the generated file.
	Package string `json:"package"`
	// OutputFile is optional metadata for tooling.
	OutputFile string `json:"outputFile,omitempty"`

	// ServiceName is the canonical service name, e.g. "pbbase.StringService".
	ServiceName string `json:"serviceName"`

	// GoServiceName is the Go identifier prefix, e.g. "StringService".
	// If empty, generator derives it from ServiceName.
	GoServiceName string `json:"goServiceName,omitempty"`

	// Methods list.
	Methods []MethodSpec `json:"methods"`
}

type MethodSpec struct {
	// Name is the RPC method name, e.g. "Echo".
	Name string `json:"name"`

	// Request is a Go type reference, e.g. "github.com/vvisun/kkdg/proto/pbbase.String".
	Request string `json:"request"`
	// Response is a Go type reference, e.g. "github.com/vvisun/kkdg/proto/pbbase.String".
	Response string `json:"response"`
}

