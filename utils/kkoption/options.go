package kkoption

// ApplyOptionWithDefault applies the given options to the default option.
func ApplyOptionWithDefault[T any](defaultOption func() T, opts ...func(o *T)) T {
	cfg := defaultOption()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

// ApplyOption applies the given options to a new zero value option.
func ApplyOptionsWithNew[T any](opts ...func(o *T)) T {
	var cfg T
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

// ApplyOptionsTo applies the given options to the given option.
func ApplyOptionsTo[T any](cfg *T, opts ...func(o *T)) *T {
	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}
	return cfg
}
