package kktcp

import (
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkprocessor"
)

func defaultRpProvider(opts kknet.ReadOptions) kknet.IReadProcessor {
	return kkprocessor.NewReadProcessor(opts)
}
