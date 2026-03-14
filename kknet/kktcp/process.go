package kktcp

import (
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkprocessor"
)

const enableWP = false

func defaultWpProvider(opts kknet.WriteOptions) kknet.IWriteProcessor {
	return kkprocessor.NewWriteProcessor(opts)
}

func defaultRpProvider(opts kknet.ReadOptions) kknet.IReadProcessor {
	return kkprocessor.NewReadProcessor(opts)
}
