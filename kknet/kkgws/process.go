package kkgws

import (
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkprocessor"
)

func defaultWpProvider(opts kknet.WriteOptions) kknet.IWriteProcessor {
	return kkprocessor.NewWriteProcessor(opts)
}

func defaultRpProvider(opts kknet.ReadOptions) kknet.IReadProcessor {
	return kkprocessor.NewReadProcessor(opts)
}
