package kkws

import (
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/netprocessor"
	"github.com/vvisun/kkdg/kknet/netprocessor/kkscsp"
)

func defaultWpProvider(opts kknet.WriteOptions) netprocessor.IWriteProcessor {
	return kkscsp.NewWriteProcessor(opts)
}

func defaultRpProvider(opts kknet.ReadOptions) netprocessor.IReadProcessor {
	return kkscsp.NewReadProcessor(opts)
}
