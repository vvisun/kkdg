package kktcp

import (
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/netprocessor/kkscsp"
)

func defaultWpProvider(opts kknet.WriteOptions) kknet.IWriteProcessor {
	return kkscsp.NewWriteProcessor(opts)
}

func defaultRpProvider(opts kknet.ReadOptions) kknet.IReadProcessor {
	return kkscsp.NewReadProcessor(opts)
}
