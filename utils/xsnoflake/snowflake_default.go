package xsnoflake

import (
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xhash"
)

var (
	defaultNode *Node
)

func InitDefaultNode(str string) {
	var (
		crc32Value = int64(xhash.CRC32(str))
		nodeValue  = crc32Value % nodeMax
	)

	SetDefaultNode(nodeValue)
}

func SetDefaultNode(nodeValue int64) {
	if defaultNode != nil {
		kklog.Warn("default snowflake node is created.")
		return
	}

	var err error
	defaultNode, err = NewNode(nodeValue)
	if err != nil {
		kklog.Warn(err)
		kklog.Warnf("create default snowflake node fail. nodeValue = %d", nodeValue)
	}

	kklog.Infof("[snowflake] nodeValue = %d, nodeMax = %d", nodeValue, nodeMax)
}

func Next() ID {
	return defaultNode.Generate()
}

func NextID() int64 {
	return defaultNode.Generate().Int64()
}
