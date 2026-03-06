package xormeng

import (
	"sync"

	"github.com/vvisun/kkdg/storage/kkdb"
)

var (
	defaultEng     *DbEngine
	onceDefaultEng sync.Once
)

func Instance() *DbEngine {
	onceDefaultEng.Do(func() {
		cfgInfo := kkdb.DefaultDBOption()
		defaultEng = NewDbEngine(cfgInfo)
	})
	return defaultEng
}
