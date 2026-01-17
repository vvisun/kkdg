//go:build openbsd
// +build openbsd

package kkstat

import (
	"syscall"
	"time"
)

// CreateTime 获取文件创建时间
func (fs *fileStat) CreateTime() time.Time {
	stat := fs.fi.Sys().(*syscall.Stat_t)

	return time.Unix(int64(stat.Ctim.Sec), int64(stat.Ctim.Nsec))
}
