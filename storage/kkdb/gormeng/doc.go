// Package gormeng 提供基于 GORM 的数据库引擎封装。
// 与 xormeng 共用 kkdb.DBOption，支持 StartUp/Ping/Close、SyncTables（AutoMigrate）、DropTables。
// 业务层可直接使用 GetInst() 获取 *gorm.DB，或使用 storage/kkdb/gormop 的 GetOne/Insert/Update 等封装。
package gormeng
