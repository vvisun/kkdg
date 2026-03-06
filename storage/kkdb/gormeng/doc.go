// Package gormeng 提供基于 GORM 的数据库引擎封装，复用 kkdb.DBOption。
//
// 生命周期：NewDbEngine(opt) → StartUp() → [SyncTables] → 业务使用 → Close()。
// 支持 Ping、SyncTables（AutoMigrate）、DropTables；事务见 Transaction / TransactionWithContext。
// 业务层可直接使用 GetInst() 获取 *gorm.DB，或配合 storage/kkdb/gormop 的 GetOne/Insert/Tx* 等封装。
package gormeng
