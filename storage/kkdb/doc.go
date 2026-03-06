// Package kkdb 提供数据库连接配置与引擎抽象。
//
// 配置：DBOption 定义 DSN、连接池等；DefaultDBOption 返回默认值（生产请用 WithDsn 或环境变量覆盖）；
// ApplyOptions 与 With* 用于链式构造；CheckOption 用于启动前校验。
//
// 引擎实现见子包：
//   - gormeng：GORM 引擎（StartUp/Close/SyncTables/Transaction）
//   - gormop：CRUD 与事务内操作（GetOne/Insert/Tx* 及 *Ctx 系列）
package kkdb
