package kkerrors

import "errors"

var (
	// 数据库未启动
	ErrDatabaseNotStarted = errors.New("database not started")
	// 数据库已启动
	ErrDatabaseAlreadyStarted = errors.New("database already started")
	// 数据库未初始化
	ErrDatabaseNotInitialized = errors.New("database not initialized")
	// 数据库已初始化
	ErrDatabaseAlreadyInitialized = errors.New("database already initialized")
	// 数据库操作失败(增删改查)
	ErrDatabaseOperationFailed = errors.New("database operation failed")
	// 数据库操作失败(查询)
	ErrDatabaseQueryFailed = errors.New("database query failed")
	// 数据库操作失败(插入)
	ErrDatabaseInsertFailed = errors.New("database insert failed")
	// 数据库操作失败(更新)
	ErrDatabaseUpdateFailed = errors.New("database update failed")
	// 数据库操作失败(删除)
	ErrDatabaseDeleteFailed = errors.New("database delete failed")
)
