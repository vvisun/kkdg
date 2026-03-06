package gormeng

import (
	"errors"
	"sync"
	"sync/atomic"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/vvisun/kkdg/storage/kkdb"
	"github.com/vvisun/kkdg/utils/kklog"
)

var (
	errNotStarted = errors.New("db not started")
)

// NewDbEngine 创建 GORM 引擎，复用 kkdb.DBOption。
func NewDbEngine(opt kkdb.DBOption) *DbEngine {
	eng := &DbEngine{}
	eng.opts = opt
	return eng
}

// NewDbEngineFromDB 用于测试或已有 *gorm.DB 时包装为 DbEngine，无需 DBOption。调用方负责 db 的生命周期。
func NewDbEngineFromDB(db *gorm.DB) *DbEngine {
	return &DbEngine{dbInst: db}
}

type DbEngine struct {
	dbInst      *gorm.DB
	opts        kkdb.DBOption
	tablesReady int32
	tbMutex     sync.RWMutex
}

// GetInst 返回 GORM 数据库实例，供业务层直接使用或配合 gormop 使用。
func (e *DbEngine) GetInst() *gorm.DB {
	return e.dbInst
}

func (e *DbEngine) initDB() bool {
	e.tbMutex.RLock()
	defer e.tbMutex.RUnlock()

	if e.dbInst != nil {
		return true
	}

	if e.opts.Dsn == "" {
		kklog.Error("db start up error: dsn is empty")
		return false
	}

	cfg := &gorm.Config{}
	if e.opts.ShowSql {
		cfg.Logger = logger.Default.LogMode(logger.Info)
	} else {
		cfg.Logger = logger.Default.LogMode(logger.Silent)
	}

	db, err := gorm.Open(mysql.Open(e.opts.Dsn), cfg)
	if err != nil {
		kklog.Error("create database instance error: ", err.Error())
		return false
	}

	sqlDB, err := db.DB()
	if err != nil {
		kklog.Error("get sql.DB error: ", err.Error())
		return false
	}
	sqlDB.SetMaxIdleConns(e.opts.MaxIdle)
	sqlDB.SetMaxOpenConns(e.opts.MaxOpen)

	e.dbInst = db
	kklog.Infof("初始化数据库(GORM): ShowSQL=%t MaxIdleConn=%v MaxOpenConn=%v", e.opts.ShowSql, e.opts.MaxIdle, e.opts.MaxOpen)
	return true
}

// StartUp 启动数据库连接并 Ping 校验。
func (e *DbEngine) StartUp() bool {
	if !e.initDB() {
		kklog.Error("数据库 start up error: create database instance fail")
		return false
	}
	if !e.Ping() {
		kklog.Error("数据库 start up error: ping database fail")
		return false
	}
	kklog.Info("数据库 start up success (GORM)")
	return true
}

// Ping 检查数据库连接是否可用。
func (e *DbEngine) Ping() bool {
	if e.dbInst == nil {
		kklog.Error("ping database error: ", errNotStarted.Error())
		return false
	}
	sqlDB, err := e.dbInst.DB()
	if err != nil {
		kklog.Error("ping database error: ", err.Error())
		return false
	}
	if err := sqlDB.Ping(); err != nil {
		kklog.Error("ping database error: ", err.Error())
		return false
	}
	return true
}

// Close 关闭数据库连接。
func (e *DbEngine) Close() error {
	if e.dbInst != nil {
		sqlDB, err := e.dbInst.DB()
		if err != nil {
			kklog.Error("db shutdown error: ", err.Error())
			return err
		}
		if err := sqlDB.Close(); err != nil {
			kklog.Error("db shutdown error: ", err.Error())
			return err
		}
		e.dbInst = nil
		kklog.Info("db shutdown success (GORM)")
	}
	return nil
}

// SyncTables 根据模型自动迁移表结构（GORM AutoMigrate）。
// beans 为模型指针，如 &User{}。
func (e *DbEngine) SyncTables(beans ...interface{}) error {
	if atomic.LoadInt32(&e.tablesReady) == 1 {
		return nil
	}

	e.tbMutex.Lock()
	defer e.tbMutex.Unlock()

	if e.dbInst == nil {
		kklog.Error("sync schema error: db instance is nil")
		return errNotStarted
	}

	if err := e.dbInst.AutoMigrate(beans...); err != nil {
		kklog.Error("sync schema error: ", err.Error())
		return err
	}

	atomic.StoreInt32(&e.tablesReady, 1)
	return nil
}

// DropTables 删除指定模型对应的表。
func (e *DbEngine) DropTables(beans ...interface{}) error {
	e.tbMutex.Lock()
	defer e.tbMutex.Unlock()

	if e.dbInst == nil {
		kklog.Error("drop tables error: db instance is nil")
		return errNotStarted
	}

	migrator := e.dbInst.Migrator()
	for _, bean := range beans {
		if err := migrator.DropTable(bean); err != nil {
			kklog.Error("drop table error: ", err.Error())
			return err
		}
	}

	atomic.StoreInt32(&e.tablesReady, 0)
	return nil
}
