package xormeng

import (
	"errors"
	"sync"
	"sync/atomic"

	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"github.com/vvisun/kkdg/storage/kkdb"
	"github.com/vvisun/kkdg/utils/kklog"
)

var (
	err_notStarted = errors.New("db not started")
)

func NewDbEngine(opt kkdb.DBOption) *DbEngine {
	eng := &DbEngine{}
	eng.opts = opt
	return eng
}

type DbEngine struct {
	dbInst      *xorm.Engine
	opts        kkdb.DBOption
	tablesReady int32
	tbMutex     sync.RWMutex
}

// 数据库实例
func (slf *DbEngine) GetInst() *xorm.Engine {
	return slf.dbInst
}

// 配置日志
func (slf *DbEngine) cfgLogger() {
	// olog := &Logger{}
	// slf.dbInst.SetLogger(olog)
}

func (slf *DbEngine) initDB() bool {
	slf.tbMutex.RLock()
	defer slf.tbMutex.RUnlock()

	if slf.dbInst != nil {
		return true
	}

	if slf.opts.Dsn == "" {
		kklog.Error("db start up error: dsn is empty")
		return false
	}

	if slf.dbInst != nil {
		return true
	}

	inst, err := xorm.NewEngine(slf.opts.DriverName, slf.opts.Dsn)
	if err != nil {
		kklog.Error("create database instance error: ", err.Error())
		return false
	}

	slf.dbInst = inst

	// 配置
	slf.cfgLogger()
	slf.dbInst.SetMaxIdleConns(slf.opts.MaxIdle)
	slf.dbInst.SetMaxOpenConns(slf.opts.MaxOpen)
	slf.dbInst.ShowSQL(slf.opts.ShowSql)

	kklog.Infof("初始化数据库: DSN=%s ShowSQL=%t MaxIdleConn=%v MaxOpenConn=%v", slf.opts.Dsn, slf.opts.ShowSql, slf.opts.MaxIdle, slf.opts.MaxOpen)

	return true
}

// 启动数据库
func (slf *DbEngine) StartUp() bool {
	// create database instance
	if !slf.initDB() {
		kklog.Error("数据库 start up error: create database instance fail")
		return false
	}

	canPing := slf.Ping()

	if !canPing {
		kklog.Error("数据库 start up error: ping database fail")
		return false
	}

	kklog.Info("数据库 start up success")

	return true
}

// ping数据库
func (slf *DbEngine) Ping() bool {
	if slf.dbInst == nil {
		kklog.Error("ping database error: ", err_notStarted.Error())
		return false
	}
	e := slf.dbInst.Ping()
	if e != nil {
		kklog.Error("ping database error: ", e.Error())
		return false
	}
	return true
}

// 关闭数据库
func (slf *DbEngine) Close() error {
	if slf.dbInst != nil {
		e := slf.dbInst.Close()
		if e != nil {
			kklog.Error("db shutdown error: ", e.Error())
			return e
		}
		slf.dbInst = nil
		kklog.Info("db shutdown success")
	}
	return nil
}

// 建表
// synchronize structs to database tables
func (slf *DbEngine) SyncTables(beans ...interface{}) error {
	if atomic.LoadInt32(&slf.tablesReady) == 1 {
		return nil
	}

	slf.tbMutex.Lock()
	defer slf.tbMutex.Unlock()

	if slf.dbInst == nil {
		kklog.Error("syncSchema error: db instance is nil")
		return err_notStarted
	}

	e := slf.dbInst.StoreEngine(slf.opts.StoreEngine).Charset(slf.opts.Charset).Sync2(beans...)

	if e != nil {
		kklog.Error("sync schema error: ", e.Error())
		return e
	}

	atomic.StoreInt32(&slf.tablesReady, 1)

	return nil
}

// 删表
// drop specify tables
func (slf *DbEngine) DropTables(beans ...interface{}) error {
	slf.tbMutex.Lock()
	defer slf.tbMutex.Unlock()

	if slf.dbInst == nil {
		kklog.Error("clearTables error: db instance is nil")
		return err_notStarted
	}

	e := slf.dbInst.DropTables(beans...)

	if e != nil {
		kklog.Error("clear tables error: ", e.Error())
		return e
	}

	atomic.StoreInt32(&slf.tablesReady, 0)

	return nil
}
