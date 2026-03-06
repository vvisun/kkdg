package gormop

import (
	"errors"

	"github.com/vvisun/kkdg/storage/kkdb/gormeng"
	"github.com/vvisun/kkdg/utils/kklog"
	"gorm.io/gorm"
)

const maxBatchInsertCount = 500

var (
	errNilDbEngine = errors.New("DbEngine is nil")
	errNilBean     = errors.New("bean is nil")
	errInvalidBean = errors.New("bean is invalid")
	errNilInstance = errors.New("DbEngine instance is nil")
	errTooMany     = errors.New("too many")
)

func paramsCheck(dbEng *gormeng.DbEngine, bean interface{}) error {
	if dbEng == nil {
		kklog.Error("dbengine is nil")
		return errNilDbEngine
	}
	if dbEng.GetInst() == nil {
		kklog.Error("dbengine instance is nil")
		return errNilInstance
	}
	if isNil(bean) {
		kklog.Error("bean is nil")
		return errNilBean
	}
	if isDoublePointer(bean) {
		kklog.Error("bean is double pointer")
		return errInvalidBean
	}
	return nil
}

// GetOne 根据 bean 的非零字段作为条件查询一条记录，结果写回 bean。若未找到返回 (nil, nil)。
func GetOne[T any](dbEng *gormeng.DbEngine, bean *T) (*T, error) {
	if e := paramsCheck(dbEng, bean); e != nil {
		return nil, e
	}
	err := dbEng.GetInst().Where(bean).First(bean).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		kklog.Error("获取数据err ", getStructName(bean), " ", err.Error())
		return nil, err
	}
	return bean, nil
}

// GetByID 根据主键 id 查询一条记录到 bean。结果写回 bean。若未找到返回 (nil, nil)。
func GetByID[T any](dbEng *gormeng.DbEngine, bean *T, id interface{}) (*T, error) {
	if e := paramsCheck(dbEng, bean); e != nil {
		return nil, e
	}
	err := dbEng.GetInst().First(bean, id).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		kklog.Error("GetByID err ", getStructName(bean), " ", err.Error())
		return nil, err
	}
	return bean, nil
}

// GetList 根据 bean 条件查询多条记录。
func GetList[T any](dbEng *gormeng.DbEngine, bean *T) ([]*T, error) {
	if e := paramsCheck(dbEng, bean); e != nil {
		return nil, e
	}
	var list []*T
	err := dbEng.GetInst().Where(bean).Find(&list).Error
	if err != nil {
		kklog.Error("获取多条数据err ", getStructName(bean), " ", err.Error())
		return nil, err
	}
	return list, nil
}

// Insert 插入一条记录，返回影响行数（GORM 通常为 1）。
func Insert[T any](dbEng *gormeng.DbEngine, bean *T) (int64, error) {
	if e := paramsCheck(dbEng, bean); e != nil {
		return 0, e
	}
	tx := dbEng.GetInst().Create(bean)
	if tx.Error != nil {
		kklog.Error("插入数据err: ", getStructName(bean), tx.Error.Error())
		return 0, tx.Error
	}
	return tx.RowsAffected, nil
}

// InsertMulty 批量插入，单次最多 maxBatchInsertCount 条。
func InsertMulty[T any](dbEng *gormeng.DbEngine, beans []*T) (int64, error) {
	if len(beans) == 0 {
		return 0, errInvalidBean
	}
	if len(beans) > maxBatchInsertCount {
		kklog.Errorf("批量插入err, 一次最多%d条", maxBatchInsertCount)
		return 0, errTooMany
	}
	for i, bean := range beans {
		if e := paramsCheck(dbEng, bean); e != nil {
			kklog.Errorf("批量插入err, 参数无效, 索引: %d", i)
			return 0, e
		}
	}
	tx := dbEng.GetInst().CreateInBatches(beans, len(beans))
	if tx.Error != nil {
		kklog.Error("批量插入err: ", getStructName(beans[0]), tx.Error.Error())
		return 0, tx.Error
	}
	return tx.RowsAffected, nil
}

// Delete 按 bean 条件删除，返回影响行数。
func Delete[T any](dbEng *gormeng.DbEngine, bean *T) (int64, error) {
	if e := paramsCheck(dbEng, bean); e != nil {
		return 0, e
	}
	tx := dbEng.GetInst().Where(bean).Delete(bean)
	if tx.Error != nil {
		kklog.Error("删除数据err: ", getStructName(bean), tx.Error.Error())
		return 0, tx.Error
	}
	return tx.RowsAffected, nil
}

// Update 按 cond 条件更新为 data 中的非零字段。注意：零值字段不会更新。
func Update[T any](dbEng *gormeng.DbEngine, cond *T, data *T) (int64, error) {
	if e := paramsCheck(dbEng, cond); e != nil {
		return 0, e
	}
	if isNil(data) {
		return 0, errNilBean
	}
	if isDoublePointer(data) {
		return 0, errInvalidBean
	}
	tx := dbEng.GetInst().Model(cond).Where(cond).Updates(data)
	if tx.Error != nil {
		kklog.Error("更新数据err: ", getStructName(data), tx.Error.Error())
		return 0, tx.Error
	}
	return tx.RowsAffected, nil
}

// UpdateAllCols 按主键 id 全量更新 data 所有列（含零值）。主键列名从模型 Schema 获取（如 uid、id）。
func UpdateAllCols[T any](dbEng *gormeng.DbEngine, id interface{}, data *T) (int64, error) {
	if e := paramsCheck(dbEng, data); e != nil {
		return 0, e
	}
	db := dbEng.GetInst()
	pkCol := getPrimaryKeyColumn(db, data)
	tx := db.Model(data).Where(pkCol+" = ?", id).Select("*").Updates(data)
	if tx.Error != nil {
		kklog.Error("全量更新数据err: ", getStructName(data), tx.Error.Error())
		return 0, tx.Error
	}
	return tx.RowsAffected, nil
}

// UpdateCols 按 cond 条件只更新指定列 cols。
func UpdateCols[T any](dbEng *gormeng.DbEngine, cond *T, data *T, cols ...string) (int64, error) {
	if e := paramsCheck(dbEng, cond); e != nil {
		return 0, e
	}
	if isNil(data) {
		return 0, errNilBean
	}
	if len(cols) == 0 {
		return 0, nil
	}
	tx := dbEng.GetInst().Model(cond).Where(cond).Select(cols).Updates(data)
	if tx.Error != nil {
		kklog.Error("更新指定列数据err: ", getStructName(data), tx.Error.Error())
		return 0, tx.Error
	}
	return tx.RowsAffected, nil
}
