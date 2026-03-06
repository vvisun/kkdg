package gormop

import (
	"errors"

	"github.com/vvisun/kkdg/utils/kklog"
	"gorm.io/gorm"
)

var errNilTx = errors.New("tx is nil")

func txParamsCheck(tx *gorm.DB, bean interface{}) error {
	if tx == nil {
		kklog.Error("tx is nil")
		return errNilTx
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

// TxGetOne 在事务内根据 bean 条件查询一条，未找到返回 (nil, nil)。
func TxGetOne[T any](tx *gorm.DB, bean *T) (*T, error) {
	if e := txParamsCheck(tx, bean); e != nil {
		return nil, e
	}
	err := tx.Where(bean).First(bean).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		kklog.Error("TxGetOne err ", getStructName(bean), " ", err.Error())
		return nil, err
	}
	return bean, nil
}

// TxGetByID 在事务内按主键 id 查询一条。
func TxGetByID[T any](tx *gorm.DB, bean *T, id interface{}) (*T, error) {
	if e := txParamsCheck(tx, bean); e != nil {
		return nil, e
	}
	err := tx.First(bean, id).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		kklog.Error("TxGetByID err ", getStructName(bean), " ", err.Error())
		return nil, err
	}
	return bean, nil
}

// TxGetList 在事务内按 bean 条件查询多条。
func TxGetList[T any](tx *gorm.DB, bean *T) ([]*T, error) {
	if e := txParamsCheck(tx, bean); e != nil {
		return nil, e
	}
	var list []*T
	err := tx.Where(bean).Find(&list).Error
	if err != nil {
		kklog.Error("TxGetList err ", getStructName(bean), " ", err.Error())
		return nil, err
	}
	return list, nil
}

// TxInsert 在事务内插入一条，返回影响行数。
func TxInsert[T any](tx *gorm.DB, bean *T) (int64, error) {
	if e := txParamsCheck(tx, bean); e != nil {
		return 0, e
	}
	res := tx.Create(bean)
	if res.Error != nil {
		kklog.Error("TxInsert err: ", getStructName(bean), res.Error.Error())
		return 0, res.Error
	}
	return res.RowsAffected, nil
}

// TxInsertMulty 在事务内批量插入，单次最多 maxBatchInsertCount 条。
func TxInsertMulty[T any](tx *gorm.DB, beans []*T) (int64, error) {
	if len(beans) == 0 {
		return 0, errInvalidBean
	}
	if len(beans) > maxBatchInsertCount {
		kklog.Errorf("TxInsertMulty 一次最多%d条", maxBatchInsertCount)
		return 0, errTooMany
	}
	if tx == nil {
		return 0, errNilTx
	}
	for i, bean := range beans {
		if e := txParamsCheck(tx, bean); e != nil {
			kklog.Errorf("TxInsertMulty 参数无效, 索引: %d", i)
			return 0, e
		}
	}
	res := tx.CreateInBatches(beans, len(beans))
	if res.Error != nil {
		kklog.Error("TxInsertMulty err: ", getStructName(beans[0]), res.Error.Error())
		return 0, res.Error
	}
	return res.RowsAffected, nil
}

// TxDelete 在事务内按 bean 条件删除，返回影响行数。
func TxDelete[T any](tx *gorm.DB, bean *T) (int64, error) {
	if e := txParamsCheck(tx, bean); e != nil {
		return 0, e
	}
	res := tx.Where(bean).Delete(bean)
	if res.Error != nil {
		kklog.Error("TxDelete err: ", getStructName(bean), res.Error.Error())
		return 0, res.Error
	}
	return res.RowsAffected, nil
}

// TxUpdate 在事务内按 cond 条件更新为 data 的非零字段。
func TxUpdate[T any](tx *gorm.DB, cond *T, data *T) (int64, error) {
	if e := txParamsCheck(tx, cond); e != nil {
		return 0, e
	}
	if isNil(data) {
		return 0, errNilBean
	}
	if isDoublePointer(data) {
		return 0, errInvalidBean
	}
	res := tx.Model(cond).Where(cond).Updates(data)
	if res.Error != nil {
		kklog.Error("TxUpdate err: ", getStructName(data), res.Error.Error())
		return 0, res.Error
	}
	return res.RowsAffected, nil
}

// TxUpdateAllCols 在事务内按主键 id 全量更新 data 所有列。主键列名从模型 Schema 获取（如 uid、id）。
func TxUpdateAllCols[T any](tx *gorm.DB, id interface{}, data *T) (int64, error) {
	if e := txParamsCheck(tx, data); e != nil {
		return 0, e
	}
	pkCol := getPrimaryKeyColumn(tx, data)
	res := tx.Model(data).Where(pkCol+" = ?", id).Select("*").Updates(data)
	if res.Error != nil {
		kklog.Error("TxUpdateAllCols err: ", getStructName(data), res.Error.Error())
		return 0, res.Error
	}
	return res.RowsAffected, nil
}

// TxUpdateCols 在事务内按 cond 条件只更新指定列 cols。
func TxUpdateCols[T any](tx *gorm.DB, cond *T, data *T, cols ...string) (int64, error) {
	if e := txParamsCheck(tx, cond); e != nil {
		return 0, e
	}
	if isNil(data) {
		return 0, errNilBean
	}
	if len(cols) == 0 {
		return 0, nil
	}
	res := tx.Model(cond).Where(cond).Select(cols).Updates(data)
	if res.Error != nil {
		kklog.Error("TxUpdateCols err: ", getStructName(data), res.Error.Error())
		return 0, res.Error
	}
	return res.RowsAffected, nil
}
