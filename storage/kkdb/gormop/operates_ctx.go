package gormop

import (
	"context"
	"errors"

	"github.com/vvisun/kkdg/storage/kkdb/gormeng"
	"github.com/vvisun/kkdg/utils/kklog"
	"gorm.io/gorm"
)

func sessionWithContext(dbEng *gormeng.DbEngine, ctx context.Context) *gorm.DB {
	if dbEng == nil || dbEng.GetInst() == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return dbEng.GetInst().WithContext(ctx)
}

// GetOneCtx 带上下文的 GetOne，支持超时与取消。
func GetOneCtx[T any](ctx context.Context, dbEng *gormeng.DbEngine, bean *T) (*T, error) {
	if e := paramsCheck(dbEng, bean); e != nil {
		return nil, e
	}
	sess := sessionWithContext(dbEng, ctx)
	err := sess.Where(bean).First(bean).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		kklog.Error("获取数据err ", getStructName(bean), " ", err.Error())
		return nil, err
	}
	return bean, nil
}

// GetByIDCtx 带上下文的 GetByID。
func GetByIDCtx[T any](ctx context.Context, dbEng *gormeng.DbEngine, bean *T, id interface{}) (*T, error) {
	if e := paramsCheck(dbEng, bean); e != nil {
		return nil, e
	}
	sess := sessionWithContext(dbEng, ctx)
	err := sess.First(bean, id).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		kklog.Error("GetByID err ", getStructName(bean), " ", err.Error())
		return nil, err
	}
	return bean, nil
}

// GetListCtx 带上下文的 GetList。
func GetListCtx[T any](ctx context.Context, dbEng *gormeng.DbEngine, bean *T) ([]*T, error) {
	if e := paramsCheck(dbEng, bean); e != nil {
		return nil, e
	}
	sess := sessionWithContext(dbEng, ctx)
	var list []*T
	err := sess.Where(bean).Find(&list).Error
	if err != nil {
		kklog.Error("获取多条数据err ", getStructName(bean), " ", err.Error())
		return nil, err
	}
	return list, nil
}

// InsertCtx 带上下文的 Insert。
func InsertCtx[T any](ctx context.Context, dbEng *gormeng.DbEngine, bean *T) (int64, error) {
	if e := paramsCheck(dbEng, bean); e != nil {
		return 0, e
	}
	sess := sessionWithContext(dbEng, ctx)
	tx := sess.Create(bean)
	if tx.Error != nil {
		kklog.Error("插入数据err: ", getStructName(bean), tx.Error.Error())
		return 0, tx.Error
	}
	return tx.RowsAffected, nil
}

// InsertMultyCtx 带上下文的 InsertMulty。
func InsertMultyCtx[T any](ctx context.Context, dbEng *gormeng.DbEngine, beans []*T) (int64, error) {
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
	sess := sessionWithContext(dbEng, ctx)
	tx := sess.CreateInBatches(beans, len(beans))
	if tx.Error != nil {
		kklog.Error("批量插入err: ", getStructName(beans[0]), tx.Error.Error())
		return 0, tx.Error
	}
	return tx.RowsAffected, nil
}

// DeleteCtx 带上下文的 Delete。
func DeleteCtx[T any](ctx context.Context, dbEng *gormeng.DbEngine, bean *T) (int64, error) {
	if e := paramsCheck(dbEng, bean); e != nil {
		return 0, e
	}
	sess := sessionWithContext(dbEng, ctx)
	tx := sess.Where(bean).Delete(bean)
	if tx.Error != nil {
		kklog.Error("删除数据err: ", getStructName(bean), tx.Error.Error())
		return 0, tx.Error
	}
	return tx.RowsAffected, nil
}

// UpdateCtx 带上下文的 Update。
func UpdateCtx[T any](ctx context.Context, dbEng *gormeng.DbEngine, cond *T, data *T) (int64, error) {
	if e := paramsCheck(dbEng, cond); e != nil {
		return 0, e
	}
	if isNil(data) {
		return 0, errNilBean
	}
	if isDoublePointer(data) {
		return 0, errInvalidBean
	}
	sess := sessionWithContext(dbEng, ctx)
	tx := sess.Model(cond).Where(cond).Updates(data)
	if tx.Error != nil {
		kklog.Error("更新数据err: ", getStructName(data), tx.Error.Error())
		return 0, tx.Error
	}
	return tx.RowsAffected, nil
}

// UpdateAllColsCtx 带上下文的 UpdateAllCols。
func UpdateAllColsCtx[T any](ctx context.Context, dbEng *gormeng.DbEngine, id interface{}, data *T) (int64, error) {
	if e := paramsCheck(dbEng, data); e != nil {
		return 0, e
	}
	sess := sessionWithContext(dbEng, ctx)
	modelSess := sess.Model(data)
	pkCol := getPrimaryKeyColumn(modelSess, data)
	tx := modelSess.Where(pkCol+" = ?", id).Select("*").Updates(data)
	if tx.Error != nil {
		kklog.Error("全量更新数据err: ", getStructName(data), tx.Error.Error())
		return 0, tx.Error
	}
	return tx.RowsAffected, nil
}

// UpdateColsCtx 带上下文的 UpdateCols。
func UpdateColsCtx[T any](ctx context.Context, dbEng *gormeng.DbEngine, cond *T, data *T, cols ...string) (int64, error) {
	if e := paramsCheck(dbEng, cond); e != nil {
		return 0, e
	}
	if isNil(data) {
		return 0, errNilBean
	}
	if len(cols) == 0 {
		return 0, nil
	}
	sess := sessionWithContext(dbEng, ctx)
	tx := sess.Model(cond).Where(cond).Select(cols).Updates(data)
	if tx.Error != nil {
		kklog.Error("更新指定列数据err: ", getStructName(data), tx.Error.Error())
		return 0, tx.Error
	}
	return tx.RowsAffected, nil
}
