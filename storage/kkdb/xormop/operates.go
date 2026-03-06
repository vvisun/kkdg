package xormop

import (
	"errors"

	"github.com/vvisun/kkdg/storage/kkdb/xormeng"
	"github.com/vvisun/kkdg/utils/kklog"
)

var (
	errNilDbEngine = errors.New("DbEngine is nil")
	errNilBean     = errors.New("bean is nil")
	errInvalidBean = errors.New("bean is invalid")
	errNilInstance = errors.New("DbEngine instance is nil")
)

func paramsCheck(dbInst *xormeng.DbEngine, bean interface{}) error {
	if dbInst == nil {
		kklog.Error("dbengine is nil")
		return errNilDbEngine
	}
	if dbInst.GetInst() == nil {
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

// 获取一条数据
func GetOne[T any](dbEng *xormeng.DbEngine, bean *T) (*T, error) {
	if e := paramsCheck(dbEng, bean); e != nil {
		kklog.Error("获取数据err, 参数无效", bean)
		return nil, e
	}
	has, err := dbEng.GetInst().Get(bean)
	if err != nil {
		kklog.Error("获取数据err ", getStructName(bean), " ", err.Error())
		return nil, err
	}
	if !has {
		// kklog.Debug("没有该条数据", getStructName(bean), bean)
		return nil, nil
	}
	return bean, nil
}

// 获取多条数据
func GetList[T any](dbEng *xormeng.DbEngine, bean *T) ([]*T, error) {
	if e := paramsCheck(dbEng, bean); e != nil {
		kklog.Error("获取多条数据err, 参数无效", bean)
		return nil, e
	}
	arr := []*T{}
	err := dbEng.GetInst().Find(&arr, bean)
	if err != nil {
		kklog.Error("获取多条数据err", getStructName(bean), err.Error())
		return nil, err
	}
	return arr, nil
}

// 插入数据
func Insert[T any](dbEng *xormeng.DbEngine, bean *T) (int64, error) {
	if e := paramsCheck(dbEng, bean); e != nil {
		kklog.Error("插入数据err, 参数无效", bean)
		return 0, e
	}
	if !isPointer(bean) {
		kklog.Warn("插入数据: bean is not pointer")
	}
	result, err := dbEng.GetInst().Insert(bean)
	if err != nil {
		kklog.Error("插入数据err: ", getStructName(bean), err.Error())
		return 0, err
	}
	return result, nil
}

// 批量插入
func InsertMulty[T any](dbEng *xormeng.DbEngine, beans []*T) (int64, error) {
	if len(beans) == 0 {
		kklog.Error("批量插入err, 参数无效", beans)
		return 0, errInvalidBean
	}
	// 检查所有bean是否有效
	for i, bean := range beans {
		if e := paramsCheck(dbEng, bean); e != nil {
			kklog.Errorf("批量插入err, 参数无效, 索引: %d, 错误: %s", i, e.Error())
			return 0, e
		}
	}
	beanArr := make([]interface{}, len(beans))
	for i, bean := range beans {
		beanArr[i] = bean
	}
	sussCnt, err := dbEng.GetInst().Insert(beanArr...)
	if err != nil {
		kklog.Errorf("批量插入err, 结构名: %s, 成功数: %d / %d, 错误: %s", getStructName(beans[0]), sussCnt, len(beans), err.Error())
		return sussCnt, err
	}
	return sussCnt, nil
}

// 删除数据
func Delete[T any](dbEng *xormeng.DbEngine, bean *T) (int64, error) {
	if e := paramsCheck(dbEng, bean); e != nil {
		kklog.Error("删除数据err, 参数无效")
		return 0, e
	}
	result, err := dbEng.GetInst().Delete(bean)
	if err != nil {
		kklog.Error("删除数据err: ", getStructName(bean), err.Error())
		return 0, err
	}
	return result, nil
}

// 更新数据(注意，更新字段为0值时不能采用该方法)
func Update[T any](dbEng *xormeng.DbEngine, cond *T, data *T) (int64, error) {
	if e := paramsCheck(dbEng, cond); e != nil {
		kklog.Error("更新数据err, 参数无效")
		return 0, e
	}
	if isNil(data) {
		kklog.Error("更新数据err, bean is nil")
		return 0, errNilBean
	}
	if isDoublePointer(data) {
		kklog.Error("更新数据err, bean is double pointer")
		return 0, errInvalidBean
	}
	result, err := dbEng.GetInst().Update(data, cond)
	if err != nil {
		kklog.Error("更新数据err", getStructName(data), err.Error())
		return 0, err
	}
	return result, nil
}

// 全量更新数据
func UpdateAllCols[T any](dbEng *xormeng.DbEngine, id int64, data *T) (int64, error) {
	if e := paramsCheck(dbEng, data); e != nil {
		kklog.Error("全量更新数据err, 参数无效")
		return 0, e
	}
	result, err := dbEng.GetInst().ID(id).AllCols().Update(data)
	if err != nil {
		kklog.Error("全量更新数据err: ", getStructName(data), err.Error())
		return 0, err
	}
	return result, nil
}

// 更新指定列数据
func UpdateCols[T any](dbEng *xormeng.DbEngine, cond *T, data *T, cols ...string) (int64, error) {
	if e := paramsCheck(dbEng, cond); e != nil {
		kklog.Error("更新指定列数据err, 参数无效")
		return 0, e
	}
	if isNil(data) {
		kklog.Error("更新指定列数据err bean is nil")
		return 0, errNilBean
	}
	if isDoublePointer(data) {
		kklog.Error("更新指定列数据err bean is double pointer")
		return 0, errInvalidBean
	}
	if len(cols) == 0 {
		kklog.Error("更新指定列数据err, 参数无效")
		return 0, nil
	}
	result, err := dbEng.GetInst().Cols(cols...).Update(data, cond)
	if err != nil {
		kklog.Error("更新指定列数据err: ", getStructName(data), err.Error())
		return 0, err
	}
	return result, nil
}
