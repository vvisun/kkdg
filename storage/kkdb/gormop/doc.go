// Package gormop 提供基于 GORM 的通用 CRUD 封装，配合 storage/kkdb/gormeng 使用。
//
// # 非事务用法
//
// 传入 *gormeng.DbEngine，直接执行单次操作：
//
//	one, err := gormop.GetOne(eng, &User{Uid: 1})
//	n, err := gormop.Insert(eng, &User{...})
//	n, err := gormop.Update(eng, cond, data)
//	n, err := gormop.Delete(eng, &User{Uid: 1})
//
// # 事务用法
//
// 使用 gormeng.DbEngine.Transaction 或 TransactionWithContext，在回调内用 Tx*：
//
//	err := eng.Transaction(func(tx *gorm.DB) error {
//	    _, e := gormop.TxInsert(tx, &User{...})
//	    if e != nil { return e }
//	    _, e = gormop.TxUpdate(tx, cond, data)
//	    return e
//	})
//
// 需要超时/取消时用 TransactionWithContext(ctx, fn)。
//
// # 带上下文的单次操作
//
// 单次操作需要超时或传递 context 时，使用 *Ctx 系列：
//
//	one, err := gormop.GetOneCtx(ctx, eng, &User{Uid: 1})
//	n, err := gormop.InsertCtx(ctx, eng, bean)
//
// # 其他说明
//
//   - GetOne/GetByID：未找到时返回 (nil, nil)，不返回 error。因为找不到严格来说不算是错误，单纯就是没有这条数据而已。
//   - Update：只更新 data 的非零字段；零值需更新时用 UpdateCols 指定列或 UpdateAllCols（按主键全量更新）。
//   - UpdateAllCols/TxUpdateAllCols：主键列名从模型 GORM Schema 自动获取（如 uid、id），无需写死。
//   - InsertMulty/TxInsertMulty：单次最多 maxBatchInsertCount 条（默认 500）。
package gormop
