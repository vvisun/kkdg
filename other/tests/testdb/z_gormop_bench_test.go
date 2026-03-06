package testdb

import (
	"fmt"
	"testing"

	"github.com/vvisun/kkdg/storage/kkdb/gormeng"
	"github.com/vvisun/kkdg/storage/kkdb/gormop"
)

func benchEng(b *testing.B) *gormeng.DbEngine {
	b.Helper()
	if testDbEng == nil {
		b.Skip("db engine not started (run with TestMain)")
	}
	eng := testDbEng
	eng.DropTables(&User{})
	eng.SyncTables(&User{})
	return testDbEng
}

// BenchmarkGormop_Insert 单条插入
func BenchmarkGormop_Insert(b *testing.B) {
	eng := benchEng(b)
	if eng == nil {
		return
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		u := &User{
			Role:         RoleTypeNormal,
			Status:       StatusNormal,
			RegisterTime: int64(i),
			PlatType:     1,
			PlatId:       fmt.Sprintf("bench_plat_%d_%d", b.N, i),
			NickName:     fmt.Sprintf("bench_nick_%d_%d", b.N, i),
		}
		_, _ = gormop.Insert(eng, u)
	}
}

// BenchmarkGormop_GetByID 按主键查询
func BenchmarkGormop_GetByID(b *testing.B) {
	eng := benchEng(b)
	if eng == nil {
		return
	}
	u := &User{
		Role:         RoleTypeNormal,
		Status:       StatusNormal,
		RegisterTime: 1,
		PlatType:     1,
		PlatId:       fmt.Sprintf("bench_getbyid_plat_%d", b.N),
		NickName:     fmt.Sprintf("bench_getbyid_nick_%d", b.N),
	}
	_, err := gormop.Insert(eng, u)
	if err != nil {
		b.Fatal("setup Insert: ", err)
	}
	uid := u.Uid
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = gormop.GetByID(eng, &User{}, uid)
	}
}

// BenchmarkGormop_GetOne 条件查询一条
func BenchmarkGormop_GetOne(b *testing.B) {
	eng := benchEng(b)
	if eng == nil {
		return
	}
	u := &User{
		Role:         RoleTypeNormal,
		Status:       StatusNormal,
		RegisterTime: 1,
		PlatType:     1,
		PlatId:       fmt.Sprintf("bench_getone_plat_%d", b.N),
		NickName:     fmt.Sprintf("bench_getone_nick_%d", b.N),
	}
	_, err := gormop.Insert(eng, u)
	if err != nil {
		b.Fatal("setup Insert: ", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = gormop.GetOne(eng, &User{Uid: u.Uid})
	}
}

// BenchmarkGormop_GetList 条件查询列表（空条件，返回全部，数据量大时较慢）
func BenchmarkGormop_GetList(b *testing.B) {
	eng := benchEng(b)
	if eng == nil {
		return
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = gormop.GetList(eng, &User{})
	}
}

// BenchmarkGormop_Update 按条件更新
func BenchmarkGormop_Update(b *testing.B) {
	eng := benchEng(b)
	if eng == nil {
		return
	}
	u := &User{
		Role:         RoleTypeNormal,
		Status:       StatusNormal,
		RegisterTime: 1,
		PlatType:     1,
		PlatId:       fmt.Sprintf("bench_upd_plat_%d", b.N),
		NickName:     "before",
	}
	_, err := gormop.Insert(eng, u)
	if err != nil {
		b.Fatal("setup Insert: ", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		upd := &User{Uid: u.Uid, NickName: fmt.Sprintf("after_%d_%d", b.N, i)}
		_, _ = gormop.Update(eng, &User{Uid: u.Uid}, upd)
	}
}

// BenchmarkGormop_UpdateCols 按条件更新指定列
func BenchmarkGormop_UpdateCols(b *testing.B) {
	eng := benchEng(b)
	if eng == nil {
		return
	}
	u := &User{
		Role:         RoleTypeNormal,
		Status:       StatusNormal,
		RegisterTime: 1,
		PlatType:     1,
		PlatId:       fmt.Sprintf("bench_cols_plat_%d", b.N),
		NickName:     "orig",
	}
	_, err := gormop.Insert(eng, u)
	if err != nil {
		b.Fatal("setup Insert: ", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		upd := &User{Uid: u.Uid, NickName: fmt.Sprintf("col_%d_%d", b.N, i)}
		_, _ = gormop.UpdateCols(eng, &User{Uid: u.Uid}, upd, "nick_name")
	}
}

// BenchmarkGormop_Delete 按条件删除（每轮先插后删，避免表被清空影响其他 bench）
func BenchmarkGormop_Delete(b *testing.B) {
	eng := benchEng(b)
	if eng == nil {
		return
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		u := &User{
			Role:         RoleTypeNormal,
			Status:       StatusNormal,
			RegisterTime: int64(i),
			PlatType:     1,
			PlatId:       fmt.Sprintf("bench_del_plat_%d_%d", b.N, i),
			NickName:     fmt.Sprintf("bench_del_nick_%d_%d", b.N, i),
		}
		_, _ = gormop.Insert(eng, u)
		_, _ = gormop.Delete(eng, &User{Uid: u.Uid})
	}
}

const benchBatchSize = 10

// BenchmarkGormop_InsertMulty 批量插入（每批 benchBatchSize 条）
func BenchmarkGormop_InsertMulty(b *testing.B) {
	eng := benchEng(b)
	if eng == nil {
		return
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		beans := make([]*User, benchBatchSize)
		for j := 0; j < benchBatchSize; j++ {
			beans[j] = &User{
				Role:         RoleTypeNormal,
				Status:       StatusNormal,
				RegisterTime: int64(i*benchBatchSize + j),
				PlatType:     1,
				PlatId:       fmt.Sprintf("bench_multi_%d_%d_%d", b.N, i, j),
				NickName:     fmt.Sprintf("bench_multi_nick_%d_%d_%d", b.N, i, j),
			}
		}
		_, _ = gormop.InsertMulty(eng, beans)
	}
}
