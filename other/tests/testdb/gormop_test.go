package testdb

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/vvisun/kkdg/storage/kkdb"
	"github.com/vvisun/kkdg/storage/kkdb/gormeng"
	"github.com/vvisun/kkdg/storage/kkdb/gormop"
	"gorm.io/gorm"
)

var testDbEng *gormeng.DbEngine

func TestMain(m *testing.M) {
	opt := kkdb.DefaultDBOption()
	opt.Dsn = "root:LIKEsql123@tcp(127.0.0.1:3306)/ddqp?charset=utf8mb4"
	eng := gormeng.NewDbEngine(opt)
	if !eng.StartUp() {
		os.Exit(1)
	}
	if err := eng.SyncTables(&User{}); err != nil {
		_ = eng.Close()
		os.Exit(1)
	}
	testDbEng = eng
	code := m.Run()
	_ = eng.Close()
	os.Exit(code)
}

// setupGormTest 返回默认 DSN 的真实 MySQL DbEngine，TestMain 中已 StartUp 并 SyncTables。
func setupGormTest(t *testing.T) *gormeng.DbEngine {
	t.Helper()
	if testDbEng == nil {
		t.Fatal("db engine not started")
	}
	testDbEng.DropTables(&User{})
	testDbEng.SyncTables(&User{})
	return testDbEng
}

func TestGormop_Insert_GetByID(t *testing.T) {
	eng := setupGormTest(t)

	u := &User{
		Role:         RoleTypeNormal,
		Status:       StatusNormal,
		RegisterTime: 1234567890,
		PlatType:     1,
		PlatId:       "plat_001",
		NickName:     "test_user",
	}
	n, err := gormop.Insert(eng, u)
	if err != nil {
		t.Fatal("Insert: ", err)
	}
	if n != 1 {
		t.Errorf("Insert RowsAffected = %d, want 1", n)
	}
	if u.Uid <= 0 {
		t.Errorf("Insert did not set Uid, got %d", u.Uid)
	}

	got, err := gormop.GetByID(eng, &User{}, u.Uid)
	if err != nil {
		t.Fatal("GetByID: ", err)
	}
	if got == nil {
		t.Fatal("GetByID: got nil, want record")
	}
	if got.Uid != u.Uid || got.NickName != u.NickName {
		t.Errorf("GetByID got Uid=%d NickName=%s, want Uid=%d NickName=%s", got.Uid, got.NickName, u.Uid, u.NickName)
	}
}

func TestGormop_GetOne_NotFound(t *testing.T) {
	eng := setupGormTest(t)

	cond := &User{Uid: 99999}
	got, err := gormop.GetOne(eng, cond)
	if err != nil {
		t.Fatal("GetOne: ", err)
	}
	if got != nil {
		t.Errorf("GetOne(不存在) got non-nil, want nil")
	}
}

func TestGormop_GetList(t *testing.T) {
	eng := setupGormTest(t)

	_, _ = gormop.Insert(eng, &User{Role: RoleTypeNormal, Status: StatusNormal, RegisterTime: 1, PlatType: 1, PlatId: "p1", NickName: "u1"})
	_, _ = gormop.Insert(eng, &User{Role: RoleTypeNormal, Status: StatusNormal, RegisterTime: 2, PlatType: 1, PlatId: "p2", NickName: "u2"})

	list, err := gormop.GetList(eng, &User{})
	if err != nil {
		t.Fatal("GetList: ", err)
	}
	if len(list) < 2 {
		t.Errorf("GetList len = %d, want >= 2", len(list))
	}
}

func TestGormop_Update_Delete(t *testing.T) {
	eng := setupGormTest(t)

	u := &User{
		Role:         RoleTypeNormal,
		Status:       StatusNormal,
		RegisterTime: 100,
		PlatType:     1,
		PlatId:       "update_plat",
		NickName:     "before",
	}
	_, err := gormop.Insert(eng, u)
	if err != nil {
		t.Fatal("Insert: ", err)
	}

	u.NickName = "after"
	n, err := gormop.Update(eng, &User{Uid: u.Uid}, u)
	if err != nil {
		t.Fatal("Update: ", err)
	}
	if n != 1 {
		t.Errorf("Update RowsAffected = %d, want 1", n)
	}

	got, _ := gormop.GetByID(eng, &User{}, u.Uid)
	if got != nil && got.NickName != "after" {
		t.Errorf("Update: GetByID NickName = %s, want after", got.NickName)
	}

	n, err = gormop.Delete(eng, &User{Uid: u.Uid})
	if err != nil {
		t.Fatal("Delete: ", err)
	}
	if n != 1 {
		t.Errorf("Delete RowsAffected = %d, want 1", n)
	}
	got2, _ := gormop.GetByID(eng, &User{}, u.Uid)
	if got2 != nil {
		t.Errorf("Delete: record still exists")
	}
}

func TestGormop_UpdateCols(t *testing.T) {
	eng := setupGormTest(t)

	u := &User{
		Role:         RoleTypeNormal,
		Status:       StatusNormal,
		RegisterTime: 200,
		PlatType:     1,
		PlatId:       "cols_plat",
		NickName:     "orig",
		HeadUrl:      "http://old",
	}
	_, err := gormop.Insert(eng, u)
	if err != nil {
		t.Fatal("Insert: ", err)
	}

	cond := &User{Uid: u.Uid}
	upd := &User{Uid: u.Uid, NickName: "updated_name"}
	n, err := gormop.UpdateCols(eng, cond, upd, "nick_name")
	if err != nil {
		t.Fatal("UpdateCols: ", err)
	}
	if n != 1 {
		t.Errorf("UpdateCols RowsAffected = %d, want 1", n)
	}

	got, _ := gormop.GetByID(eng, &User{}, u.Uid)
	if got == nil {
		t.Fatal("GetByID after UpdateCols: nil")
	}
	if got.NickName != "updated_name" {
		t.Errorf("UpdateCols NickName = %s, want updated_name", got.NickName)
	}
	if got.HeadUrl != "http://old" {
		t.Errorf("UpdateCols should not change HeadUrl, got %s", got.HeadUrl)
	}
}

func TestGormop_InsertMulty(t *testing.T) {
	eng := setupGormTest(t)

	eng.DropTables(&User{})
	eng.SyncTables(&User{})

	beans := []*User{
		{Role: RoleTypeNormal, Status: StatusNormal, RegisterTime: 1, PlatType: 1, PlatId: "m1", NickName: "multi1"},
		{Role: RoleTypeNormal, Status: StatusNormal, RegisterTime: 2, PlatType: 1, PlatId: "m2", NickName: "multi2"},
	}
	n, err := gormop.InsertMulty(eng, beans)
	if err != nil {
		t.Fatal("InsertMulty: ", err)
	}
	if n != 2 {
		t.Errorf("InsertMulty RowsAffected = %d, want 2", n)
	}
	if beans[0].Uid <= 0 || beans[1].Uid <= 0 {
		t.Errorf("InsertMulty did not set Uid")
	}
}

func TestGormop_Transaction(t *testing.T) {
	eng := setupGormTest(t)

	err := eng.Transaction(func(tx *gorm.DB) error {
		u1 := &User{
			Role:         RoleTypeNormal,
			Status:       StatusNormal,
			RegisterTime: 1,
			PlatType:     1,
			PlatId:       "tx_plat_1",
			NickName:     "tx_nick_1",
		}
		if _, e := gormop.TxInsert(tx, u1); e != nil {
			return e
		}
		u2 := &User{
			Role:         RoleTypeNormal,
			Status:       StatusNormal,
			RegisterTime: 2,
			PlatType:     1,
			PlatId:       "tx_plat_2",
			NickName:     "tx_nick_2",
		}
		_, e := gormop.TxInsert(tx, u2)
		return e
	})
	if err != nil {
		t.Fatal("Transaction: ", err)
	}

	// 事务提交后应能查到
	list, err := gormop.GetList(eng, &User{PlatId: "tx_plat_1"})
	if err != nil {
		t.Fatal("GetList: ", err)
	}
	if len(list) != 1 || list[0].NickName != "tx_nick_1" {
		t.Errorf("after transaction: got %v", list)
	}
}

func TestGormop_TransactionRollback(t *testing.T) {
	eng := setupGormTest(t)

	_ = eng.Transaction(func(tx *gorm.DB) error {
		u := &User{
			Role:         RoleTypeNormal,
			Status:       StatusNormal,
			RegisterTime: 1,
			PlatType:     1,
			PlatId:       "tx_rollback_plat",
			NickName:     "tx_rollback_nick",
		}
		if _, e := gormop.TxInsert(tx, u); e != nil {
			return e
		}
		// 返回错误触发回滚
		return fmt.Errorf("intentional rollback")
	})

	// 回滚后不应存在该条
	got, err := gormop.GetOne(eng, &User{PlatId: "tx_rollback_plat"})
	if err != nil {
		t.Fatal("GetOne: ", err)
	}
	if got != nil {
		t.Errorf("TransactionRollback: record should not exist, got %v", got)
	}
}

// TestGormop_UpdateAllCols 验证按主键全量更新（含零值），主键列为 uid。
func TestGormop_UpdateAllCols(t *testing.T) {
	eng := setupGormTest(t)

	u := &User{
		Role:         RoleTypeNormal,
		Status:       StatusNormal,
		RegisterTime: 100,
		PlatType:     1,
		PlatId:       "allcols_plat",
		NickName:     "before",
		HeadUrl:      "http://before",
		Exp:          10,
		Grade:        1,
	}
	_, err := gormop.Insert(eng, u)
	if err != nil {
		t.Fatal("Insert: ", err)
	}

	// 全量更新为含零值的新 data（主键为 uid，由 Schema 解析）
	data := &User{
		Uid:            u.Uid,
		Role:           RoleTypeNormal,
		Status:         StatusNormal,
		RegisterTime:   200,
		LastLogoutTime: 0,
		Exp:            0,
		Grade:          0,
		AvatarId:       0,
		HeadFrame:      0,
		ShowAreaId:     0,
		PlatType:       1,
		PlatId:         "allcols_plat",
		NickName:       "after_all",
		HeadUrl:        "",
	}
	n, err := gormop.UpdateAllCols(eng, u.Uid, data)
	if err != nil {
		t.Fatal("UpdateAllCols: ", err)
	}
	if n != 1 {
		t.Errorf("UpdateAllCols RowsAffected = %d, want 1", n)
	}

	got, err := gormop.GetByID(eng, &User{}, u.Uid)
	if err != nil {
		t.Fatal("GetByID: ", err)
	}
	if got == nil {
		t.Fatal("GetByID: nil")
	}
	if got.NickName != "after_all" || got.HeadUrl != "" || got.Exp != 0 || got.Grade != 0 {
		t.Errorf("UpdateAllCols: got NickName=%s HeadUrl=%q Exp=%d Grade=%d", got.NickName, got.HeadUrl, got.Exp, got.Grade)
	}
}

// TestGormop_GetList_WithCond 条件查询列表。
func TestGormop_GetList_WithCond(t *testing.T) {
	eng := setupGormTest(t)

	_, _ = gormop.Insert(eng, &User{Role: RoleTypeNormal, Status: StatusNormal, RegisterTime: 1, PlatType: 1, PlatId: "cond_a", NickName: "na"})
	_, _ = gormop.Insert(eng, &User{Role: RoleTypeNormal, Status: StatusNormal, RegisterTime: 2, PlatType: 1, PlatId: "cond_b", NickName: "nb"})
	_, _ = gormop.Insert(eng, &User{Role: RoleTypeNormal, Status: StatusNormal, RegisterTime: 3, PlatType: 2, PlatId: "cond_c", NickName: "nc"})

	list, err := gormop.GetList(eng, &User{PlatType: 1})
	if err != nil {
		t.Fatal("GetList: ", err)
	}
	if len(list) != 2 {
		t.Errorf("GetList(PlatType=1) len = %d, want 2", len(list))
	}
}

// TestGormop_Ctx 带 context 的接口（用 Background 做冒烟测试）。
func TestGormop_Ctx(t *testing.T) {
	eng := setupGormTest(t)
	ctx := context.Background()

	u := &User{
		Role:         RoleTypeNormal,
		Status:       StatusNormal,
		RegisterTime: 1,
		PlatType:     1,
		PlatId:       "ctx_plat",
		NickName:     "ctx_nick",
	}
	n, err := gormop.InsertCtx(ctx, eng, u)
	if err != nil {
		t.Fatal("InsertCtx: ", err)
	}
	if n != 1 || u.Uid <= 0 {
		t.Errorf("InsertCtx: n=%d uid=%d", n, u.Uid)
	}

	got, err := gormop.GetByIDCtx(ctx, eng, &User{}, u.Uid)
	if err != nil {
		t.Fatal("GetByIDCtx: ", err)
	}
	if got == nil || got.NickName != "ctx_nick" {
		t.Errorf("GetByIDCtx: got %v", got)
	}

	gotOne, err := gormop.GetOneCtx(ctx, eng, &User{Uid: u.Uid})
	if err != nil {
		t.Fatal("GetOneCtx: ", err)
	}
	if gotOne == nil || gotOne.NickName != "ctx_nick" {
		t.Errorf("GetOneCtx: got %v", gotOne)
	}

	list, err := gormop.GetListCtx(ctx, eng, &User{PlatId: "ctx_plat"})
	if err != nil {
		t.Fatal("GetListCtx: ", err)
	}
	if len(list) != 1 {
		t.Errorf("GetListCtx: len = %d", len(list))
	}
}

// TestGormop_TransactionWithContext 带 context 的事务提交。
func TestGormop_TransactionWithContext(t *testing.T) {
	eng := setupGormTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := eng.TransactionWithContext(ctx, func(tx *gorm.DB) error {
		u := &User{
			Role:         RoleTypeNormal,
			Status:       StatusNormal,
			RegisterTime: 1,
			PlatType:     1,
			PlatId:       "txctx_plat",
			NickName:     "txctx_nick",
		}
		_, e := gormop.TxInsert(tx, u)
		return e
	})
	if err != nil {
		t.Fatal("TransactionWithContext: ", err)
	}

	got, _ := gormop.GetOne(eng, &User{PlatId: "txctx_plat"})
	if got == nil || got.NickName != "txctx_nick" {
		t.Errorf("after TransactionWithContext: got %v", got)
	}
}

// TestGormop_NilBean_ReturnsError 参数校验：nil bean 应返回错误而非 panic。
func TestGormop_NilBean_ReturnsError(t *testing.T) {
	eng := setupGormTest(t)

	_, err := gormop.Insert(eng, (*User)(nil))
	if err == nil {
		t.Error("Insert(nil bean) want error, got nil")
	}

	_, err = gormop.GetByID(eng, (*User)(nil), int64(1))
	if err == nil {
		t.Error("GetByID(nil bean) want error, got nil")
	}

	_, err = gormop.GetOne(eng, (*User)(nil))
	if err == nil {
		t.Error("GetOne(nil bean) want error, got nil")
	}
}

// TestGormop_Transaction_TxGetAndUpdate 事务内 TxGetByID + TxUpdate。
func TestGormop_Transaction_TxGetAndUpdate(t *testing.T) {
	eng := setupGormTest(t)

	var uid int64
	err := eng.Transaction(func(tx *gorm.DB) error {
		u := &User{
			Role:         RoleTypeNormal,
			Status:       StatusNormal,
			RegisterTime: 1,
			PlatType:     1,
			PlatId:       "txget_plat",
			NickName:     "orig",
		}
		if _, e := gormop.TxInsert(tx, u); e != nil {
			return e
		}
		uid = u.Uid
		got, e := gormop.TxGetByID(tx, &User{}, uid)
		if e != nil {
			return e
		}
		if got == nil || got.NickName != "orig" {
			return fmt.Errorf("TxGetByID: got %v", got)
		}
		got.NickName = "updated_in_tx"
		_, e = gormop.TxUpdate(tx, &User{Uid: uid}, got)
		return e
	})
	if err != nil {
		t.Fatal("Transaction: ", err)
	}

	got, _ := gormop.GetByID(eng, &User{}, uid)
	if got == nil || got.NickName != "updated_in_tx" {
		t.Errorf("after tx Update: got %v", got)
	}
}
