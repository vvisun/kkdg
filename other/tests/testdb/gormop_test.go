package testdb

import (
	"fmt"
	"os"
	"testing"

	"github.com/vvisun/kkdg/storage/kkdb"
	"github.com/vvisun/kkdg/storage/kkdb/gormeng"
	"github.com/vvisun/kkdg/storage/kkdb/gormop"
	"gorm.io/gorm"
)

var testDbEng *gormeng.DbEngine

func TestMain(m *testing.M) {
	opt := kkdb.DefaultDBOption()
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
