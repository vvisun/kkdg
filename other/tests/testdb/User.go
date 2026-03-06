package testdb

const (
	RoleTypeNormal int32 = 0  //db.User.Role 普通玩家
	RoleTypeRobot  int32 = 1  //db.User.Role 机器人玩家
	RoleTypeAdmin  int32 = 2  //db.User.Role 管理员账号
	RoleTypeNpc    int32 = 99 //db.User.Role NPC不存入数据库
)

const (
	StatusNormal  int32 = 0 //db.User.Status 正常
	StatusFreezed int32 = 1 //db.User.Status 冻结
	StatusBound   int32 = 2 //db.User.Status 绑定
	StatusDeleted int32 = 3 //db.User.Status 删除
)

func IsValidRoleType(roleType int32) bool {
	return roleType == RoleTypeNormal || roleType == RoleTypeRobot || roleType == RoleTypeAdmin || roleType == RoleTypeNpc
}

func IsValidRoleStatus(roleStatus int32) bool {
	return roleStatus >= StatusNormal && roleStatus <= StatusDeleted
}

// 用户表（GORM 模型，表名为 user）
type User struct {
	Uid            int64  `gorm:"column:uid;primaryKey;autoIncrement"`
	Role           int32  `gorm:"not null"`           // 角色类型 RoleTypeNormal ...
	Status         int32  `gorm:"not null"`           // 角色状态 StatusNormal ...
	RegisterTime   int64  `gorm:"not null"`           // 玩家注册时间
	LastLogoutTime int64  `gorm:"not null;default:0"` // 最后一次离线时间
	Exp            int64  `gorm:"not null;default:0"` // 经验
	Grade          int32  `gorm:"not null;default:0"` // 等级
	AvatarId       int32  `gorm:"not null"`          // 形象ID
	HeadFrame      int32  `gorm:"not null;default:0"` // 头像框ID
	ShowAreaId     int32  `gorm:"not null"`          // 区服ID
	PlatType       int32  `gorm:"not null"`          // 平台类型 guest, account, wx, tapTap, facebook, ...
	PlatId         string `gorm:"uniqueIndex;size:128;default:''"` // 平台 openId，账号密码登录时用的账号ID作为平台ID
	NickName       string `gorm:"uniqueIndex;size:64;default:''"`  // 昵称
	HeadUrl        string `gorm:"size:256;default:''"`             // 头像
}

// TableName 指定表名，与迁移前 xorm 默认表名一致。
func (User) TableName() string {
	return "user"
}

func (slf *User) GetUid() int64 {
	return slf.Uid
}
