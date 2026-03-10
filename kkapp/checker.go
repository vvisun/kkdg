package kkapp

import (
	"strings"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
)

const (
	// 节点ID最大长度
	maxNodeIDLength int = 16
	// 节点类型最大长度
	maxNodeTypeLength int = 16
)

const (
	// nodeId与actorKey的分隔符
	ActorKeySep_NodeAndActor = "/"

	// actorKey分隔符，用于分隔层级，例如 "game_player"、"gate_router"
	ActorKeySep_ParantAndChild = "_"
)

// checkNodeID 检查节点ID是否有效
// 只允许【英文字母、数字】组合，例如 "game1"、"game2"
func checkNodeID(nodeId string) error {
	if len(nodeId) < 1 {
		kklog.Errorf("invalid node id: %s", nodeId)
		return kkerrors.ErrInvalidNodeID
	}
	if len(nodeId) > maxNodeIDLength {
		kklog.Errorf("node id is too long: %s", nodeId)
		return kkerrors.ErrInvalidNodeID
	}
	if strings.Contains(nodeId, ActorKeySep_NodeAndActor) {
		kklog.Errorf("node id contains actor key separator: %s", nodeId)
		return kkerrors.ErrInvalidNodeID
	}
	return nil
}

// checkNodeType 检查节点类型是否有效
// 只允许英文字母，例如 "logic"、"chat"、"gate"
func checkNodeType(nodeType string) error {
	if len(nodeType) < 1 {
		kklog.Errorf("invalid node type: %s", nodeType)
		return kkerrors.ErrInvalidNodeType
	}
	if len(nodeType) > maxNodeTypeLength {
		kklog.Errorf("node type is too long: %s", nodeType)
		return kkerrors.ErrInvalidNodeType
	}
	if strings.Contains(nodeType, ActorKeySep_NodeAndActor) {
		kklog.Errorf("node type contains actor key separator: %s", nodeType)
		return kkerrors.ErrInvalidNodeType
	}
	return nil
}

// 只允许【英文字母、数字】组合，例如 "game1"、"game2"
func IsValidActorNodeId(nodeId string) bool {
	if nodeId == "" {
		return true //允许空字符串，表示本地Actor
	}
	if strings.Contains(nodeId, ActorKeySep_NodeAndActor) {
		return false //不允许包含分隔符
	}
	return true
}

// 只允许【英文字母、数字、下划线("_")】组合，例如 "game_player"、"gate_router"
// 一般用_分隔层级，不过不强制要求，上层逻辑已经有规范拼接接口。
func IsValidActorKey(actorKey string) bool {
	if actorKey == "" {
		return false
	}
	if strings.Contains(actorKey, ActorKeySep_NodeAndActor) {
		return false
	}
	return true
}
