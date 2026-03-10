package kkapp

import (
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

func isASCIIAlphaNumeric(s string) bool {
	if s == "" {
		return false
	}
	for _, ch := range s {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') {
			continue
		}
		return false
	}
	return true
}

func isASCIIAlpha(s string) bool {
	if s == "" {
		return false
	}
	for _, ch := range s {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') {
			continue
		}
		return false
	}
	return true
}

func isASCIIAlphaNumericUnderscore(s string) bool {
	if s == "" {
		return false
	}
	for _, ch := range s {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' {
			continue
		}
		return false
	}
	return true
}

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
	if !isASCIIAlphaNumeric(nodeId) {
		kklog.Errorf("node id must be alphanumeric: %s", nodeId)
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
	if !isASCIIAlpha(nodeType) {
		kklog.Errorf("node type must contain only letters: %s", nodeType)
		return kkerrors.ErrInvalidNodeType
	}
	return nil
}

// 只允许【英文字母、数字】组合，例如 "game1"、"game2"
func IsValidActorNodeId(nodeId string) bool {
	if nodeId == "" {
		return true //允许空字符串，表示本地Actor
	}
	return isASCIIAlphaNumeric(nodeId)
}

// 只允许【英文字母、数字、下划线("_")】组合，例如 "game_player"、"gate_router"
// 一般用_分隔层级，不过不强制要求，上层逻辑已经有规范拼接接口。
func IsValidActorKey(actorKey string) bool {
	return isASCIIAlphaNumericUnderscore(actorKey)
}
