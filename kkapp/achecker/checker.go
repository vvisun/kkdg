package achecker

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

// CheckNodeID 检查节点ID是否有效
// 只允许【英文字母，数字，下划线("_")，中划线("-")】组合，例如 "game_player"、"gate_router-1001"
func CheckNodeID(nodeId string) error {
	if len(nodeId) < 1 {
		kklog.Errorf("node id is empty: %s", nodeId)
		return kkerrors.ErrAppInvalidNodeID
	}
	if len(nodeId) > maxNodeIDLength {
		kklog.Errorf("node id is too long: %s, max length is %d", nodeId, maxNodeIDLength)
		return kkerrors.ErrAppInvalidNodeID
	}
	if !isASCIIAlphaNumericUnderscoreHyphen(nodeId) {
		kklog.Errorf("node id must contain only letters, digits, underscore, or hyphen: %s", nodeId)
		return kkerrors.ErrAppInvalidNodeID
	}
	return nil
}

// CheckNodeType 检查节点类型是否有效
// 只允许英文字母，例如 "logic"、"chat"、"gate"。
func CheckNodeType(nodeType string) error {
	if len(nodeType) < 1 {
		kklog.Errorf("node type is empty: %s", nodeType)
		return kkerrors.ErrAppInvalidNodeType
	}
	if len(nodeType) > maxNodeTypeLength {
		kklog.Errorf("node type is too long: %s, max length is %d", nodeType, maxNodeTypeLength)
		return kkerrors.ErrAppInvalidNodeType
	}
	if !isASCIIAlpha(nodeType) {
		kklog.Errorf("node type must contain only letters: %s", nodeType)
		return kkerrors.ErrAppInvalidNodeType
	}
	return nil
}

// 只允许【英文字母，数字，下划线("_")，中划线("-")】组合，例如 "game_player"、"gate_router-1001"
func IsValidActorNodeId(nodeId string) bool {
	return isASCIIAlphaNumericUnderscoreHyphen(nodeId)
}

// 只允许【英文字母，数字，下划线("_")，中划线("-")】组合，例如 "game_player"、"gate_router-1001"
// 一般用_分隔层级。
func IsValidActorKey(actorKey string) bool {
	return isASCIIAlphaNumericUnderscoreHyphen(actorKey)
}

//--------------------------------------------------

func isASCIIAlphaNumericUnderscoreHyphen(s string) bool {
	if s == "" {
		return false
	}
	for _, ch := range s {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '-' {
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
