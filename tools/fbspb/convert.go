package fbspb

import (
	"os"
	"path/filepath"
	"strings"
)

// flatProtoDomains 与 proto/ptoflats/<name> 下的 .fbs 一一对应，.proto 仍在 proto/<name>/。
var flatProtoDomains = map[string]struct{}{
	"pbgate": {}, "pbcluster": {}, "pbrpc": {},
}

func protoPathForFBSOutput(fbsPath string) string {
	dir := filepath.Clean(filepath.Dir(fbsPath))
	parent := filepath.Dir(dir)
	grand := filepath.Dir(parent)
	leaf := filepath.Base(dir)
	if filepath.Base(parent) == "ptoflats" {
		if _, ok := flatProtoDomains[leaf]; ok {
			protoDir := filepath.Join(grand, leaf)
			base := strings.TrimSuffix(filepath.Base(fbsPath), ".fbs") + ".proto"
			return filepath.Join(protoDir, base)
		}
	}
	return strings.TrimSuffix(fbsPath, ".fbs") + ".proto"
}

func fbsPathForProtoOutput(protoPath string) string {
	dir := filepath.Clean(filepath.Dir(protoPath))
	leaf := filepath.Base(dir)
	if _, ok := flatProtoDomains[leaf]; ok {
		ptoflatsDir := filepath.Join(filepath.Dir(dir), "ptoflats", leaf)
		base := strings.TrimSuffix(filepath.Base(protoPath), ".proto") + ".fbs"
		return filepath.Join(ptoflatsDir, base)
	}
	return strings.TrimSuffix(protoPath, ".proto") + ".fbs"
}

// FBSFileToProto 将 .fbs 文件转为 .proto 文件；ptoflats 下各域的 schema 输出到 proto/<域>/，其余为同目录仅替换扩展名
func FBSFileToProto(fbsPath string) (string, error) {
	data, err := os.ReadFile(fbsPath)
	if err != nil {
		return "", err
	}
	sc, err := ParseFBS(strings.NewReader(string(data)))
	if err != nil {
		return "", err
	}
	protoPath := protoPathForFBSOutput(fbsPath)
	if err := os.MkdirAll(filepath.Dir(protoPath), 0o755); err != nil {
		return "", err
	}
	f, err := os.Create(protoPath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if err := FBS2Proto(sc, f); err != nil {
		os.Remove(protoPath)
		return "", err
	}
	return protoPath, nil
}

// ProtoFileToFBS 将 .proto 文件转为 .fbs 文件；pbgate/pbcluster/pbrpc 的 .proto 输出到 proto/ptoflats/<域>/，其余为同目录
func ProtoFileToFBS(protoPath string) (string, error) {
	data, err := os.ReadFile(protoPath)
	if err != nil {
		return "", err
	}
	sc, err := ParseProto(strings.NewReader(string(data)))
	if err != nil {
		return "", err
	}
	fbsPath := fbsPathForProtoOutput(protoPath)
	if err := os.MkdirAll(filepath.Dir(fbsPath), 0o755); err != nil {
		return "", err
	}
	f, err := os.Create(fbsPath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if err := Proto2FBS(sc, f); err != nil {
		os.Remove(fbsPath)
		return "", err
	}
	return fbsPath, nil
}

// ConvertDirFBS2Proto 递归将目录下所有 .fbs 转为 .proto
func ConvertDirFBS2Proto(dir string) ([]string, error) {
	var converted []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".fbs") {
			return nil
		}
		out, err := FBSFileToProto(path)
		if err != nil {
			return err
		}
		converted = append(converted, out)
		return nil
	})
	return converted, err
}

// ConvertDirProto2FBS 递归将目录下所有 .proto 转为 .fbs
func ConvertDirProto2FBS(dir string) ([]string, error) {
	var converted []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".proto") {
			return nil
		}
		out, err := ProtoFileToFBS(path)
		if err != nil {
			return err
		}
		converted = append(converted, out)
		return nil
	})
	return converted, err
}
