package fbspb

import (
	"os"
	"path/filepath"
	"strings"
)

// FBSFileToProto 将 .fbs 文件转为 .proto 文件，输出到同目录，仅替换扩展名
func FBSFileToProto(fbsPath string) (string, error) {
	data, err := os.ReadFile(fbsPath)
	if err != nil {
		return "", err
	}
	sc, err := ParseFBS(strings.NewReader(string(data)))
	if err != nil {
		return "", err
	}
	protoPath := strings.TrimSuffix(fbsPath, ".fbs") + ".proto"
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

// ProtoFileToFBS 将 .proto 文件转为 .fbs 文件，输出到同目录
func ProtoFileToFBS(protoPath string) (string, error) {
	data, err := os.ReadFile(protoPath)
	if err != nil {
		return "", err
	}
	sc, err := ParseProto(strings.NewReader(string(data)))
	if err != nil {
		return "", err
	}
	fbsPath := strings.TrimSuffix(protoPath, ".proto") + ".fbs"
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
