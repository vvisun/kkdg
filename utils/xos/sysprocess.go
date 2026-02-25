package xos

import (
	"os"
	"runtime"

	"github.com/shirou/gopsutil/process"
)

// num cpu
func NumCPU() int {
	n := runtime.NumCPU()
	if n < 1 {
		return 1
	}
	return n
}

func GetProcessNameByPID(pid int32) (string, error) {
	proc, err := process.NewProcess(pid)
	if err != nil {
		return "", err
	}

	processName, err := proc.Name()
	if err != nil {
		return "", err
	}

	return processName, nil
}

func GetMyProcessName() (string, error) {
	return GetProcessNameByPID(int32(os.Getpid()))
}
