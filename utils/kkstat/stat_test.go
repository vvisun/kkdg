package kkstat_test

import (
	"testing"

	"github.com/vvisun/kkdg/utils/kkstat"
)

func TestStat(t *testing.T) {
	fi, err := kkstat.Stat("stat_linux.go")
	if err != nil {
		t.Fatal(err)
	}

	t.Log(fi.CreateTime())
}
