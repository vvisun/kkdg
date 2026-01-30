package xnet_test

import (
	"fmt"
	"testing"

	net "github.com/vvisun/kkdg/utils/xnet"
)

func setTestResolvers() {
	// Avoid relying on real network in CI / sandbox.
	net.SetPublicIPResolver(customPublicIPResolver)
	net.SetPrivateIPResolver(customPrivateIPResolver)
}

func TestParseAddr(t *testing.T) {
	setTestResolvers()
	listenAddr, exposeAddr, err := net.ParseAddr("0.0.0.0:0", true)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(listenAddr, exposeAddr)
}

func TestInternalIP(t *testing.T) {
	setTestResolvers()
	ip, err := net.InternalIP()
	if err != nil {
		t.Fatal(err)
	}

	t.Log(ip)
}

func TestExternalIP(t *testing.T) {
	setTestResolvers()
	for range 3 {
		ip, err := net.ExternalIP()
		if err != nil {
			t.Fatal(err)
		}

		fmt.Println(ip)
	}
}

func TestPublicIP(t *testing.T) {
	setTestResolvers()
	if ip, err := net.PublicIP(); err != nil {
		t.Fatal(err)
	} else {
		t.Log(ip)
	}

	net.SetPublicIPResolver(customPublicIPResolver)

	if ip, err := net.PublicIP(); err != nil {
		t.Fatal(err)
	} else {
		t.Log(ip)
	}
}

func TestPrivateIP(t *testing.T) {
	setTestResolvers()
	if ip, err := net.PrivateIP(); err != nil {
		t.Fatal(err)
	} else {
		t.Log(ip)
	}

	net.SetPrivateIPResolver(customPrivateIPResolver)

	if ip, err := net.PrivateIP(); err != nil {
		t.Fatal(err)
	} else {
		t.Log(ip)
	}
}

func customPublicIPResolver() (string, error) {
	return "1.1.1.1", nil
}

func customPrivateIPResolver() (string, error) {
	return "192.168.1.1", nil
}
