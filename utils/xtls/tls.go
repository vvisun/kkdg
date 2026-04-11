package xtls

import (
	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/vvisun/kkdg/kkerrors"
)

func MakeRedisTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	// Load client cert
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}

	// Load CA cert
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()

	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, kkerrors.ErrInvalidCertFile
	}

	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}, nil
}

func MakeTCPClientTLSConfig(caFile string, serverName string) (*tls.Config, error) {
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()

	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, kkerrors.ErrInvalidCertFile
	}

	return &tls.Config{ServerName: serverName, RootCAs: caCertPool}, nil
}
