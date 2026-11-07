package platform

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
)

// TLSConfig preserves hostname verification and never permits insecure bypasses.
func TLSConfig(caPEM string) (*tls.Config, error) {
	roots, err := x509.SystemCertPool()
	if err != nil {
		roots = x509.NewCertPool()
	}
	if caPEM != "" && !roots.AppendCertsFromPEM([]byte(caPEM)) {
		return nil, errors.New("invalid CA certificate PEM")
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, nil
}
