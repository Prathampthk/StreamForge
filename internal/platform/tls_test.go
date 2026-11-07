package platform

import (
	"crypto/tls"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTLSRejectsUntrustedServerAndAcceptsConfiguredCA(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") }))
	defer server.Close()
	roots, err := TLSConfig("")
	if err != nil {
		t.Fatal(err)
	}
	if roots.InsecureSkipVerify || roots.MinVersion < tls.VersionTLS12 {
		t.Fatal("unsafe TLS defaults")
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: roots}}
	if r, err := client.Get(server.URL); err == nil {
		r.Body.Close()
		t.Fatal("untrusted certificate accepted")
	}
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	roots, err = TLSConfig(string(ca))
	if err != nil {
		t.Fatal(err)
	}
	client = &http.Client{Transport: &http.Transport{TLSClientConfig: roots}}
	r, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	if _, err := TLSConfig("bad PEM"); err == nil {
		t.Fatal("malformed CA accepted")
	}
}
