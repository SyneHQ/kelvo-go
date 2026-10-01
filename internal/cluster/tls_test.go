package cluster

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func tlsFiles(t *testing.T, uri string, ca *x509.Certificate, key *rsa.PrivateKey) (TLSConfig, *x509.Certificate, *rsa.PrivateKey) {
	t.Helper()
	d := t.TempDir()
	if ca == nil {
		key, _ = rsa.GenerateKey(rand.Reader, 2048)
		ca = &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ca"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), BasicConstraintsValid: true, IsCA: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
		b, _ := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
		ca, _ = x509.ParseCertificate(b)
		os.WriteFile(filepath.Join(d, "ca.pem"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: b}), 0600)
	}
	os.WriteFile(filepath.Join(d, "ca.pem"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Raw}), 0600)
	k, _ := rsa.GenerateKey(rand.Reader, 2048)
	u, _ := url.Parse(uri)
	leaf := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), DNSNames: []string{"gateway.test"}, URIs: []*url.URL{u}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	b, _ := x509.CreateCertificate(rand.Reader, leaf, ca, &k.PublicKey, key)
	os.WriteFile(filepath.Join(d, "cert.pem"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: b}), 0600)
	os.WriteFile(filepath.Join(d, "key.pem"), pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)}), 0600)
	return TLSConfig{CAFile: filepath.Join(d, "ca.pem"), CertFile: filepath.Join(d, "cert.pem"), KeyFile: filepath.Join(d, "key.pem")}, ca, key
}
func TestMTLSWorkerIdentity(t *testing.T) {
	gw, ca, key := tlsFiles(t, GatewayIdentity, nil, nil)
	worker, _, _ := tlsFiles(t, WorkerIdentity("a", "w"), ca, key)
	srvCfg, e := BuildServerTLS(worker, WorkerIdentity("a", "w"), GatewayIdentity)
	if e != nil {
		t.Fatal(e)
	}
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	s.TLS = srvCfg
	s.StartTLS()
	defer s.Close()
	client, e := BuildClientTLS(gw, WorkerIdentity("a", "w"))
	if e != nil {
		t.Fatal(e)
	}
	client.ServerName = "gateway.test"
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = client
	r, e := (&http.Client{Transport: tr}).Get(s.URL)
	if e != nil || r.StatusCode != 204 {
		t.Fatalf("good mtls: %v", e)
	}
	r.Body.Close()
	tr.CloseIdleConnections()
	bad, e := BuildClientTLS(gw, WorkerIdentity("b", "w"))
	if e != nil {
		t.Fatal(e)
	}
	bad.ServerName = "gateway.test"
	tr = http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = bad
	if _, e = (&http.Client{Transport: tr}).Get(s.URL); e == nil {
		t.Fatal("wrong worker URI accepted")
	}
	legacy := client.Clone()
	legacy.MinVersion = tls.VersionTLS12
	legacy.MaxVersion = tls.VersionTLS12
	tr = http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = legacy
	if _, e = (&http.Client{Transport: tr}).Get(s.URL); e == nil {
		t.Fatal("TLS 1.2 accepted")
	}
}
