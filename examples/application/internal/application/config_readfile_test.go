// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package application

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestReadFileRequiresBoundedRegularPrivateFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "key")
	if err := os.WriteFile(path, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	if raw, err := readFile(path, 4, true); err != nil || string(raw) != "test" {
		t.Fatal("valid exact-limit file rejected")
	}
	for _, limit := range []int64{-1, 0, 3, 1 << 21} {
		if _, err := readFile(path, limit, true); !errors.Is(err, ErrConfiguration) {
			t.Fatal("invalid size bound accepted")
		}
	}
	if err := os.Chmod(path, 0640); err != nil {
		t.Fatal(err)
	}
	if _, err := readFile(path, 4, true); !errors.Is(err, ErrConfiguration) {
		t.Fatal("group-readable private file accepted")
	}
	if raw, err := readFile(path, 4, false); err != nil || string(raw) != "test" {
		t.Fatal("public file mode rejected")
	}
	if _, err := readFile(dir, 100, false); !errors.Is(err, ErrConfiguration) {
		t.Fatal("directory accepted")
	}
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readFile(path, 4, false); !errors.Is(err, ErrConfiguration) {
		t.Fatal("empty file accepted")
	}
}
func TestReadFileRejectsFIFOWithoutWaitingForWriter(t *testing.T) {
	fifo := filepath.Join(t.TempDir(), "fifo")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := readFile(fifo, 128, true); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrConfiguration) {
			t.Fatal("FIFO accepted")
		}
	case <-time.After(time.Second):
		// Release a broken blocking implementation before failing the test.
		fd, err := syscall.Open(fifo, syscall.O_RDWR|syscall.O_NONBLOCK, 0600)
		if err == nil {
			defer syscall.Close(fd)
		}
		t.Fatal("configuration read blocked on FIFO")
	}
}
func TestTLSKeyPairUsesBoundedPrivateFileReader(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	rawKey, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	files := TLSFiles{CertFile: filepath.Join(dir, "cert.pem"), KeyFile: filepath.Join(dir, "key.pem")}
	if err := os.WriteFile(files.CertFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(files.KeyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: rawKey}), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := keyPair(files); err != nil {
		t.Fatal("valid key pair rejected")
	}
	if err := os.Chmod(files.KeyFile, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := keyPair(files); !errors.Is(err, ErrConfiguration) {
		t.Fatal("publicly readable TLS key accepted")
	}
}
