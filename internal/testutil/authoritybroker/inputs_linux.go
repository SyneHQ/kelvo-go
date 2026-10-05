//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package authoritybroker

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

type release struct{ archive, binary, env string }

var releases = map[string]release{
	"2.14.7": {"e5c20b1cb2c0566b54c544312e91e011f9e130c5c80f16a14f4cf28ef30b8be2", "667bfa7d71ac190ab745e00bd1f9a037338b719b3293cda25bd44aef24b457fa", Env + "_ARCHIVE_2_14_7"},
	"2.15.0": {"5d2c51caca950333aba84911df7d377f826f3a59ec36061c6539105084f65c92", "387ce54e7a0a249d877a579ed641d715bab047d7e061bfa42b4d07f6a0401341", Env + "_ARCHIVE_2_15_0"},
}

func Require(t testing.TB) {
	t.Helper()
	if os.Getenv(Env) != "1" {
		t.Skip("set KELVO_TEST_GATEWAY_AUTHORITY=1 for pinned TLS broker acceptance")
	}
	if runtime.GOARCH != "amd64" {
		t.Fatal("pinned broker fixture requires Linux amd64")
	}
	raw, err := os.ReadFile("/proc/self/cgroup")
	if err != nil || len(raw) > 4096 {
		t.Fatal("cannot inspect fixture cgroup")
	}
	path := ""
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "0::/") {
			path = strings.TrimPrefix(line, "0::")
		}
	}
	if path == "" || strings.Contains(path, "..") {
		t.Fatal("fixture requires cgroup v2")
	}
	base := filepath.Join("/sys/fs/cgroup", path)
	read := func(name string) []string {
		raw, err := os.ReadFile(filepath.Join(base, name))
		if err != nil || len(raw) > 128 {
			t.Fatal("missing finite fixture limit", name)
		}
		return strings.Fields(string(raw))
	}
	number := func(fields []string, maximum uint64) uint64 {
		if len(fields) != 1 {
			t.Fatal("invalid fixture limit")
		}
		n, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil || n == 0 || n > maximum {
			t.Fatal("fixture is not finitely capped")
		}
		return n
	}
	memory := number(read("memory.max"), 8<<30)
	pids := number(read("pids.max"), 512)
	cpu := read("cpu.max")
	if len(cpu) != 2 {
		t.Fatal("invalid fixture CPU cap")
	}
	quota, period := number(cpu[:1], 4_000_000), number(cpu[1:], 1_000_000)
	if quota > 4*period {
		t.Fatal("fixture CPU cap exceeds four CPUs")
	}
	t.Logf("GATEWAY_AUTHORITY_CAPS memory_bytes=%d tasks=%d cpu_quota=%d cpu_period=%d", memory, pids, quota, period)
}

func extractBinary(dir, version string, release release) (string, string, error) {
	bad := errors.New("invalid cached pinned broker archive")
	path := os.Getenv(release.env)
	info, err := os.Lstat(path)
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 64<<20 {
		return "", "", bad
	}
	f, err := os.Open(path)
	if err != nil {
		return "", "", bad
	}
	raw, err := io.ReadAll(io.LimitReader(f, (64<<20)+1))
	closeErr := f.Close()
	if err != nil || closeErr != nil || len(raw) > 64<<20 || fmt.Sprintf("%x", sha256.Sum256(raw)) != release.archive {
		return "", "", bad
	}
	gz, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return "", "", bad
	}
	defer gz.Close()
	bundle := tar.NewReader(io.LimitReader(gz, (96<<20)+1))
	want := "nats-server-v" + version + "-linux-amd64/nats-server"
	var binary []byte
	complete := false
	for i := 0; i < 32; i++ {
		header, err := bundle.Next()
		if errors.Is(err, io.EOF) {
			complete = true
			break
		}
		if err != nil {
			return "", "", bad
		}
		if header.Name != want {
			continue
		}
		if binary != nil || header.Typeflag != tar.TypeReg || header.Size <= 0 || header.Size > 64<<20 {
			return "", "", bad
		}
		binary, err = io.ReadAll(io.LimitReader(bundle, (64<<20)+1))
		if err != nil || int64(len(binary)) != header.Size {
			return "", "", bad
		}
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(binary))
	if !complete || len(binary) == 0 || digest != release.binary {
		return "", "", bad
	}
	destination := filepath.Join(dir, "nats-server")
	if err := writePrivate(destination, binary, 0700); err != nil {
		return "", "", errors.New("owned broker executable write failed")
	}
	return destination, digest, nil
}

func (f *Fixture) credentials() error {
	bad := errors.New("fixture credential generation failed")
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return bad
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "kelvo-gateway-authority-fixture-ca"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		return bad
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return bad
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "kelvo-gateway-authority-fixture"}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, caKey)
	if err != nil {
		return bad
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return bad
	}
	f.ca, f.cert, f.key = filepath.Join(f.dir, "ca.pem"), filepath.Join(f.dir, "leaf.pem"), filepath.Join(f.dir, "leaf.key")
	for path, block := range map[string]*pem.Block{f.ca: {Type: "CERTIFICATE", Bytes: caDER}, f.cert: {Type: "CERTIFICATE", Bytes: leafDER}, f.key: {Type: "PRIVATE KEY", Bytes: keyDER}} {
		if writePrivate(path, pem.EncodeToMemory(block), 0600) != nil {
			return bad
		}
	}
	var suffix [12]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return bad
	}
	for _, role := range []string{"initializer", "control", "gateway-a", "gateway-b"} {
		var secret [32]byte
		if _, err := rand.Read(secret[:]); err != nil {
			return bad
		}
		name := Env + "_PASSWORD_" + strings.ToUpper(hex.EncodeToString(suffix[:])) + "_" + strings.ToUpper(strings.ReplaceAll(role, "-", "_"))
		if _, exists := os.LookupEnv(name); exists {
			return bad
		}
		f.passwords[role], f.passwordEnvs[role] = hex.EncodeToString(secret[:]), name
		if os.Setenv(name, f.passwords[role]) != nil {
			return bad
		}
	}
	return nil
}

func (f *Fixture) serverConfig(index int) map[string]any {
	// These literal permissions match the protocol fixture. Keeping the helper
	// independent of authfence lets that package and cluster both import it.
	users := []any{}
	for _, role := range []string{"initializer", "control", "gateway-a", "gateway-b"} {
		replica := role
		if role == "initializer" {
			replica = "control"
		}
		publish := []string{"$JS.API.STREAM.INFO.KELVO_AUTHORITY", "$JS.API.STREAM.MSG.GET.KELVO_AUTHORITY", "auth.witness." + replica}
		if replica == "control" {
			publish = append(publish, "auth.current")
		}
		if role == "initializer" {
			publish = append(publish, "$JS.API.STREAM.CREATE.KELVO_AUTHORITY")
		}
		users = append(users, map[string]any{"user": role, "password": f.passwords[role], "permissions": map[string]any{"publish": map[string]any{"allow": publish}, "subscribe": map[string]any{"allow": []string{"_INBOX.kelvo-authfence." + replica + ".*"}}}})
	}
	routes := []string{}
	for i, node := range f.nodes {
		if i != index {
			routes = append(routes, "nats-route://"+node.route)
		}
	}
	tls := map[string]any{"cert_file": f.cert, "key_file": f.key, "ca_file": f.ca, "verify": true, "min_version": "1.3", "timeout": 1}
	return map[string]any{
		"server_name": f.nodes[index].name, "listen": f.nodes[index].client, "http": f.nodes[index].monitor,
		"max_connections": 64, "max_subscriptions": 64, "max_payload": 32 << 10, "max_pending": 128 << 10, "write_deadline": "1s", "tls": tls,
		"jetstream": map[string]any{"store_dir": filepath.Join(f.dir, fmt.Sprintf("state-%d", index)), "max_memory_store": 8 << 20, "max_file_store": 16 << 20},
		"accounts":  map[string]any{"AUTH": map[string]any{"jetstream": map[string]any{"max_memory": 8 << 20, "max_file": 8 << 20, "max_streams": 1, "max_consumers": 1}, "users": users}},
		"cluster":   map[string]any{"name": "kelvo-gateway-authority", "listen": f.nodes[index].route, "routes": routes, "pool_size": 1, "tls": tls},
	}
}
