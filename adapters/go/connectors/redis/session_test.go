package redis

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/provider"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type fixtureStep struct {
	args  []string
	reply string
}

func tlsFixture(t *testing.T, steps []fixtureStep) (adapter.Connection, <-chan error) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "redis-test"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(certDER)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{Certificate: [][]byte{certDER}, PrivateKey: key}}})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var active net.Conn
	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		mu.Lock()
		active = conn
		mu.Unlock()
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		reader := bufio.NewReader(conn)
		for _, step := range steps {
			value, err := readReply(reader, &replyBudget{bytes: 2 << 20, values: 2048}, 0)
			if err != nil {
				done <- err
				return
			}
			args, ok := value.([]any)
			got := make([]string, len(args))
			for i, v := range args {
				got[i], _ = redisString(v)
			}
			if !ok || !reflect.DeepEqual(got, step.args) {
				done <- errors.New("unexpected Redis fixture command")
				return
			}
			if step.reply == "CLOSE" {
				done <- nil
				return
			}
			if _, err := io.WriteString(conn, step.reply); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	t.Cleanup(func() {
		listener.Close()
		mu.Lock()
		if active != nil {
			active.Close()
		}
		mu.Unlock()
	})
	host, port, _ := net.SplitHostPort(listener.Addr().String())
	n, _ := strconv.Atoi(port)
	return adapter.Connection{TenantID: "tenant-a", ConnectionID: "saved-redis", Revision: "revision-a", Engine: "redis", Host: host, Port: n, Namespace: "7", Username: "tenant-user", Password: "fixture-secret", TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}}, done
}
func fixtureStart() []fixtureStep {
	return []fixtureStep{{[]string{"AUTH", "tenant-user", "fixture-secret"}, "+OK\r\n"}, {[]string{"SELECT", "7"}, "+OK\r\n"}}
}
func fixtureComplete(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("fixture stalled")
	}
}

type resultSink struct {
	schema    *arrow.Schema
	documents []json.RawMessage
	rows      int64
	fail      error
}

func (s *resultSink) Schema(schema *arrow.Schema) error { s.schema = schema; return s.fail }
func (s *resultSink) Write(record arrow.RecordBatch) error {
	s.rows += record.NumRows()
	if record.NumCols() == 1 {
		if v, ok := record.Column(0).(*array.Binary); ok {
			for i := 0; i < v.Len(); i++ {
				s.documents = append(s.documents, append(json.RawMessage(nil), v.Value(i)...))
			}
		}
	}
	return s.fail
}
func native(t *testing.T, text string, bytes int64) adapter.Native {
	t.Helper()
	kind, spec, err := provider.InvocationText("redis", text)
	if err != nil {
		t.Fatal(err)
	}
	return adapter.Native{Kind: kind, Spec: *spec, Limits: adapter.Limits{MaxRows: 100, MaxBytes: bytes, BatchRows: 10}}
}
func TestTLSACLAndSavedDatabaseOwnNativeSession(t *testing.T) {
	steps := append(fixtureStart(), fixtureStep{[]string{"GET", "key with spaces"}, "$2\r\n\x00\xff\r\n"})
	c, done := tlsFixture(t, steps)
	opened, err := (Driver{}).Open(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	sink := &resultSink{}
	result, err := opened.(adapter.NativeSession).RunNative(context.Background(), native(t, `GET "key with spaces"`, 4096), sink)
	if err != nil || result.Outcome != operations.Completed || result.Effect != operations.EffectNone || len(sink.documents) != 1 || !strings.Contains(string(sink.documents[0]), `"encoding":"binary"`) {
		t.Fatal(result, err, sink.documents)
	}
	fixtureComplete(t, done)
}
func TestRedisWriteEffectSurvivesResultLimitsAndLostReply(t *testing.T) {
	for _, test := range []struct {
		name, reply string
		bytes       int64
		out         operations.Outcome
		effect      operations.Effect
	}{{"result-limit", "+OK\r\n", 1, operations.Failed, operations.EffectCommitted}, {"lost-reply", "CLOSE", 4096, operations.OutcomeUnknown, operations.EffectUnknown}, {"server-rejection", "-NOPERM private diagnostic\r\n", 4096, operations.Failed, operations.EffectNone}} {
		t.Run(test.name, func(t *testing.T) {
			c, done := tlsFixture(t, append(fixtureStart(), fixtureStep{[]string{"SET", "key", "value"}, test.reply}))
			opened, err := (Driver{}).Open(context.Background(), c)
			if err != nil {
				t.Fatal(err)
			}
			defer opened.Close()
			result, err := opened.(adapter.NativeSession).RunNative(context.Background(), native(t, "SET key value", test.bytes), &resultSink{})
			if err == nil || result.Outcome != test.out || result.Effect != test.effect || strings.Contains(err.Error(), "private") {
				t.Fatal(result, err)
			}
			fixtureComplete(t, done)
		})
	}
}
func TestRedisKeysUsesScanAndDeduplicates(t *testing.T) {
	steps := append(fixtureStart(), fixtureStep{[]string{"SCAN", "0", "MATCH", "tenant:*", "COUNT", "256"}, "*2\r\n$1\r\n4\r\n*2\r\n$8\r\ntenant:a\r\n$8\r\ntenant:b\r\n"}, fixtureStep{[]string{"SCAN", "4", "MATCH", "tenant:*", "COUNT", "256"}, "*2\r\n$1\r\n0\r\n*2\r\n$8\r\ntenant:a\r\n$8\r\ntenant:c\r\n"})
	c, done := tlsFixture(t, steps)
	opened, err := (Driver{}).Open(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	sink := &resultSink{}
	result, err := opened.(adapter.NativeSession).RunNative(context.Background(), native(t, "KEYS tenant:*", 4096), sink)
	if err != nil || result.Outcome != operations.Completed || string(sink.documents[0]) != `{"value":["tenant:a","tenant:b","tenant:c"]}` {
		t.Fatal(result, err, sink.documents)
	}
	fixtureComplete(t, done)
}
func TestRedisCancellationAndInvalidSourcesNeverDial(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (Driver{}).Open(ctx, adapter.Connection{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, c := range []adapter.Connection{{Engine: "redis"}, {Engine: "redis", TLS: &tls.Config{InsecureSkipVerify: true}}} {
		if _, err := (Driver{}).Open(context.Background(), c); !errors.Is(err, adapter.ErrInvalid) {
			t.Fatal(err)
		}
	}
}
func TestRedisMetadataCannotSelectAnotherDatabase(t *testing.T) {
	c, done := tlsFixture(t, fixtureStart())
	opened, err := (Driver{}).Open(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	s := opened.(*Session)
	limits := adapter.Limits{MaxRows: 100, MaxBytes: 4096, BatchRows: 10}
	if _, err := s.Inspect(context.Background(), operations.MetadataSpec{Object: "tables", Target: operations.ObjectRef{Catalog: "8"}, Limit: 10}, limits, &resultSink{}); !errors.Is(err, adapter.ErrInvalid) {
		t.Fatal(err)
	}
	sink := &resultSink{}
	stats, err := s.Inspect(context.Background(), operations.MetadataSpec{Object: "catalogs", Limit: 10}, limits, sink)
	if err != nil || stats.Rows != 1 || sink.schema.Field(0).Name != "catalog" {
		t.Fatal(stats, err)
	}
	fixtureComplete(t, done)
}
