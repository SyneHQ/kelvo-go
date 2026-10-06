package redis

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// The VM harness creates a private TLS/ACL Redis instance and removes its exact
// directory/process afterwards. No production account or default credential.
func TestRedisLiveTLSACLNativeCommandsAndMetadata(t *testing.T) {
	endpoint := os.Getenv("KELVO_REDIS_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("scoped live Redis fixture not configured")
	}
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	number, _ := strconv.Atoi(port)
	ca, err := os.ReadFile(os.Getenv("KELVO_REDIS_TEST_CA"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("invalid fixture CA")
	}
	c := adapter.Connection{TenantID: "fixture-tenant", ConnectionID: "fixture-redis", Revision: "fixture-revision", Engine: "redis", Host: host, Port: number, Namespace: "7", Username: "tenant-user", Password: os.Getenv("KELVO_REDIS_TEST_PASSWORD"), TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}
	opened, err := (Driver{}).Open(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	session := opened.(*Session)
	if err := session.Test(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"SET fixture:string value", "HSET fixture:hash name Alice role analyst", "LPUSH fixture:list first second", "SADD fixture:set a b", "ZADD fixture:zset 1 one 2 two"} {
		result, err := session.RunNative(context.Background(), native(t, text, 1<<20), &resultSink{})
		if err != nil || result.Outcome != operations.Completed || result.Effect != operations.EffectCommitted {
			t.Fatal("native fixture mutation failed", result, err)
		}
	}
	for _, text := range []string{"GET fixture:string", "HGETALL fixture:hash", "LRANGE fixture:list 0 -1", "SMEMBERS fixture:set", "ZRANGE fixture:zset 0 -1", "KEYS fixture:*"} {
		sink := &resultSink{}
		result, err := session.RunNative(context.Background(), native(t, text, 1<<20), sink)
		if err != nil || result.Outcome != operations.Completed || len(sink.documents) != 1 {
			t.Fatal("native fixture read failed", result, err)
		}
	}
	limits := adapter.Limits{MaxRows: 100, MaxBytes: 1 << 20, BatchRows: 10}
	for _, spec := range []operations.MetadataSpec{{Object: "tables", Limit: 100}, {Object: "columns", Target: operations.ObjectRef{Name: "fixture:hash"}, Limit: 100}} {
		sink := &resultSink{}
		stats, err := session.Inspect(context.Background(), spec, limits, sink)
		if err != nil || stats.Rows < 2 {
			t.Fatal("typed fixture metadata failed", stats, err)
		}
	}
	// A second fresh socket starts in DB 0; selected DB 7 cannot leak through a pool.
	other := c
	other.Namespace = "0"
	fresh, err := (Driver{}).Open(context.Background(), other)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	sink := &resultSink{}
	if _, err := fresh.(adapter.NativeSession).RunNative(context.Background(), native(t, "GET fixture:string", 1<<20), sink); err != nil || len(sink.documents) != 1 || !strings.Contains(string(sink.documents[0]), `"value":null`) {
		t.Fatal("saved database leaked across sessions", err, sink.documents)
	}
	bad := c
	bad.Password = "wrong-credential"
	if opened, err := (Driver{}).Open(context.Background(), bad); err == nil {
		opened.Close()
		t.Fatal("invalid ACL password accepted")
	}
}
