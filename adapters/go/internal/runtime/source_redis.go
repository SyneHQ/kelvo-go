package runtime

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"strconv"

	"github.com/SYNEHQ/kelvo-go/adapter"
	redisadapter "github.com/SYNEHQ/kelvo-go/adapters/go/connectors/redis"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func openRedisSource(ctx context.Context, spec adapter.ConnectionSpec, request operations.Request) (adapter.Session, error) {
	if err := adapter.ValidateRedisProcessSource(spec); err != nil {
		return nil, err
	}
	u, _ := url.Parse(spec.URL)
	port, _ := strconv.Atoi(u.Port())
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: u.Hostname()}
	if name := spec.Options["tls_server_name"]; name != "" {
		tlsConfig.ServerName = name
	}
	if raw := spec.Options["tls_ca_pem"]; raw != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(raw)) {
			return nil, adapter.ErrInvalid
		}
		tlsConfig.RootCAs = pool
	}
	registry, err := New(redisadapter.Driver{})
	if err != nil {
		return nil, err
	}
	return registry.Open(ctx, adapter.Connection{TenantID: spec.TenantID, ConnectionID: spec.ConnectionID, Revision: spec.Revision, Engine: "redis", Host: u.Hostname(), Port: port, Namespace: spec.Database, Username: spec.Username, Password: spec.Password, TLS: tlsConfig}, request)
}
