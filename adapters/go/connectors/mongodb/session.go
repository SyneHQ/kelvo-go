// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package mongodb implements explicit MongoDB commands without a shell or
// arbitrary runCommand passthrough. Source credentials remain operation-scoped.
package mongodb

import (
	"context"
	"crypto/tls"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

type Driver struct{}

func (Driver) Capabilities() operations.Capabilities {
	return operations.Capabilities{Version: operations.Version, Engine: "mongodb", Operations: []operations.Capability{
		{Kind: operations.QueryRead, Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.NativeRead, ParameterTypes: []string{"json"}, Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.NativeExecute, ParameterTypes: []string{"json"}, Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.ConnectionTest, Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.MetadataInspect, Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.WatchInstall, Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.WatchRead, Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.WatchAck, Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.WatchRemove, Idempotency: "none", Cancellation: "best_effort"},
	}}
}

// ConnectionOptions is also used by the process boundary to reject unsupported
// URI options before dialing. Endpoint contains no username or password.
func ConnectionOptions(c adapter.Connection) (*options.ClientOptions, error) {
	if c.Engine != "mongodb" || c.TenantID == "" || c.ConnectionID == "" || c.Revision == "" || !validDatabase(c.Namespace) || c.Schema != "" && c.Schema != c.Namespace || c.Username == "" || c.Password == "" || len(c.Username) > 32<<10 || len(c.Password) > 32<<10 || strings.ContainsAny(c.Username+c.Password, "\x00\r\n") {
		return nil, adapter.ErrInvalid
	}
	u, err := url.Parse(c.Endpoint)
	if err != nil || len(c.Endpoint) > 32<<10 || (u.Scheme != "mongodb" && u.Scheme != "mongodb+srv") || u.User != nil || u.Fragment != "" || u.Opaque != "" || (u.Path != "" && u.Path != "/") || u.Hostname() == "" || strings.ContainsAny(u.Hostname(), "/\\, \t\r\n") {
		return nil, adapter.ErrInvalid
	}
	if u.Scheme == "mongodb+srv" {
		if u.Port() != "" || net.ParseIP(u.Hostname()) != nil {
			return nil, adapter.ErrInvalid
		}
	} else if u.Port() != "" {
		port, e := strconv.Atoi(u.Port())
		if e != nil || port < 1 || port > 65535 {
			return nil, adapter.ErrInvalid
		}
	}
	params, err := url.ParseQuery(u.RawQuery)
	if err != nil || params.Get("tls") != "true" || !validDatabase(params.Get("authSource")) {
		return nil, adapter.ErrInvalid
	}
	for key, values := range params {
		if len(values) != 1 {
			return nil, adapter.ErrInvalid
		}
		switch key {
		case "tls", "authSource":
		case "replicaSet":
			if values[0] == "" || len(values[0]) > 256 || strings.ContainsAny(values[0], "\x00\r\n") {
				return nil, adapter.ErrInvalid
			}
		case "directConnection":
			if values[0] != "true" || u.Scheme == "mongodb+srv" {
				return nil, adapter.ErrInvalid
			}
		default:
			return nil, adapter.ErrUnsupported
		}
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12}
	if c.TLS != nil {
		config = c.TLS.Clone()
	}
	if config.InsecureSkipVerify || config.MaxVersion != 0 && config.MaxVersion < tls.VersionTLS12 {
		return nil, adapter.ErrInvalid
	}
	config.MinVersion = max(config.MinVersion, tls.VersionTLS12)
	// An empty ServerName lets TLS verify each discovered replica/SRV endpoint.
	// An explicitly supplied name is retained for approved private PKI routing.
	client := options.Client().ApplyURI(u.String()).SetTLSConfig(config).
		SetAuth(options.Credential{AuthSource: params.Get("authSource"), Username: c.Username, Password: c.Password}).
		SetAppName("kelvo-go").SetMaxPoolSize(1).SetMinPoolSize(0).SetMaxConnecting(1).
		SetRetryReads(false).SetRetryWrites(false).SetReadPreference(readpref.Primary()).
		SetWriteConcern(writeconcern.Majority()).SetConnectTimeout(5 * time.Second).
		SetServerSelectionTimeout(5 * time.Second).SetTimeout(5 * time.Minute)
	if err := client.Validate(); err != nil {
		return nil, adapter.ErrInvalid
	}
	return client, nil
}

func (Driver) Open(ctx context.Context, c adapter.Connection) (adapter.Session, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, adapter.ErrInvalid
	}
	opts, err := ConnectionOptions(c)
	if err != nil {
		return nil, err
	}
	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, adapter.ErrInvalid
	}
	s := &Session{client: client, database: c.Namespace, revision: c.Revision}
	if err := s.Test(ctx); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

type Session struct {
	client   *mongo.Client
	database string
	revision string
}

func (s *Session) Close() error {
	if s == nil || s.client == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return s.client.Disconnect(ctx)
}
func (s *Session) Test(ctx context.Context) error {
	if s == nil || s.client == nil || ctx == nil {
		return adapter.ErrInvalid
	}
	return s.client.Ping(ctx, readpref.Primary())
}

func validDatabase(name string) bool {
	return name != "" && len(name) <= 63 && utf8.ValidString(name) && !strings.ContainsAny(name, "/\\.\"$ \x00\r\n")
}

func validCollection(name string) bool {
	return name != "" && len(name) <= 120 && utf8.ValidString(name) && !strings.ContainsAny(name, "$\x00\r\n") && !strings.HasPrefix(name, "system.")
}
