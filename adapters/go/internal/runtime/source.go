//go:build !slim

package runtime

import (
	"context"
	"net/url"
	"strconv"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/cassandra"
	chadapter "github.com/SYNEHQ/kelvo-go/adapters/go/connectors/clickhouse"
	mongoadapter "github.com/SYNEHQ/kelvo-go/adapters/go/connectors/mongodb"
	oracleadapter "github.com/SYNEHQ/kelvo-go/adapters/go/connectors/oracle"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/provider"
)

func openSource(ctx context.Context, spec adapter.ConnectionSpec, request operations.Request) (adapter.Session, error) {
	if spec.Engine == "redis" {
		return openRedisSource(ctx, spec, request)
	}
	if provider.SaaS(spec.Engine) {
		return openSaaSSource(ctx, spec, request)
	}
	if provider.Supported(spec.Engine) {
		return openBusinessSource(ctx, spec, request)
	}
	connection, err := sourceConnection(spec)
	if err != nil {
		return nil, err
	}
	registry, err := New(append(relationalDrivers(), chadapter.Driver{}, mongoadapter.Driver{}, oracleadapter.Driver{}, cassandra.Driver{Engine: "cassandra"}, cassandra.Driver{Engine: "scylla"})...)
	if err != nil {
		return nil, err
	}
	return registry.Open(ctx, connection, request)
}

func sourceConnection(spec adapter.ConnectionSpec) (adapter.Connection, error) {
	c := adapter.Connection{TenantID: spec.TenantID, ConnectionID: spec.ConnectionID, Revision: spec.Revision, Engine: spec.Engine, Namespace: spec.Database, Schema: spec.Schema}
	if c.Engine == "postgres" {
		c.Engine = "postgresql"
	}
	for key := range spec.Options {
		if key == "database" && c.Engine == "mongodb" {
			if spec.Options[key] != c.Namespace {
				return c, adapter.ErrInvalid
			}
			continue
		}
		if key == "migration_server_uuid" && c.Engine == "clickhouse" {
			c.Options = map[string]string{key: spec.Options[key]}
			continue
		}
		if key != "tls_ca_pem" && key != "tls_server_name" {
			return c, adapter.ErrUnsupported
		}
	}
	switch c.Engine {
	case "postgresql", "cockroachdb", "redshift", "alloydb", "mysql", "mariadb", "sqlserver":
		var err error
		if c, err = relationalConnection(spec, c); err != nil {
			return c, err
		}
	case "oracle":
		if spec.DSN == "" || spec.URL != "" || spec.Username != "" || spec.Password != "" || spec.Token != "" {
			return c, adapter.ErrInvalid
		}
		u, err := url.Parse(spec.DSN)
		if err != nil || u.Scheme != "oracle" || u.User == nil || u.Hostname() == "" || u.Opaque != "" || u.Fragment != "" || u.Path != "/"+c.Namespace {
			return c, adapter.ErrInvalid
		}
		params, err := url.ParseQuery(u.RawQuery)
		if err != nil || params.Get("SSL") != "enable" || params.Get("SSL VERIFY") != "true" || len(params) != 2 || len(params["SSL"]) != 1 || len(params["SSL VERIFY"]) != 1 {
			return c, adapter.ErrInvalid
		}
		c.Host = u.Hostname()
		c.Port = 2484
		c.Username = u.User.Username()
		var exists bool
		c.Password, exists = u.User.Password()
		if !exists {
			return c, adapter.ErrInvalid
		}
		if u.Port() != "" {
			c.Port, err = strconv.Atoi(u.Port())
			if err != nil {
				return c, adapter.ErrInvalid
			}
		}
	case "clickhouse":
		if spec.DSN != "" || spec.URL == "" || spec.Token != "" {
			return c, adapter.ErrInvalid
		}
		u, err := url.Parse(spec.URL)
		if err != nil || u.Scheme != "https" || u.User != nil || u.Hostname() == "" || (u.Path != "" && u.Path != "/") || u.Fragment != "" || u.Opaque != "" {
			return c, adapter.ErrInvalid
		}
		params, err := url.ParseQuery(u.RawQuery)
		if err != nil || params.Get("database") != c.Namespace || len(params) != 1 || len(params["database"]) != 1 {
			return c, adapter.ErrInvalid
		}
		c.Host = u.Hostname()
		c.Port = 443
		if u.Port() != "" {
			c.Port, err = strconv.Atoi(u.Port())
			if err != nil {
				return c, adapter.ErrInvalid
			}
		}
		c.Username = spec.Username
		c.Password = spec.Password
	case "mongodb":
		if spec.DSN == "" || spec.URL != "" || spec.Username != "" || spec.Password != "" || spec.Token != "" {
			return c, adapter.ErrInvalid
		}
		u, err := url.Parse(spec.DSN)
		if err != nil || u.User == nil {
			return c, adapter.ErrInvalid
		}
		c.Username = u.User.Username()
		var exists bool
		c.Password, exists = u.User.Password()
		if !exists {
			return c, adapter.ErrInvalid
		}
		u.User = nil
		c.Endpoint, c.Host = u.String(), u.Hostname()
		c.Port = 27017
		if u.Port() != "" {
			c.Port, err = strconv.Atoi(u.Port())
			if err != nil {
				return c, adapter.ErrInvalid
			}
		}
	case "cassandra", "scylla":
		if spec.DSN != "" || spec.URL == "" || spec.Token != "" {
			return c, adapter.ErrInvalid
		}
		u, err := url.Parse(spec.URL)
		if err != nil || u.Scheme != "tls" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" {
			return c, adapter.ErrInvalid
		}
		c.Host = u.Hostname()
		c.Port, err = strconv.Atoi(u.Port())
		if err != nil {
			return c, adapter.ErrInvalid
		}
		c.Username = spec.Username
		c.Password = spec.Password
	default:
		return c, adapter.ErrUnsupported
	}
	var err error
	if c, err = connectionTLS(spec, c); err != nil {
		return c, err
	}
	if c.Engine == "mongodb" {
		if _, err := mongoadapter.ConnectionOptions(c); err != nil {
			return c, err
		}
	}
	return c, nil
}
