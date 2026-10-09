package runtime

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/cassandra"
	chadapter "github.com/SYNEHQ/kelvo-go/adapters/go/connectors/clickhouse"
	mongoadapter "github.com/SYNEHQ/kelvo-go/adapters/go/connectors/mongodb"
	oracleadapter "github.com/SYNEHQ/kelvo-go/adapters/go/connectors/oracle"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/relational"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/provider"
	mysqldriver "github.com/go-sql-driver/mysql"
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
	registry, err := New(relational.NewPostgreSQL(), relational.PGFamily{Engine: "cockroachdb"}, relational.PGFamily{Engine: "redshift"}, relational.PGFamily{Engine: "alloydb"}, relational.NewMySQL(), relational.NewMariaDB(), relational.NewSQLServer(), chadapter.Driver{}, mongoadapter.Driver{}, oracleadapter.Driver{}, cassandra.Driver{Engine: "cassandra"}, cassandra.Driver{Engine: "scylla"})
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
	case "postgresql", "cockroachdb", "redshift", "alloydb":
		if spec.DSN == "" || spec.URL != "" || spec.Username != "" || spec.Password != "" || spec.Token != "" {
			return c, adapter.ErrInvalid
		}
		u, err := url.Parse(spec.DSN)
		if err != nil || (u.Scheme != "postgresql" && u.Scheme != "postgres") || u.User == nil || u.Fragment != "" || u.Opaque != "" || u.Hostname() == "" || len(u.Path) < 2 || u.Path[1:] != c.Namespace {
			return c, adapter.ErrInvalid
		}
		params, err := url.ParseQuery(u.RawQuery)
		if err != nil || params.Get("sslmode") != "verify-full" {
			return c, adapter.ErrInvalid
		}
		for key, values := range params {
			if len(values) != 1 {
				return c, adapter.ErrInvalid
			}
			switch key {
			case "sslmode":
			case "connect_timeout":
				n, e := strconv.Atoi(values[0])
				if e != nil || n < 1 || n > 60 {
					return c, adapter.ErrInvalid
				}
			default:
				return c, adapter.ErrUnsupported
			}
		}
		c.Host = u.Hostname()
		c.Port = 5432
		c.Username = u.User.Username()
		var hasPassword bool
		c.Password, hasPassword = u.User.Password()
		if !hasPassword {
			return c, adapter.ErrInvalid
		}
		if u.Port() != "" {
			c.Port, err = strconv.Atoi(u.Port())
			if err != nil {
				return c, adapter.ErrInvalid
			}
		}
	case "mysql", "mariadb":
		if spec.DSN == "" || spec.URL != "" || spec.Username != "" || spec.Password != "" || spec.Token != "" {
			return c, adapter.ErrInvalid
		}
		slash := strings.LastIndexByte(spec.DSN, '/')
		if slash < 0 {
			return c, adapter.ErrInvalid
		}
		_, raw, exists := strings.Cut(spec.DSN[slash+1:], "?")
		if !exists {
			return c, adapter.ErrInvalid
		}
		params, err := url.ParseQuery(raw)
		if err != nil || params.Get("tls") != "true" || params.Get("parseTime") != "true" || params.Get("loc") != "UTC" || params.Get("time_zone") != "'+00:00'" {
			return c, adapter.ErrInvalid
		}
		for key, values := range params {
			if len(values) != 1 {
				return c, adapter.ErrInvalid
			}
			switch key {
			case "tls", "parseTime", "loc", "time_zone":
			case "charset":
				if values[0] != "utf8mb4" {
					return c, adapter.ErrInvalid
				}
			case "timeout", "readTimeout", "writeTimeout":
				d, e := time.ParseDuration(values[0])
				if e != nil || d <= 0 || d > time.Minute {
					return c, adapter.ErrInvalid
				}
			default:
				return c, adapter.ErrUnsupported
			}
		}
		config, err := mysqldriver.ParseDSN(spec.DSN)
		if err != nil || config.Net != "tcp" || config.DBName != c.Namespace || config.TLS == nil || config.TLS.InsecureSkipVerify || config.MultiStatements || config.InterpolateParams || config.AllowAllFiles || config.AllowFallbackToPlaintext || config.AllowCleartextPasswords || config.AllowOldPasswords {
			return c, adapter.ErrInvalid
		}
		if !strings.HasSuffix(spec.DSN[:slash], "@tcp("+config.Addr+")") {
			return c, adapter.ErrInvalid
		}
		var port string
		c.Host, port, err = net.SplitHostPort(config.Addr)
		if err != nil {
			return c, adapter.ErrInvalid
		}
		c.Port, err = strconv.Atoi(port)
		if err != nil {
			return c, adapter.ErrInvalid
		}
		c.Username = config.User
		c.Password = config.Passwd
	case "sqlserver":
		if spec.DSN == "" || spec.URL != "" || spec.Username != "" || spec.Password != "" || spec.Token != "" {
			return c, adapter.ErrInvalid
		}
		u, err := url.Parse(spec.DSN)
		if err != nil || u.Scheme != "sqlserver" || u.User == nil || u.Hostname() == "" || u.Opaque != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return c, adapter.ErrInvalid
		}
		params, err := url.ParseQuery(u.RawQuery)
		if err != nil || params.Get("database") != c.Namespace || params.Get("encrypt") != "true" || params.Get("TrustServerCertificate") != "false" {
			return c, adapter.ErrInvalid
		}
		for key, values := range params {
			if len(values) != 1 {
				return c, adapter.ErrInvalid
			}
			switch key {
			case "database", "encrypt", "TrustServerCertificate":
			case "connection timeout":
				n, e := strconv.Atoi(values[0])
				if e != nil || n < 1 || n > 60 {
					return c, adapter.ErrInvalid
				}
			default:
				return c, adapter.ErrUnsupported
			}
		}
		c.Host = u.Hostname()
		c.Port = 1433
		c.Username = u.User.Username()
		var hasPassword bool
		c.Password, hasPassword = u.User.Password()
		if !hasPassword {
			return c, adapter.ErrInvalid
		}
		if u.Port() != "" {
			c.Port, err = strconv.Atoi(u.Port())
			if err != nil {
				return c, adapter.ErrInvalid
			}
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
	c.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: c.Host}
	if c.Engine == "mongodb" {
		// Let MongoDB verify each discovered replica/SRV hostname; the seed
		// hostname is not necessarily a valid TLS identity for another member.
		c.TLS.ServerName = ""
	}
	if name := spec.Options["tls_server_name"]; name != "" {
		c.TLS.ServerName = name
	}
	if pem := spec.Options["tls_ca_pem"]; pem != "" {
		roots := x509.NewCertPool()
		if len(pem) > 64<<10 || !roots.AppendCertsFromPEM([]byte(pem)) {
			return c, adapter.ErrInvalid
		}
		c.TLS.RootCAs = roots
	}
	if c.Engine == "mongodb" {
		if _, err := mongoadapter.ConnectionOptions(c); err != nil {
			return c, err
		}
	}
	return c, nil
}
