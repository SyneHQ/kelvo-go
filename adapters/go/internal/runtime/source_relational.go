package runtime

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/relational"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// relationalConnection validates one saved connection of a relational engine
// the warm read worker serves (PostgreSQL family, MySQL, MariaDB, SQL Server)
// and fills its address and credentials. Both adapter builds use it.
func relationalConnection(spec adapter.ConnectionSpec, c adapter.Connection) (adapter.Connection, error) {
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
		// Only the two explicit modes exist. "disable" is the saved connection's
		// own TLS-off choice (databases without TLS, or hosts reached through an
		// already encrypted Rabbit tunnel). "require", "prefer" and other
		// unverified middle modes stay rejected.
		sslmode := params.Get("sslmode")
		if err != nil || (sslmode != "verify-full" && sslmode != "disable") {
			return c, adapter.ErrInvalid
		}
		if sslmode == "disable" {
			c.Options = map[string]string{"sslmode": "disable"}
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
	default:
		return c, adapter.ErrUnsupported
	}
	return c, nil
}

// connectionTLS sets the connection's TLS configuration from its options.
func connectionTLS(spec adapter.ConnectionSpec, c adapter.Connection) (adapter.Connection, error) {
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
	return c, nil
}

// relationalDrivers are the engines a warm read worker serves. The full build
// registers them with its other engines; the slim build registers only these.
func relationalDrivers() []adapter.Driver {
	return []adapter.Driver{relational.NewPostgreSQL(), relational.PGFamily{Engine: "cockroachdb"}, relational.PGFamily{Engine: "redshift"},
		relational.PGFamily{Engine: "alloydb"}, relational.NewMySQL(), relational.NewMariaDB(), relational.NewSQLServer()}
}
