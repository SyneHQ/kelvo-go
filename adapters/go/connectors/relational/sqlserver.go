package relational

import (
	"regexp"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/microsoft/go-mssqldb/msdsn"
)

func sqlserverConfig(c adapter.Connection) (msdsn.Config, error) {
	if !validConnection(c) {
		return msdsn.Config{}, adapter.ErrInvalid
	}
	tlsConfig, err := sourceTLS(c)
	if err != nil {
		return msdsn.Config{}, err
	}
	// Construct configuration directly: no DSN-selected authentication plugins,
	// file certificates, named instances, fallback servers or ambient settings.
	return msdsn.Config{Host: c.Host, Port: uint64(c.Port), Database: c.Namespace, User: c.Username, Password: c.Password,
		Encryption: msdsn.EncryptionRequired, TLSConfig: tlsConfig, HostInCertificateProvided: true,
		DisableRetry: true, DialTimeout: 10 * time.Second, ConnTimeout: 30 * time.Second, PacketSize: 4096,
		AppName: "kelvo-go", Protocols: []string{"tcp"}, Parameters: map[string]string{}, Encoding: msdsn.EncodeParameters{Timezone: time.UTC}}, nil
}

var sqlserverExternal = regexp.MustCompile(`(?i)\b(next|openquery|openrowset|opendatasource|bulk|clr|xp_[a-z0-9_]+)\b`)

// This adds a conservative syntax restriction, not a security claim about
// source permissions. SQL Server has no read-only transaction enforcement.
func validateSQLServerRead(statement string) error {
	if sqlserverExternal.MatchString(statement) {
		return adapter.ErrUnsupported
	}
	return nil
}
