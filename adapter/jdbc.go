package adapter

import (
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/SYNEHQ/kelvo-go/operations"
)

// JDBCRuntime is parent-owned containment metadata. Source resolvers cannot
// select executable paths, class names, flags or JARs. The launcher verifies and
// opens operator-pinned artifacts before writing these descriptor numbers.
type JDBCRuntime struct {
	Version  int   `json:"version"`
	JavaFD   int   `json:"java_fd"`
	JARFDs   []int `json:"jar_fds"`
	HeapMB   int   `json:"heap_mb"`
	DirectMB int   `json:"direct_mb"`
}

func JDBCProfile(engine string) bool {
	switch engine {
	case "h2", "hive", "spark", "db2", "sap_hana", "sap_ase":
		return true
	}
	return false
}

func (r *JDBCRuntime) Validate() error {
	if r == nil || r.Version != 1 || r.JavaFD != 7 || len(r.JARFDs) < 1 || len(r.JARFDs) > 32 || r.HeapMB < 32 || r.HeapMB > 4096 || r.DirectMB < 16 || r.DirectMB > 4096 {
		return ErrInvalid
	}
	for i, fd := range r.JARFDs {
		if fd != 8+i {
			return ErrInvalid
		}
	}
	return nil
}

func (r ProcessRequest) validateRuntime() error {
	if r.Runtime == nil {
		return nil
	}
	if r.SourceFile != nil || r.Runtime.Validate() != nil {
		return ErrInvalid
	}
	return ValidateJDBCSource(r.Source)
}

func ValidateJDBCSource(s ConnectionSpec) error {
	if !JDBCProfile(s.Engine) {
		return ErrUnsupported
	}
	if s.DSN != "" || s.Token != "" || s.Username == "" || s.Password == "" || !jdbcIdentifier(s.Database, false) || !jdbcIdentifier(s.Schema, true) {
		return ErrInvalid
	}
	endpoint, err := url.Parse(s.URL)
	if err != nil || endpoint.Scheme != "tls" || endpoint.User != nil || endpoint.Path != "" || endpoint.RawQuery != "" || endpoint.ForceQuery || endpoint.Fragment != "" || endpoint.Opaque != "" || endpoint.Hostname() == "" || strings.ContainsAny(endpoint.Hostname(), "%/\\ \t\r\n") {
		return ErrInvalid
	}
	port, err := strconv.Atoi(endpoint.Port())
	if err != nil || port < 1 || port > 65535 {
		return ErrInvalid
	}
	host := endpoint.Hostname()
	if len(host) > 253 || strings.Contains(host, ":") && net.ParseIP(host) == nil {
		return ErrInvalid
	}

	return nil
}

func jdbcIdentifier(value string, optional bool) bool {
	if value == "" {
		return optional
	}
	if len(value) > 128 {
		return false
	}
	for _, c := range value {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

// Declarations describe the optional protocol; live driver conformance is a
// separate release gate. Hive/Spark never promise atomic transactions.
func JDBCCapabilities(engine string) operations.Capabilities {
	c := operations.Capabilities{Version: operations.Version, Engine: engine}
	if !JDBCProfile(engine) {
		return c
	}
	types := []string{"null", "bool", "int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64", "float32", "float64", "decimal128", "decimal256", "string", "binary", "date", "timestamp", "json"}
	c.Operations = []operations.Capability{
		{Kind: operations.ConnectionTest, Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.MetadataInspect, Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.QueryRead, ParameterTypes: types, Idempotency: "none", Cancellation: "best_effort"},
	}
	modes := []operations.TransactionMode{operations.TransactionAutocommit}
	isolation := []string{}
	if engine == "h2" || engine == "db2" {
		modes = append(modes, operations.TransactionRequired)
		isolation = []string{"read_committed", "repeatable_read", "serializable"}
	}
	c.Operations = append(c.Operations, operations.Capability{Kind: operations.StatementExecute, ParameterTypes: types, Transactions: modes, IsolationLevels: isolation, Idempotency: "none", Cancellation: "best_effort"})
	return c
}
