package adapter

import (
	"net/url"
	"regexp"
	"strings"

	"github.com/SYNEHQ/kelvo-go/provider"
)

var warehouseIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$-]{0,127}$`)
var warehouseComponent = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_-]{0,127}$`)
var snowflakeAccountHost = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]*\.snowflakecomputing\.com$`)

func ValidateWarehouseProcessSource(s ConnectionSpec) error {
	if s.DSN != "" || s.Username != "" || s.Password != "" || len(s.Options) > 6 {
		return ErrInvalid
	}
	allowed := map[string]bool{}
	switch s.Engine {
	case "bigquery":
		if s.URL != "https://bigquery.googleapis.com" || s.Schema != "" || !warehouseComponent.MatchString(s.Options["project"]) || !warehouseComponent.MatchString(s.Options["location"]) || !warehouseIdentifier.MatchString(s.Database) || s.Options["dataset"] != s.Database {
			return ErrInvalid
		}
		if _, err := provider.CanonicalGoogleCredential(s.Token); err != nil {
			return ErrInvalid
		}
		for _, name := range []string{"project", "location", "dataset"} {
			allowed[name] = true
		}
	case "snowflake":
		u, err := url.Parse(s.URL)
		if err != nil || u.Scheme != "https" || u.Host != strings.ToLower(u.Host) || !snowflakeAccountHost.MatchString(u.Host) || u.User != nil || u.Path != "" || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || !provider.CloudBearer(s.Token) || !warehouseIdentifier.MatchString(s.Database) || s.Options["database"] != s.Database || s.Options["schema"] != s.Schema {
			return ErrInvalid
		}
		if s.Schema != "" && !warehouseIdentifier.MatchString(s.Schema) {
			return ErrInvalid
		}
		for _, name := range []string{"database", "schema", "warehouse", "role"} {
			allowed[name] = true
			if value := s.Options[name]; value != "" && !warehouseIdentifier.MatchString(value) {
				return ErrInvalid
			}
		}
		allowed["token_type"] = true
		switch s.Options["token_type"] {
		case "OAUTH", "KEYPAIR_JWT", "PROGRAMMATIC_ACCESS_TOKEN":
		default:
			return ErrInvalid
		}
	default:
		return ErrUnsupported
	}
	for name := range s.Options {
		if !allowed[name] {
			return ErrUnsupported
		}
	}
	return nil
}
