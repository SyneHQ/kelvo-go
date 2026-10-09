package adapter

import (
	"encoding/json"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/filesnapshot"
	"github.com/SYNEHQ/kelvo-go/migration"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/provider"
)

const ProcessVersion = 1
const MaxProcessRequestBytes = 3 << 20

// ProcessRequest is private pipe input. It must never enter the operation
// ledger, argv, environment, files, logs or an HTTP response.
type ProcessRequest struct {
	Version               int                      `json:"version"`
	OperationID           string                   `json:"operation_id"`
	RequestSHA256         string                   `json:"request_sha256"`
	Request               operations.Request       `json:"request"`
	Limits                ProcessLimits            `json:"limits"`
	Source                ConnectionSpec           `json:"source"`
	SourceFile            *filesnapshot.Descriptor `json:"source_file,omitempty"`
	PrivateTransport      *PrivateTransport        `json:"private_transport,omitempty"`
	Runtime               *JDBCRuntime             `json:"runtime,omitempty"`
	AppTeam               string                   `json:"app_team,omitempty"`
	Input                 []byte                   `json:"input,omitempty"`
	CredentialsValidUntil int64                    `json:"credentials_valid_until"`
	ExpiresAt             int64                    `json:"expires_at"`
}

type ProcessLimits struct {
	MaxRows   int64 `json:"max_rows"`
	MaxBytes  int64 `json:"max_bytes"`
	BatchRows int   `json:"batch_rows"`
	TimeoutMS int64 `json:"timeout_ms"`
	MemoryMB  int   `json:"memory_mb,omitempty"`
	Threads   int   `json:"threads,omitempty"`
	MaxTempMB int   `json:"max_temp_mb,omitempty"`
}

type ConnectionSpec struct {
	Engine       string            `json:"engine"`
	DSN          string            `json:"dsn,omitempty"`
	URL          string            `json:"url,omitempty"`
	Username     string            `json:"username,omitempty"`
	Password     string            `json:"password,omitempty"`
	Token        string            `json:"token,omitempty"`
	Options      map[string]string `json:"options,omitempty"`
	TenantID     string            `json:"tenant_id"`
	ConnectionID string            `json:"connection_id"`
	Revision     string            `json:"revision"`
	Database     string            `json:"database"`
	Schema       string            `json:"schema"`
}

func (r ProcessRequest) Validate() error { return r.ValidateAt(time.Now()) }

func (r ProcessRequest) ValidateAt(now time.Time) error {
	if r.Version != ProcessVersion || !operations.ValidID(r.OperationID) || !operations.ValidDigest(r.RequestSHA256) || r.Request.Validate() != nil {
		return ErrInvalid
	}
	if err := r.validateInput(); err != nil {
		return err
	}
	digest, err := operations.Digest(r.Request)
	if err != nil || digest != r.RequestSHA256 {
		return ErrInvalid
	}
	if r.ExpiresAt <= now.Unix() || r.ExpiresAt > now.Unix()+300 || r.CredentialsValidUntil <= now.Unix() || r.CredentialsValidUntil > now.Unix()+5 || r.CredentialsValidUntil > r.ExpiresAt {
		return ErrInvalid
	}
	if r.Limits.TimeoutMS < 1 || r.Limits.TimeoutMS > 300000 {
		return ErrInvalid
	}
	if r.Limits.MemoryMB < 0 || r.Limits.MemoryMB > 1048576 || r.Limits.Threads < 0 || r.Limits.Threads > 1024 || r.Limits.MaxTempMB < 0 || r.Limits.MaxTempMB > 1048576 {
		return ErrInvalid
	}
	if (Query{Statement: "process", MaxRows: r.Limits.MaxRows, MaxBytes: r.Limits.MaxBytes, BatchRows: r.Limits.BatchRows}).Validate() != nil {
		return ErrInvalid
	}
	if !operations.ValidID(r.Source.Engine) || r.Source.ConnectionID != r.Request.Connection.ID || r.Source.Database != r.Request.Connection.Database || r.Source.Schema != r.Request.Connection.Schema {
		return ErrInvalid
	}
	for _, v := range []string{r.Source.TenantID, r.Source.ConnectionID, r.Source.Revision} {
		if v == "" || len(v) > 256 || !utf8.ValidString(v) || strings.ContainsAny(v, "\x00\r\n") {
			return ErrInvalid
		}
	}
	for _, v := range []string{r.Source.DSN, r.Source.URL, r.Source.Username, r.Source.Password, r.Source.Token, r.Source.Database, r.Source.Schema} {
		if len(v) > 64<<10 || !utf8.ValidString(v) || strings.ContainsRune(v, 0) {
			return ErrInvalid
		}
	}
	if err := r.validatePrivateTransport(); err != nil {
		return err
	}
	if err := r.validateSourceFile(); err != nil {
		return err
	}
	if err := r.validateRuntime(); err != nil {
		return err
	}
	if r.SourceFile != nil {
		return nil
	}
	if NativeReader(r.Source.Engine) {
		if r.Limits.MemoryMB < 16 || r.Limits.Threads < 1 || r.Limits.MaxTempMB < 1 {
			return ErrInvalid
		}
		return ValidateNativeReaderProcessSource(r.Source)
	}
	if CloudSQL(r.Source.Engine) {
		if r.Limits.MemoryMB < 16 || r.Limits.Threads < 1 || r.Limits.MaxTempMB < 1 {
			return ErrInvalid
		}
		return ValidateCloudSQLProcessSource(r.Source)
	}
	if r.Source.Engine == "google_sheets" {
		if r.Limits.MemoryMB < 16 || r.Limits.Threads < 1 || r.Limits.MaxTempMB < 1 {
			return ErrInvalid
		}
		return ValidateSheetsProcessSource(r.Source)
	}
	if provider.SaaS(r.Source.Engine) {
		return ValidateSaaSProcessSource(r.Source)
	}
	if r.Source.Engine == "redis" {
		return ValidateRedisProcessSource(r.Source)
	}
	if provider.Supported(r.Source.Engine) {
		return validateBusinessProcessSource(r.Source)
	}
	if len(r.Source.Options) > 3 {
		return ErrInvalid
	}
	for key, value := range r.Source.Options {
		if len(value) > 64<<10 || !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
			return ErrInvalid
		}
		switch key {
		case "tls_ca_pem", "tls_server_name":
		case "database":
			if r.Source.Engine != "mongodb" || value == "" || value != r.Source.Database {
				return ErrInvalid
			}
		case "migration_server_uuid":
			if r.Source.Engine != "clickhouse" || !migration.ValidServerUUID(value) {
				return ErrInvalid
			}
		default:
			return ErrUnsupported
		}
	}
	return nil
}

func validateBusinessProcessSource(s ConnectionSpec) error {
	if strings.ToLower(s.Engine) != s.Engine || s.DSN != "" || (s.URL != "" && s.Engine != "posthog") || s.Password != "" || s.Schema != "" || strings.TrimSpace(s.Token) == "" || len(s.Token) > 32<<10 || len(s.Username) > 1024 || strings.ContainsAny(s.Token+s.Username, "\r\n") || (s.Username != "" && s.Engine != "ramp") || len(s.Options) > 1 {
		return ErrInvalid
	}
	if s.Engine == "posthog" && (!provider.ValidPostHogOrigin(s.URL) || !provider.ValidProject(s.Database) || len(s.Options) != 0) {
		return ErrInvalid
	}
	for key, value := range s.Options {
		if key != "environment" || (value != "" && value != "production" && value != "sandbox") {
			return ErrUnsupported
		}
	}
	return nil
}

func ParseProcessRequest(raw []byte) (ProcessRequest, error) {
	var request ProcessRequest
	if operations.DecodeStrict(raw, &request, MaxProcessRequestBytes) != nil {
		return ProcessRequest{}, ErrInvalid
	}
	if err := request.Validate(); err != nil {
		return ProcessRequest{}, err
	}
	return request, nil
}

func EncodeProcessRequest(request ProcessRequest) ([]byte, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(request)
	if err != nil || len(raw) > MaxProcessRequestBytes {
		return nil, ErrInvalid
	}
	return raw, nil
}
