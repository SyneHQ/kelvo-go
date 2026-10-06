// Package clickhouselambda executes the existing synchronous ClickHouse Lambda
// event contract with explicit credentials and bounded, non-replayed requests.
package clickhouselambda

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/awsapi"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlguard"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

const maxPayload = 6 << 20

var functionName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
var regionName = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)+-[0-9]+$`)

type Engine struct {
	source                               catalog.Source
	limits                               query.Limits
	origin, function, region, bucketPath string
	credentials                          aws.Credentials
	client                               *http.Client
	signer                               *v4.Signer
}

func NewResolved(c catalog.Config, l query.Limits, credentials awsapi.Credentials) (*Engine, error) {
	invalid := query.NewError("CONFIGURATION_ERROR", "ClickHouse Lambda requires explicit verified AWS connection details")
	if l.Validate() != nil {
		return nil, invalid
	}
	s, err := cloudapi.SingleSource(c, "clickhouse_lambda")
	if err != nil || !catalog.ValidID(s.ID) || s.DSNEnv != "" || s.Adapter != "" || s.Path != "" || len(s.Options) != 3 {
		return nil, invalid
	}
	region, function, path := s.Options["region"], s.Options["function_name"], s.Options["bucket_path"]
	if !regionName.MatchString(region) || len(region) > 64 || !functionName.MatchString(function) || !ValidBucketPath(path) {
		return nil, invalid
	}
	origin := RegionalOrigin(region)
	if credentials.URL != origin || credentials.AccessKeyID == "" || credentials.SecretAccessKey == "" || len(credentials.AccessKeyID) > 1024 || len(credentials.SecretAccessKey) > 4096 || len(credentials.SessionToken) > 16384 || strings.ContainsAny(credentials.AccessKeyID+credentials.SecretAccessKey+credentials.SessionToken, "\x00\r\n") {
		return nil, invalid
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12}
	if credentials.TLS != nil {
		if credentials.TLS.InsecureSkipVerify || credentials.TLS.MaxVersion != 0 && credentials.TLS.MaxVersion < tls.VersionTLS12 {
			return nil, invalid
		}
		config = credentials.TLS.Clone()
		config.MinVersion = max(config.MinVersion, tls.VersionTLS12)
		if config.RootCAs != nil {
			config.RootCAs = config.RootCAs.Clone()
		}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableCompression = true
	transport.TLSClientConfig = config
	transport.TLSHandshakeTimeout = 5 * time.Second
	transport.ResponseHeaderTimeout = l.Timeout
	transport.MaxConnsPerHost = 2
	transport.MaxResponseHeaderBytes = 64 << 10
	return &Engine{source: s, limits: l, origin: origin, function: function, region: region, bucketPath: path, credentials: aws.Credentials{AccessKeyID: credentials.AccessKeyID, SecretAccessKey: credentials.SecretAccessKey, SessionToken: credentials.SessionToken}, client: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, signer: v4.NewSigner()}, nil
}

func RegionalOrigin(region string) string {
	suffix := "amazonaws.com"
	if strings.HasPrefix(region, "cn-") {
		suffix += ".cn"
	}
	return "https://lambda." + region + "." + suffix
}

func ValidBucketPath(path string) bool {
	if len(path) == 0 || len(path) > 4096 || !strings.HasPrefix(path, "/") || !utf8.ValidString(path) || strings.ContainsAny(path, "\x00\r\n\\") {
		return false
	}
	for _, segment := range strings.Split(path, "/") {
		if segment == ".." || segment == "." {
			return false
		}
	}
	return true
}

func (e *Engine) Close() error { e.client.CloseIdleConnections(); return nil }

type lambdaEvent struct {
	RawPath        string `json:"rawPath"`
	RequestContext struct {
		HTTP struct {
			Method string `json:"method"`
		} `json:"http"`
	} `json:"requestContext"`
	Body string `json:"body"`
}

// ValidateStatement lets the operation adapter check every batch slot before
// any write. The encoded event, including escaping and rawPath, must fit AWS.
func (e *Engine) ValidateStatement(statement string) error {
	_, err := e.statementPayload(statement)
	return err
}

func (e *Engine) statementPayload(statement string) ([]byte, error) {
	if e == nil || len(statement) == 0 || len(statement) > 1<<20 || !utf8.ValidString(statement) || strings.ContainsRune(statement, 0) {
		return nil, query.NewError("INVALID_ARGUMENT", "Invalid Lambda statement")
	}
	event := lambdaEvent{RawPath: e.bucketPath, Body: statement}
	event.RequestContext.HTTP.Method = http.MethodPost
	payload, err := json.Marshal(event)
	if err != nil || len(payload) > maxPayload {
		return nil, query.NewError("RESOURCE_EXHAUSTED", "Lambda request exceeds payload limit")
	}
	return payload, nil
}

func (e *Engine) invoke(parent context.Context, statement string) (string, int64, error) {
	if parent == nil {
		return "", 0, query.NewError("INVALID_ARGUMENT", "Invalid Lambda statement")
	}
	payload, err := e.statementPayload(statement)
	if err != nil {
		return "", 0, err
	}
	ctx, cancel := context.WithTimeout(parent, e.limits.Timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return "", 0, err
	}
	endpoint := e.origin + "/2015-03-31/functions/" + url.PathEscape(e.function) + "/invocations"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return "", 0, query.NewError("QUERY_FAILED", "Cannot prepare Lambda request")
	}
	request.GetBody = nil
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Amz-Invocation-Type", "RequestResponse")
	hash := sha256.Sum256(payload)
	if err := e.signer.SignHTTP(ctx, e.credentials, request, hex.EncodeToString(hash[:]), "lambda", e.region, time.Now()); err != nil {
		return "", 0, query.NewError("QUERY_FAILED", "Cannot sign Lambda request")
	}
	response, err := e.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return "", 0, ctx.Err()
		}
		return "", 0, query.NewError("QUERY_FAILED", "Lambda request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("X-Amz-Function-Error") != "" {
		return "", 0, query.NewError("QUERY_FAILED", "Lambda execution failed")
	}
	if encoding := response.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return "", 0, query.NewError("QUERY_FAILED", "Unsupported Lambda response encoding")
	}
	limit := min(int64(maxPayload), e.limits.MaxBytes, int64(e.limits.MemoryMB)<<18)
	data, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	wire := int64(len(data))
	if err != nil {
		return "", wire, query.NewError("QUERY_FAILED", "Lambda response was incomplete")
	}
	if wire > limit {
		return "", wire, query.NewError("RESOURCE_EXHAUSTED", "Lambda response exceeds byte limit")
	}
	var result struct {
		StatusCode      int     `json:"statusCode"`
		Body            *string `json:"body"`
		IsBase64Encoded bool    `json:"isBase64Encoded"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if !utf8.Valid(data) || decoder.Decode(&result) != nil || decoder.Decode(new(any)) != io.EOF || result.StatusCode != http.StatusOK || result.Body == nil || result.IsBase64Encoded || !utf8.ValidString(*result.Body) {
		return "", wire, query.NewError("QUERY_FAILED", "Lambda returned an invalid acknowledgement")
	}
	return *result.Body, wire, nil
}

// ApplyStatement receives one classified autocommit operation. The old Lambda
// response has no affected-row count, so a successful acknowledgement returns nil.
func (e *Engine) ApplyStatement(ctx context.Context, statement string) (*int64, error) {
	_, _, err := e.invoke(ctx, statement)
	return nil, err
}

func (e *Engine) Execute(ctx context.Context, r query.Request, sink query.Sink) (stats query.Stats, err error) {
	started := time.Now()
	defer func() { stats.Backend = "clickhouse_lambda"; stats.DurationNS = time.Since(started).Nanoseconds() }()
	if query.ValidateRequest(r) != nil || r.Mode != "native" || r.ConnectionID != e.source.ID || len(r.Sources) != 0 || sink == nil {
		return stats, query.NewError("INVALID_ARGUMENT", "Invalid Lambda read request")
	}
	if len(r.Parameters) != 0 || r.Mongo != nil {
		return stats, query.NewError("UNSUPPORTED", "ClickHouse Lambda does not support bound parameters")
	}
	statement, err := sqlguard.ReadOnly(r.SQL)
	if err != nil {
		return stats, err
	}
	body, wire, err := e.invoke(ctx, statement)
	stats.WireBytes = wire
	if err != nil {
		return stats, err
	}
	stats.PrepareNS = time.Since(started).Nanoseconds()
	rows, err := writeTSV(ctx, body, e.limits, sink)
	stats.Rows, stats.Bytes, stats.Batches = rows.Rows, rows.Bytes, rows.Batches
	return stats, err
}
