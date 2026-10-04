// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package secrets

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

const (
	MaxProviders          = 16
	MaxCredentialLifetime = 24 * time.Hour
	maxCloudResponseBytes = 128 << 10
)

// CloudProvider is operator-owned configuration. CredentialsFile is never part
// of a source catalog, a query request, or a child's environment.
type CloudProvider struct {
	Type            string `yaml:"type"`
	CredentialsFile string `yaml:"credentials_file"`
	Region          string `yaml:"region,omitempty"`
	Vault           string `yaml:"vault,omitempty"`
}

type Reference struct {
	Provider string `yaml:"provider"`
	Secret   string `yaml:"secret"`
	Version  string `yaml:"version,omitempty"`
}

type secretValue struct {
	data    []byte
	expires time.Time
}
type backend interface {
	Fetch(context.Context, Reference) (secretValue, error)
	Close()
}

type cloudBackend struct {
	config CloudProvider
	origin string
	http   *http.Client
}

var (
	providerID   = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)
	awsRegion    = regexp.MustCompile(`^[a-z]{2}(-[a-z]+)+-[1-9][0-9]*$`)
	awsSecretARN = regexp.MustCompile(`^arn:(aws|aws-cn|aws-us-gov):secretsmanager:([a-z0-9-]+):[0-9]{12}:secret:[A-Za-z0-9/_+=.@-]{1,512}-[A-Za-z0-9]{6}$`)
	awsVersion   = regexp.MustCompile(`^[A-Za-z0-9-]{32,64}$`)
	azureVault   = regexp.MustCompile(`^[a-z][a-z0-9-]{1,22}[a-z0-9]$`)
	azureSecret  = regexp.MustCompile(`^[A-Za-z0-9-]{1,127}$`)
	azureVersion = regexp.MustCompile(`^[a-f0-9]{32}$`)
	// Access responses canonicalize project IDs to project numbers. Require the
	// number explicitly so a response cannot silently switch authorized scope.
	gcpSecret  = regexp.MustCompile(`^projects/[1-9][0-9]{0,19}/secrets/[A-Za-z0-9_-]{1,255}$`)
	gcpVersion = regexp.MustCompile(`^[1-9][0-9]{0,19}$`)
)

func configuredBackends(config Config) (map[string]backend, map[string]Reference, error) {
	if len(config.Providers) > MaxProviders {
		return nil, nil, ErrInvalid
	}
	providers := make(map[string]backend, len(config.Providers))
	refs := make(map[string]Reference, len(config.References))
	used := map[string]bool{}
	for key, ref := range config.References {
		_, duplicate := config.Files[key]
		cfg, exists := config.Providers[ref.Provider]
		if key == "" || len(key) > 256 || duplicate || !exists || !validReference(cfg, ref) {
			return nil, nil, ErrInvalid
		}
		refs[key], used[ref.Provider] = ref, true
	}
	for name, cfg := range config.Providers {
		if !providerID.MatchString(name) || !used[name] || !validCredentialPath(cfg.CredentialsFile) {
			return nil, nil, ErrInvalid
		}
		if _, err := cloudOrigin(cfg); err != nil {
			return nil, nil, ErrInvalid
		}
	}
	for name, cfg := range config.Providers {
		origin, _ := cloudOrigin(cfg)
		timeout := config.Timeout
		if timeout == 0 {
			timeout = DefaultTimeout
		}
		transport := &http.Transport{
			Proxy: nil, DialContext: (&net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}).DialContext,
			ForceAttemptHTTP2: true, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
			TLSHandshakeTimeout: timeout, ResponseHeaderTimeout: timeout, MaxResponseHeaderBytes: 16 << 10,
			MaxConnsPerHost: MaxConcurrentLookups, MaxIdleConns: MaxConcurrentLookups, MaxIdleConnsPerHost: MaxConcurrentLookups,
			IdleConnTimeout: 30 * time.Second, DisableCompression: true,
		}
		providers[name] = &cloudBackend{config: cfg, origin: origin, http: &http.Client{Transport: transport, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	}
	return providers, refs, nil
}

func validCredentialPath(path string) bool {
	return path != "" && len(path) <= 4096 && filepath.IsAbs(path) && filepath.Clean(path) == path && path != string(filepath.Separator)
}

func awsPartition(region string) string {
	if strings.HasPrefix(region, "cn-") {
		return "aws-cn"
	}
	if strings.HasPrefix(region, "us-gov-") {
		return "aws-us-gov"
	}
	return "aws"
}

func cloudOrigin(cfg CloudProvider) (string, error) {
	switch cfg.Type {
	case "aws_secrets_manager":
		if len(cfg.Region) > 32 || !awsRegion.MatchString(cfg.Region) || cfg.Vault != "" {
			return "", ErrInvalid
		}
		suffix := "amazonaws.com"
		if awsPartition(cfg.Region) == "aws-cn" {
			suffix += ".cn"
		}
		return "https://secretsmanager." + cfg.Region + "." + suffix, nil
	case "azure_key_vault":
		if !azureVault.MatchString(cfg.Vault) || strings.Contains(cfg.Vault, "--") || cfg.Region != "" {
			return "", ErrInvalid
		}
		return "https://" + cfg.Vault + ".vault.azure.net", nil
	case "gcp_secret_manager":
		if cfg.Region != "" || cfg.Vault != "" {
			return "", ErrInvalid
		}
		return "https://secretmanager.googleapis.com", nil
	default:
		return "", ErrInvalid
	}
}

func validReference(cfg CloudProvider, ref Reference) bool {
	if len(ref.Secret) > 1024 || len(ref.Version) > 64 {
		return false
	}
	switch cfg.Type {
	case "aws_secrets_manager":
		matches := awsSecretARN.FindStringSubmatch(ref.Secret)
		return len(matches) == 3 && matches[1] == awsPartition(cfg.Region) && matches[2] == cfg.Region && (ref.Version == "" || awsVersion.MatchString(ref.Version))
	case "azure_key_vault":
		return azureSecret.MatchString(ref.Secret) && (ref.Version == "" || azureVersion.MatchString(ref.Version))
	case "gcp_secret_manager":
		return gcpSecret.MatchString(ref.Secret) && (ref.Version == "" || gcpVersion.MatchString(ref.Version))
	default:
		return false
	}
}

type cloudCredentials struct {
	AccessKeyID     string    `yaml:"access_key_id,omitempty"`
	SecretAccessKey string    `yaml:"secret_access_key,omitempty"`
	SessionToken    string    `yaml:"session_token,omitempty"`
	Token           string    `yaml:"token,omitempty"`
	ExpiresAt       time.Time `yaml:"expires_at"`
}

func (c *cloudBackend) credentials(ctx context.Context) (cloudCredentials, error) {
	var credentials cloudCredentials
	raw, err := readPrivateDocument(ctx, c.config.CredentialsFile, MaxValueBytes)
	if err != nil {
		return credentials, ErrUnavailable
	}
	defer wipe(raw)
	var node yaml.Node
	if yaml.Unmarshal(raw, &node) != nil {
		return credentials, ErrUnavailable
	}
	nodes := 0
	var bounded func(*yaml.Node, int) bool
	bounded = func(n *yaml.Node, depth int) bool {
		nodes++
		if nodes > 32 || depth > 4 || n.Anchor != "" || n.Kind == yaml.AliasNode || n.Tag == "!!merge" {
			return false
		}
		for _, child := range n.Content {
			if !bounded(child, depth+1) {
				return false
			}
		}
		return true
	}
	if !bounded(&node, 0) {
		return credentials, ErrUnavailable
	}
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if decoder.Decode(&credentials) != nil || decoder.Decode(new(any)) != io.EOF {
		return cloudCredentials{}, ErrUnavailable
	}
	now := time.Now()
	if !credentials.ExpiresAt.After(now) || credentials.ExpiresAt.Sub(now) > MaxCredentialLifetime {
		return cloudCredentials{}, ErrUnavailable
	}
	// Anchor the credential lease before network work. A wall-clock change
	// during the request must not extend this authority.
	credentials.ExpiresAt = now.Add(credentials.ExpiresAt.Sub(now))
	if c.config.Type == "aws_secrets_manager" {
		if !visibleASCII(credentials.AccessKeyID, 16, 128) || !visibleASCII(credentials.SecretAccessKey, 1, 4096) || (credentials.SessionToken != "" && !visibleASCII(credentials.SessionToken, 1, 8192)) || credentials.Token != "" {
			return cloudCredentials{}, ErrUnavailable
		}
	} else if !visibleASCII(credentials.Token, 1, 12288) || credentials.AccessKeyID != "" || credentials.SecretAccessKey != "" || credentials.SessionToken != "" {
		return cloudCredentials{}, ErrUnavailable
	}
	return credentials, nil
}

func visibleASCII(value string, minimum, maximum int) bool {
	if len(value) < minimum || len(value) > maximum {
		return false
	}
	for i := range len(value) {
		if value[i] < 33 || value[i] > 126 {
			return false
		}
	}
	return true
}

func (c *cloudBackend) Close() { c.http.CloseIdleConnections() }

func (c *cloudBackend) Fetch(ctx context.Context, ref Reference) (secretValue, error) {
	if !validReference(c.config, ref) {
		return secretValue{}, ErrUnavailable
	}
	credentials, err := c.credentials(ctx)
	if err != nil {
		return secretValue{}, ErrUnavailable
	}
	var result secretValue
	switch c.config.Type {
	case "aws_secrets_manager":
		result, err = c.aws(ctx, ref, credentials)
	case "azure_key_vault":
		result, err = c.azure(ctx, ref, credentials)
	case "gcp_secret_manager":
		result, err = c.gcp(ctx, ref, credentials)
	default:
		err = ErrUnavailable
	}
	now := time.Now()
	expires, valid := cloudExpiry(now, now, credentials.ExpiresAt, result.expires)
	if err != nil || !valid || len(result.data) > MaxValueBytes || bytes.IndexByte(result.data, 0) >= 0 {
		wipe(result.data)
		return secretValue{}, ErrUnavailable
	}
	result.expires = expires
	return result, nil
}

// monoNow and wallNow are the same time sample in production. Separating their
// roles allows deterministic tests of a wall adjustment during a network read.
// credential was anchored before that read; provider timestamps are wall-only.
func cloudExpiry(monoNow, wallNow, credential, response time.Time) (time.Time, bool) {
	remaining := credential.Sub(monoNow)
	if !response.IsZero() {
		remaining = min(remaining, response.Sub(wallNow))
	}
	if remaining <= 0 {
		return time.Time{}, false
	}
	return monoNow.Add(remaining), true
}

func (c *cloudBackend) decode(request *http.Request, target any) error {
	response, err := c.http.Do(request)
	if err != nil {
		return ErrUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.ContentLength > maxCloudResponseBytes || (response.Header.Get("Content-Encoding") != "" && response.Header.Get("Content-Encoding") != "identity") {
		return ErrUnavailable
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxCloudResponseBytes+1))
	if err != nil || len(raw) > maxCloudResponseBytes {
		wipe(raw)
		return ErrUnavailable
	}
	defer wipe(raw)
	if !boundedJSON(raw) || json.Unmarshal(raw, target) != nil {
		return ErrUnavailable
	}
	return nil
}

// Reject duplicate members and deeply nested/oversized response envelopes before
// decoding provider fields. Unknown ordinary members remain forward compatible.
func boundedJSON(raw []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	nodes := 0
	var value func(int) bool
	value = func(depth int) bool {
		nodes++
		if depth > 16 || nodes > 1024 {
			return false
		}
		token, err := decoder.Token()
		if err != nil {
			return false
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return true
		}
		if delim != '{' && delim != '[' {
			return false
		}
		seen := map[string]bool{}
		for decoder.More() {
			if delim == '{' {
				key, err := decoder.Token()
				name, ok := key.(string)
				if err != nil || !ok || seen[strings.ToLower(name)] {
					return false
				}
				seen[strings.ToLower(name)] = true
			}
			if !value(depth + 1) {
				return false
			}
		}
		end, err := decoder.Token()
		return err == nil && ((delim == '{' && end == json.Delim('}')) || (delim == '[' && end == json.Delim(']')))
	}
	if !value(0) {
		return false
	}
	_, err := decoder.Token()
	return err == io.EOF
}
