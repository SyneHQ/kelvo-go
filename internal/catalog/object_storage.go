// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import (
	"errors"
	"net/url"
	"regexp"
	"strings"
)

// ObjectCredentials contains references only. Reader and writer identities are
// configured independently; writer references never enter a query subprocess.
type ObjectCredentials struct {
	AccessKeyIDEnv     string `json:"access_key_id_env,omitempty" yaml:"access_key_id_env,omitempty"`
	SecretAccessKeyEnv string `json:"secret_access_key_env,omitempty" yaml:"secret_access_key_env,omitempty"`
	SessionTokenEnv    string `json:"session_token_env,omitempty" yaml:"session_token_env,omitempty"`
	SASTokenEnv        string `json:"sas_token_env,omitempty" yaml:"sas_token_env,omitempty"`
}

type ObjectLocation struct {
	Provider string `json:"provider" yaml:"provider"`
	Endpoint string `json:"endpoint" yaml:"endpoint"`
	Bucket   string `json:"bucket" yaml:"bucket"`
	Prefix   string `json:"prefix" yaml:"prefix"`
	Region   string `json:"region,omitempty" yaml:"region,omitempty"`
	Account  string `json:"account,omitempty" yaml:"account,omitempty"`
}

type ObjectStorage struct {
	ObjectLocation   `yaml:",inline"`
	ReadCredentials  ObjectCredentials `json:"read_credentials" yaml:"read_credentials"`
	WriteCredentials ObjectCredentials `json:"write_credentials" yaml:"write_credentials"`
}

// ObjectRead is resolved by the parent for one selected immutable generation.
// It carries no write identity or remote manifest path.
type ObjectRead struct {
	Provider    string            `json:"provider" yaml:"provider"`
	Endpoint    string            `json:"endpoint" yaml:"endpoint"`
	Bucket      string            `json:"bucket" yaml:"bucket"`
	Region      string            `json:"region,omitempty" yaml:"region,omitempty"`
	Account     string            `json:"account,omitempty" yaml:"account,omitempty"`
	Key         string            `json:"key" yaml:"key"`
	Credentials ObjectCredentials `json:"credentials" yaml:"credentials"`
}

func (c ObjectCredentials) EnvironmentNames() []string {
	return []string{c.AccessKeyIDEnv, c.SecretAccessKeyEnv, c.SessionTokenEnv, c.SASTokenEnv}
}

var objectBucket = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
var objectSegment = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$`)
var objectRegion = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)+-[0-9]+$`)
var azureAccount = regexp.MustCompile(`^[a-z0-9]{3,24}$`)

func (l ObjectLocation) Validate() error {
	if l.Provider != "s3" && l.Provider != "r2" && l.Provider != "gcs" && l.Provider != "azure" {
		return errors.New("object storage provider must be s3, r2, gcs, or azure")
	}
	u, err := url.Parse(l.Endpoint)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Path != "" || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || len(l.Endpoint) > 512 {
		return errors.New("object storage endpoint must be an HTTPS origin without a path or credentials")
	}
	if !objectBucket.MatchString(l.Bucket) || strings.Contains(l.Bucket, "..") || strings.Contains(l.Bucket, ".-") || strings.Contains(l.Bucket, "-.") {
		return errors.New("object storage requires a valid bucket or container name")
	}
	if err := ValidateObjectKey(l.Prefix); err != nil || len(l.Prefix) > 256 {
		return errors.New("object storage requires a nonempty bounded namespace prefix")
	}
	switch l.Provider {
	case "s3":
		if !objectRegion.MatchString(l.Region) || len(l.Region) > 64 || l.Account != "" {
			return errors.New("S3 requires a region and no Azure account")
		}
	case "r2", "gcs":
		if (l.Region != "" && l.Region != "auto") || l.Account != "" {
			return errors.New("R2 and GCS require automatic region selection and no Azure account")
		}
	case "azure":
		if !azureAccount.MatchString(l.Account) || l.Region != "" || strings.Contains(l.Bucket, ".") || strings.Contains(l.Bucket, "--") {
			return errors.New("Azure storage requires an account and valid container, without a region")
		}
	}
	return nil
}

func ValidateObjectKey(key string) error {
	if key == "" || len(key) > 1024 {
		return errors.New("object key is invalid")
	}
	for _, part := range strings.Split(key, "/") {
		if !objectSegment.MatchString(part) {
			return errors.New("object key must contain plain path components without traversal or globs")
		}
	}
	return nil
}

func (l ObjectLocation) ValidateKey(key string) error {
	if err := ValidateObjectKey(key); err != nil {
		return err
	}
	if !strings.HasPrefix(key, l.Prefix+"/") {
		return errors.New("object key is outside the configured namespace")
	}
	return nil
}

func (c ObjectCredentials) Validate(provider string) error {
	for _, name := range c.EnvironmentNames() {
		if name != "" && (len(name) > 256 || ValidateEnvironment(name) != nil || !strings.HasPrefix(name, "KELVO_SOURCE_")) {
			return errors.New("object credentials require dedicated KELVO_SOURCE_* environment references")
		}
	}
	if provider == "azure" {
		if c.SASTokenEnv == "" || c.AccessKeyIDEnv != "" || c.SecretAccessKeyEnv != "" || c.SessionTokenEnv != "" {
			return errors.New("Azure object credentials require only sas_token_env")
		}
	} else if c.AccessKeyIDEnv == "" || c.SecretAccessKeyEnv == "" || c.SASTokenEnv != "" || (provider != "s3" && c.SessionTokenEnv != "") {
		return errors.New("S3-compatible object credentials require access key and secret references; session tokens are S3-only")
	}
	return nil
}

func (s ObjectStorage) Validate() error {
	if err := s.ObjectLocation.Validate(); err != nil {
		return err
	}
	if err := s.ReadCredentials.Validate(s.Provider); err != nil {
		return err
	}
	if err := s.WriteCredentials.Validate(s.Provider); err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, name := range s.ReadCredentials.EnvironmentNames() {
		if name != "" {
			seen[name] = true
		}
	}
	for _, name := range s.WriteCredentials.EnvironmentNames() {
		if name != "" && seen[name] {
			return errors.New("object readers and publishers require separate credential references")
		}
	}
	return nil
}

func (l ObjectLocation) URI(key string) (string, error) {
	if err := l.Validate(); err != nil {
		return "", err
	}
	if err := l.ValidateKey(key); err != nil {
		return "", err
	}
	return objectURI(l.Provider, l.Account, l.Bucket, key), nil
}

func objectURI(provider, account, bucket, key string) string {
	scheme := map[string]string{"s3": "s3", "r2": "r2", "gcs": "gs", "azure": "az"}[provider]
	if provider == "azure" {
		return scheme + "://" + account + ".blob.core.windows.net/" + bucket + "/" + key
	}
	return scheme + "://" + bucket + "/" + key
}

func (s ObjectStorage) Read(key string) (*ObjectRead, error) {
	if err := s.ObjectLocation.ValidateKey(key); err != nil {
		return nil, err
	}
	r := &ObjectRead{Provider: s.Provider, Endpoint: s.Endpoint, Bucket: s.Bucket, Region: s.Region, Account: s.Account, Key: key, Credentials: s.ReadCredentials}
	return r, r.Validate()
}

func (r ObjectRead) URI() string { return objectURI(r.Provider, r.Account, r.Bucket, r.Key) }

func (r ObjectRead) Validate() error {
	if err := ValidateObjectKey(r.Key); err != nil {
		return err
	}
	parts := strings.Split(r.Key, "/")
	if len(parts) < 2 {
		return errors.New("object reads require an exact namespaced key")
	}
	l := ObjectLocation{Provider: r.Provider, Endpoint: r.Endpoint, Bucket: r.Bucket, Region: r.Region, Account: r.Account, Prefix: parts[0]}
	if err := l.Validate(); err != nil {
		return err
	}
	return r.Credentials.Validate(r.Provider)
}

// ParseAzureSAS accepts an explicit SAS token, never an endpoint or a complete
// connection string. Errors deliberately omit the token and parser diagnostics.
func ParseAzureSAS(raw string) (url.Values, error) {
	invalid := errors.New("Azure SAS token is unavailable or invalid")
	if raw == "" || len(raw) > 16384 || strings.ContainsAny(raw, "\r\n\x00;#") {
		return nil, invalid
	}
	v, err := url.ParseQuery(strings.TrimPrefix(raw, "?"))
	if err != nil {
		return nil, invalid
	}
	allowed := map[string]bool{"sv": true, "ss": true, "srt": true, "sp": true, "st": true, "se": true, "spr": true, "sip": true, "sr": true, "sig": true, "si": true, "skoid": true, "sktid": true, "skt": true, "ske": true, "sks": true, "skv": true, "saoid": true, "suoid": true, "scid": true, "ses": true}
	for k, vs := range v {
		if !allowed[k] || len(vs) != 1 || vs[0] == "" || strings.ContainsAny(vs[0], "\r\n\x00;") {
			return nil, invalid
		}
	}
	if v.Get("sv") == "" || v.Get("sig") == "" || v.Get("sp") == "" || v.Get("se") == "" || (v.Get("sr") == "" && (v.Get("ss") == "" || v.Get("srt") == "")) {
		return nil, invalid
	}
	if p := v.Get("spr"); p != "" && p != "https" {
		return nil, invalid
	}
	return v, nil
}

func ValidateAzureReadSAS(raw string) error {
	v, err := ParseAzureSAS(raw)
	if err != nil {
		return err
	}
	if v.Get("sp") != "r" {
		return errors.New("Azure snapshot readers require a read-only SAS token")
	}
	return nil
}
