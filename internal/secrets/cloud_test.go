//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package secrets

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"go.yaml.in/yaml/v3"
)

const testAWSSecret = "arn:aws:secretsmanager:us-east-1:123456789012:secret:warehouse-ABC123"
const testAWSVersion = "0123456789abcdef0123456789abcdef"
const testAzureVersion = "0123456789abcdef0123456789abcdef"

func cloudConfig(t *testing.T, kind string) Config {
	t.Helper()
	credentials := cloudCredentials{Token: "fixture-master-token", ExpiresAt: time.Now().Add(time.Hour)}
	provider := CloudProvider{Type: kind}
	ref := Reference{Provider: "cloud"}
	switch kind {
	case "aws_secrets_manager":
		credentials.Token = ""
		credentials.AccessKeyID = "fixture-access-key"
		credentials.SecretAccessKey = "fixture-secret-key"
		credentials.SessionToken = "fixture-session-token"
		provider.Region = "us-east-1"
		ref.Secret = testAWSSecret
	case "azure_key_vault":
		provider.Vault = "fixture-vault"
		ref.Secret = "warehouse"
	case "gcp_secret_manager":
		ref.Secret = "projects/123456789012/secrets/warehouse"
	default:
		t.Fatal(kind)
	}
	raw, err := yaml.Marshal(credentials)
	if err != nil {
		t.Fatal(err)
	}
	provider.CredentialsFile = privateSecret(t, string(raw))
	return Config{Providers: map[string]CloudProvider{"cloud": provider}, References: map[string]Reference{"KEY": ref}}
}

type routeCloudTransport struct {
	target *url.URL
	base   http.RoundTripper
}

func (r routeCloudTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	copy := request.Clone(request.Context())
	copyURL := *request.URL
	copy.URL = &copyURL
	copy.Host = request.URL.Host
	copy.URL.Scheme, copy.URL.Host = r.target.Scheme, r.target.Host
	return r.base.RoundTrip(copy)
}
func (r routeCloudTransport) CloseIdleConnections() {
	if c, ok := r.base.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}

func cloudFixture(t *testing.T, config Config, handler http.HandlerFunc) *Provider {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	p, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	origin, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	for _, provider := range p.providers {
		provider.(*cloudBackend).http.Transport = routeCloudTransport{origin, server.Client().Transport}
	}
	return p
}

func cloudResponse(kind string, ref Reference, value string) map[string]any {
	switch kind {
	case "aws_secrets_manager":
		return map[string]any{"ARN": ref.Secret, "VersionId": testAWSVersion, "VersionStages": []string{"AWSCURRENT"}, "SecretString": value}
	case "azure_key_vault":
		return map[string]any{"id": "https://fixture-vault.vault.azure.net/secrets/" + ref.Secret + "/" + testAzureVersion, "value": value, "attributes": map[string]any{"enabled": true}}
	case "gcp_secret_manager":
		return map[string]any{"name": ref.Secret + "/versions/7", "payload": map[string]any{"data": base64.StdEncoding.EncodeToString([]byte(value)), "dataCrc32c": strconv.FormatUint(uint64(crc32.Checksum([]byte(value), crc32.MakeTable(crc32.Castagnoli))), 10)}}
	}
	panic("unknown fixture")
}

func TestCloudProtocolsPreserveExactValuesAndScope(t *testing.T) {
	for _, kind := range []string{"aws_secrets_manager", "azure_key_vault", "gcp_secret_manager"} {
		t.Run(kind, func(t *testing.T) {
			config := cloudConfig(t, kind)
			ref := config.References["KEY"]
			value := "database-value\n世界"
			var calls atomic.Int32
			p := cloudFixture(t, config, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if kind == "aws_secrets_manager" {
					if r.Method != http.MethodPost || r.Host != "secretsmanager.us-east-1.amazonaws.com" || r.Header.Get("X-Amz-Target") != "secretsmanager.GetSecretValue" {
						t.Error("wrong AWS endpoint or operation")
					}
					raw, _ := io.ReadAll(r.Body)
					var payload map[string]string
					if json.Unmarshal(raw, &payload) != nil || payload["SecretId"] != ref.Secret || payload["VersionStage"] != "AWSCURRENT" || len(payload) != 2 {
						t.Error("wrong AWS secret selector")
					}
					stamp, err := time.Parse("20060102T150405Z", r.Header.Get("X-Amz-Date"))
					if err != nil {
						t.Error("missing AWS signed date")
					}
					want := r.Header.Get("Authorization")
					copy := r.Clone(context.Background())
					copy.URL.Scheme, copy.URL.Host = "https", r.Host
					parts := strings.SplitN(want, "SignedHeaders=", 2)
					if len(parts) != 2 {
						t.Error("AWS request is unsigned")
						return
					}
					signed := map[string]bool{}
					for _, name := range strings.Split(strings.SplitN(parts[1], ",", 2)[0], ";") {
						signed[name] = true
					}
					// HTTP transports may add unsigned headers after signing.
					for name := range copy.Header {
						if !signed[strings.ToLower(name)] {
							copy.Header.Del(name)
						}
					}
					digest := sha256.Sum256(raw)
					err = v4.NewSigner().SignHTTP(context.Background(), aws.Credentials{AccessKeyID: "fixture-access-key", SecretAccessKey: "fixture-secret-key", SessionToken: "fixture-session-token"}, copy, hex.EncodeToString(digest[:]), "secretsmanager", "us-east-1", stamp)
					if err != nil || want != copy.Header.Get("Authorization") {
						t.Error("invalid AWS signature")
					}
				} else {
					if r.Header.Get("Authorization") != "Bearer fixture-master-token" {
						t.Error("wrong explicit bearer token")
					}
					if kind == "azure_key_vault" && (r.Host != "fixture-vault.vault.azure.net" || r.URL.Path != "/secrets/warehouse" || r.URL.Query().Get("api-version") != "2025-07-01") {
						t.Error("wrong Azure selector")
					}
					if kind == "gcp_secret_manager" && (r.Host != "secretmanager.googleapis.com" || r.URL.Path != "/v1/"+ref.Secret+"/versions/latest:access") {
						t.Error("wrong GCP selector")
					}
				}
				json.NewEncoder(w).Encode(cloudResponse(kind, ref, value))
			})
			if got, known, err := p.Resolve(context.Background(), "UNSELECTED"); got != "" || known || err != nil {
				t.Fatal("unknown reference resolved")
			}
			if calls.Load() != 0 {
				t.Fatal("unknown reference used provider authority")
			}
			if got, known, err := p.Resolve(context.Background(), "KEY"); err != nil || !known || got != value {
				t.Fatal("cloud bytes not preserved", err)
			}
			if calls.Load() != 1 {
				t.Fatal("unexpected provider calls", calls.Load())
			}
		})
	}
}

func TestCloudResponseRejectionsAndSanitizedErrors(t *testing.T) {
	for _, kind := range []string{"aws_secrets_manager", "azure_key_vault", "gcp_secret_manager"} {
		for _, failure := range []string{"denied", "redirect", "oversize", "duplicate", "wrong_resource", "nul", "value_limit", "invalid_payload"} {
			t.Run(kind+"/"+failure, func(t *testing.T) {
				config := cloudConfig(t, kind)
				ref := config.References["KEY"]
				p := cloudFixture(t, config, func(w http.ResponseWriter, r *http.Request) {
					payload := cloudResponse(kind, ref, "private-result")
					switch failure {
					case "denied":
						w.WriteHeader(http.StatusForbidden)
						io.WriteString(w, "private-result private-url fixture-master-token")
						return
					case "redirect":
						w.Header().Set("Location", "https://redirect.invalid/private")
						w.WriteHeader(http.StatusFound)
						return
					case "oversize":
						io.WriteString(w, strings.Repeat(" ", maxCloudResponseBytes+1))
						return
					case "duplicate":
						io.WriteString(w, `{"value":"one","Value":"private-result"}`)
						return
					case "nul":
						payload = cloudResponse(kind, ref, "a\x00b")
					case "value_limit":
						payload = cloudResponse(kind, ref, strings.Repeat("x", MaxValueBytes+1))
					case "wrong_resource":
						switch kind {
						case "aws_secrets_manager":
							payload["ARN"] = testAWSSecret + "wrong"
						case "azure_key_vault":
							payload["id"] = "https://other.vault.azure.net/secrets/warehouse/" + testAzureVersion
						case "gcp_secret_manager":
							payload["name"] = "projects/999999/secrets/warehouse/versions/7"
						}
					case "invalid_payload":
						switch kind {
						case "aws_secrets_manager":
							payload["SecretBinary"] = "YWJj"
						case "azure_key_vault":
							payload["attributes"] = map[string]any{"enabled": false}
						case "gcp_secret_manager":
							payload["payload"].(map[string]any)["dataCrc32c"] = "0"
						}
					}
					json.NewEncoder(w).Encode(payload)
				})
				value, known, err := p.Resolve(context.Background(), "KEY")
				if value != "" || !known || !errors.Is(err, ErrUnavailable) || err.Error() != ErrUnavailable.Error() {
					t.Fatal("unsafe cloud response exposed data or detail", err)
				}
			})
		}
	}
}

func TestCloudCredentialExpiryRotationAndBounds(t *testing.T) {
	for _, failure := range []string{"expired", "missing_expiry", "excess_lifetime", "unknown_field", "unsafe_mode", "missing", "oversize"} {
		t.Run(failure, func(t *testing.T) {
			config := cloudConfig(t, "gcp_secret_manager")
			path := config.Providers["cloud"].CredentialsFile
			raw := []byte("token: fixture-master-token\nexpires_at: " + time.Now().Add(time.Hour).Format(time.RFC3339Nano) + "\n")
			switch failure {
			case "expired":
				raw = []byte("token: fixture-master-token\nexpires_at: " + time.Now().Add(-time.Second).Format(time.RFC3339Nano) + "\n")
			case "missing_expiry":
				raw = []byte("token: fixture-master-token\n")
			case "excess_lifetime":
				raw = []byte("token: fixture-master-token\nexpires_at: " + time.Now().Add(25*time.Hour).Format(time.RFC3339Nano) + "\n")
			case "unknown_field":
				raw = append(raw, []byte("private_field: private-value\n")...)
			case "oversize":
				raw = []byte(strings.Repeat("x", MaxValueBytes+1))
			}
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			if failure == "unsafe_mode" {
				os.Chmod(path, 0640)
			}
			if failure == "missing" {
				os.Remove(path)
			}
			var calls atomic.Int32
			p := cloudFixture(t, config, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(500) })
			if value, known, err := p.Resolve(context.Background(), "KEY"); value != "" || !known || !errors.Is(err, ErrUnavailable) {
				t.Fatal("invalid master credential accepted", err)
			}
			if calls.Load() != 0 {
				t.Fatal("invalid master credential used on network")
			}
		})
	}
	t.Run("cached credential expiration", func(t *testing.T) {
		config := cloudConfig(t, "gcp_secret_manager")
		config.TTL = time.Minute
		expires := time.Now().Add(250 * time.Millisecond)
		path := config.Providers["cloud"].CredentialsFile
		os.WriteFile(path, []byte("token: first-master-token\nexpires_at: "+expires.Format(time.RFC3339Nano)+"\n"), 0600)
		var calls atomic.Int32
		p := cloudFixture(t, config, func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			json.NewEncoder(w).Encode(cloudResponse("gcp_secret_manager", config.References["KEY"], "source-value"))
		})
		if _, _, err := p.Resolve(context.Background(), "KEY"); err != nil {
			t.Fatal(err)
		}
		p.mu.Lock()
		cached := p.cache["KEY"].expires
		p.mu.Unlock()
		// These wall timestamps were sampled around filesystem/network work;
		// allow sub-millisecond host clock slewing. The deterministic lease
		// test below checks the exact monotonic bound without that fixture noise.
		if cached.Round(0).Sub(expires.Round(0)) > time.Millisecond {
			t.Fatal("cache outlived credential")
		}
		time.Sleep(time.Until(expires) + time.Millisecond)
		if value, known, err := p.Resolve(context.Background(), "KEY"); value != "" || !known || !errors.Is(err, ErrUnavailable) {
			t.Fatal("served past credential expiry", err)
		}
		if calls.Load() != 1 {
			t.Fatal("expired credential sent to cloud")
		}
	})
	t.Run("atomic master rotation", func(t *testing.T) {
		config := cloudConfig(t, "azure_key_vault")
		path := config.Providers["cloud"].CredentialsFile
		p := cloudFixture(t, config, func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(cloudResponse("azure_key_vault", config.References["KEY"], strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")))
		})
		if value, _, err := p.Resolve(context.Background(), "KEY"); err != nil || value != "fixture-master-token" {
			t.Fatal(err)
		}
		raw := []byte("token: rotated-master-token\nexpires_at: " + time.Now().Add(time.Hour).Format(time.RFC3339Nano) + "\n")
		if os.WriteFile(path+".new", raw, 0600) != nil || os.Rename(path+".new", path) != nil {
			t.Fatal("could not rotate fixture")
		}
		if value, _, err := p.Resolve(context.Background(), "KEY"); err != nil || value != "rotated-master-token" {
			t.Fatal("master rotation not observed", err)
		}
	})
}

func TestCloudTimeoutCoalescingAndInvalidationFence(t *testing.T) {
	config := cloudConfig(t, "gcp_secret_manager")
	config.Timeout = 50 * time.Millisecond
	p := cloudFixture(t, config, func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() })
	start := time.Now()
	if value, known, err := p.Resolve(context.Background(), "KEY"); value != "" || !known || !errors.Is(err, ErrUnavailable) {
		t.Fatal("lookup timeout not fail closed", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("lookup exceeded bounded timeout")
	}

	config = cloudConfig(t, "gcp_secret_manager")
	config.TTL = time.Minute
	release := make(chan struct{})
	var calls atomic.Int32
	p = cloudFixture(t, config, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			<-release
		}
		json.NewEncoder(w).Encode(cloudResponse("gcp_secret_manager", config.References["KEY"], "source-value"))
	})
	const waiters = 12
	var group sync.WaitGroup
	results := make(chan error, waiters)
	for range waiters {
		group.Add(1)
		go func() {
			defer group.Done()
			value, known, err := p.Resolve(context.Background(), "KEY")
			if value != "" || !known || !errors.Is(err, ErrUnavailable) {
				results <- errors.New("invalidated flight delivered secret")
			}
		}()
	}
	waitSecretCondition(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return calls.Load() == 1 && p.calls["KEY"] != nil && p.calls["KEY"].waiters == waiters
	})
	if !p.Invalidate("KEY") || p.Invalidate("UNMAPPED") {
		t.Fatal("invalid invalidation mapping")
	}
	close(release)
	group.Wait()
	close(results)
	for err := range results {
		t.Error(err)
	}
	waitSecretCondition(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); f := p.calls["KEY"]; return f == nil || f.complete })
	if value, known, err := p.Resolve(context.Background(), "KEY"); err != nil || !known || value != "source-value" {
		t.Fatal("fresh read did not recover", err)
	}
	if calls.Load() != 2 {
		t.Fatal("coalescing or fresh lookup failed", calls.Load())
	}
	if !p.Invalidate("KEY") {
		t.Fatal("cache invalidation failed")
	}
	if _, _, err := p.Resolve(context.Background(), "KEY"); err != nil || calls.Load() != 3 {
		t.Fatal("invalidated cache reused", err)
	}
}

func TestCloudConfigurationRejectsImplicitAuthority(t *testing.T) {
	for _, mutate := range []func(*Config){
		func(c *Config) {
			cfg := c.Providers["cloud"]
			cfg.CredentialsFile = "relative"
			c.Providers["cloud"] = cfg
		},
		func(c *Config) {
			cfg := c.Providers["cloud"]
			cfg.Region = "us-east-1.attacker.invalid"
			c.Providers["cloud"] = cfg
		},
		func(c *Config) { cfg := c.Providers["cloud"]; cfg.Type = "ambient"; c.Providers["cloud"] = cfg },
		func(c *Config) { c.Files = map[string]string{"KEY": "/private/path"} },
		func(c *Config) { ref := c.References["KEY"]; ref.Secret = "warehouse"; c.References["KEY"] = ref },
		func(c *Config) {
			ref := c.References["KEY"]
			ref.Secret = strings.ReplaceAll(ref.Secret, "us-east-1", "us-west-2")
			c.References["KEY"] = ref
		},
		func(c *Config) { ref := c.References["KEY"]; ref.Version = "latest"; c.References["KEY"] = ref },
		func(c *Config) { c.Timeout = MaxTimeout + 1 },
		func(c *Config) { c.Providers["unused"] = c.Providers["cloud"] },
	} {
		config := cloudConfig(t, "aws_secrets_manager")
		mutate(&config)
		if p, err := New(config); !errors.Is(err, ErrInvalid) {
			if p != nil {
				p.Close()
			}
			t.Fatal("implicit or ambiguous cloud authority accepted", err)
		}
	}
	config := cloudConfig(t, "gcp_secret_manager")
	config.Files = map[string]string{"FILE": privateSecret(t, "file")}
	keys := config.Keys()
	if strings.Join(keys, ",") != "FILE,KEY" {
		t.Fatal("wrong configured source references", keys)
	}
	provider, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	backend := provider.providers["cloud"].(*cloudBackend)
	transport := backend.http.Transport.(*http.Transport)
	if transport.Proxy != nil || !transport.DisableCompression || transport.TLSClientConfig.InsecureSkipVerify || backend.http.Timeout != DefaultTimeout {
		t.Fatal("unsafe cloud transport defaults")
	}
	config.References["KEY"] = Reference{}
	config.Providers["cloud"] = CloudProvider{}
	if provider.references["KEY"].Secret == "" || backend.config.CredentialsFile == "" {
		t.Fatal("configuration not copied")
	}
}

func TestCloudPinnedVersionsAndBinaryPayload(t *testing.T) {
	for _, kind := range []string{"aws_secrets_manager", "azure_key_vault", "gcp_secret_manager"} {
		t.Run(kind, func(t *testing.T) {
			config := cloudConfig(t, kind)
			ref := config.References["KEY"]
			switch kind {
			case "aws_secrets_manager":
				ref.Version = testAWSVersion
			case "azure_key_vault":
				ref.Version = testAzureVersion
			case "gcp_secret_manager":
				ref.Version = "7"
			}
			config.References["KEY"] = ref
			var wrong atomic.Bool
			p := cloudFixture(t, config, func(w http.ResponseWriter, r *http.Request) {
				response := cloudResponse(kind, ref, "binary\xff-value")
				// Binary source values are meaningful only through explicit
				// base64 payloads, not invalid UTF-8 JSON strings.
				if kind == "aws_secrets_manager" {
					delete(response, "SecretString")
					response["SecretBinary"] = base64.StdEncoding.EncodeToString([]byte("binary\xff-value"))
					var request map[string]string
					if json.NewDecoder(r.Body).Decode(&request) != nil || request["VersionId"] != ref.Version || request["VersionStage"] != "" {
						t.Error("pinned AWS version not requested")
					}
					if wrong.Load() {
						response["VersionId"] = strings.Repeat("a", 32)
					}
				} else if kind == "azure_key_vault" {
					response["value"] = "azure-value"
					if !strings.HasSuffix(r.URL.Path, "/"+ref.Version) {
						t.Error("pinned Azure version not requested")
					}
					if wrong.Load() {
						response["id"] = "https://fixture-vault.vault.azure.net/secrets/warehouse/" + strings.Repeat("a", 32)
					}
				} else {
					if !strings.HasSuffix(r.URL.Path, "/7:access") {
						t.Error("pinned Google version not requested")
					}
					if wrong.Load() {
						response["name"] = ref.Secret + "/versions/8"
					}
				}
				json.NewEncoder(w).Encode(response)
			})
			want := "binary\xff-value"
			if kind == "azure_key_vault" {
				want = "azure-value"
			}
			if value, known, err := p.Resolve(context.Background(), "KEY"); err != nil || !known || value != want {
				t.Fatal("pinned payload changed", err)
			}
			wrong.Store(true)
			if value, known, err := p.Resolve(context.Background(), "KEY"); value != "" || !known || !errors.Is(err, ErrUnavailable) {
				t.Fatal("wrong pinned version accepted", err)
			}
		})
	}
}

func TestAzureProviderValidityCapsCache(t *testing.T) {
	for _, kind := range []string{"expired", "not_yet_valid", "missing_enabled", "cache_expiry"} {
		t.Run(kind, func(t *testing.T) {
			config := cloudConfig(t, "azure_key_vault")
			config.TTL = time.Minute
			expires := time.Now().Add(2 * time.Second).Unix()
			p := cloudFixture(t, config, func(w http.ResponseWriter, r *http.Request) {
				response := cloudResponse("azure_key_vault", config.References["KEY"], "valid-source")
				attributes := response["attributes"].(map[string]any)
				switch kind {
				case "expired":
					attributes["exp"] = time.Now().Add(-time.Second).Unix()
				case "not_yet_valid":
					attributes["nbf"] = time.Now().Add(time.Hour).Unix()
				case "missing_enabled":
					delete(attributes, "enabled")
				case "cache_expiry":
					attributes["exp"] = expires
				}
				json.NewEncoder(w).Encode(response)
			})
			value, known, err := p.Resolve(context.Background(), "KEY")
			if kind != "cache_expiry" {
				if value != "" || !known || !errors.Is(err, ErrUnavailable) {
					t.Fatal("provider validity not enforced", err)
				}
				return
			}
			if err != nil || !known || value != "valid-source" {
				t.Fatal(err)
			}
			p.mu.Lock()
			cached := p.cache["KEY"].expires
			p.mu.Unlock()
			if cached.Round(0).Sub(time.Unix(expires, 0)) > time.Millisecond {
				t.Fatal("cache exceeds provider response expiry")
			}
			time.Sleep(time.Until(time.Unix(expires, 0)) + time.Millisecond)
			if value, known, err = p.Resolve(context.Background(), "KEY"); value != "" || !known || !errors.Is(err, ErrUnavailable) {
				t.Fatal("expired provider response served from cache", err)
			}
		})
	}
}

func TestSecretReadsHaveBoundedConcurrency(t *testing.T) {
	config := Config{Files: map[string]string{}}
	path := privateSecret(t, "unused")
	for i := range 2 * MaxConcurrentLookups {
		config.Files[strconv.Itoa(i)] = path
	}
	p, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	release := make(chan struct{})
	var active, maximum atomic.Int32
	p.read = func(context.Context, string) ([]byte, error) {
		n := active.Add(1)
		for old := maximum.Load(); n > old; old = maximum.Load() {
			if maximum.CompareAndSwap(old, n) {
				break
			}
		}
		<-release
		active.Add(-1)
		return []byte("bounded"), nil
	}
	var group sync.WaitGroup
	for key := range config.Files {
		group.Add(1)
		go func() {
			defer group.Done()
			if value, known, err := p.Resolve(context.Background(), key); err != nil || !known || value != "bounded" {
				t.Error("bounded read failed", err)
			}
		}()
	}
	waitSecretCondition(t, func() bool { return active.Load() == MaxConcurrentLookups })
	close(release)
	group.Wait()
	if maximum.Load() != MaxConcurrentLookups {
		t.Fatal("concurrent read bound violated", maximum.Load())
	}
}

func TestCloudExpiryCannotExtendAcrossWallClockAdjustment(t *testing.T) {
	loaded := time.Now()
	credential := loaded.Add(time.Minute)
	completed := loaded.Add(20 * time.Second)
	for _, adjustment := range []time.Duration{-time.Hour, time.Hour} {
		wall := completed.Round(0).Add(adjustment)
		deadline, ok := cloudExpiry(completed, wall, credential, time.Time{})
		if !ok || deadline.Sub(completed) != 40*time.Second {
			t.Fatal("wall change extended credential lease")
		}
		deadline, ok = cloudExpiry(completed, wall, credential, wall.Add(10*time.Second))
		if !ok || deadline.Sub(completed) != 10*time.Second {
			t.Fatal("provider expiry did not shorten credential lease")
		}
		deadline, ok = cloudExpiry(completed, wall, credential, wall.Add(2*time.Hour))
		if !ok || deadline.Sub(completed) != 40*time.Second {
			t.Fatal("provider wall expiry extended anchored credential")
		}
		if _, ok = cloudExpiry(completed, wall, credential, wall.Add(-time.Second)); ok {
			t.Fatal("expired provider lease accepted")
		}
	}
	if _, ok := cloudExpiry(loaded.Add(61*time.Second), loaded.Round(0).Add(-time.Hour), credential, time.Time{}); ok {
		t.Fatal("expired credential restored by clock rollback")
	}
}
