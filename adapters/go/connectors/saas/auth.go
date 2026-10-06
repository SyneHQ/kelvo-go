// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package saas

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

type googleSecret struct{ Type, Email, KeyID, PrivateKey, TokenURI, AccessToken string }

// GoogleAccessToken resolves only the supplied operation credential. It never
// reads ambient credentials or caches tokens between tenants or operations.
func GoogleAccessToken(ctx context.Context, credentials, scope string) (string, error) {
	if ctx != nil && ctx.Err() != nil {
		return "", ctx.Err()
	}
	if ctx == nil {
		return "", adapter.ErrInvalid
	}
	switch scope {
	case "https://www.googleapis.com/auth/analytics.readonly", "https://www.googleapis.com/auth/adwords", "https://www.googleapis.com/auth/spreadsheets.readonly", "https://www.googleapis.com/auth/bigquery":
	default:
		return "", adapter.ErrUnsupported
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.MaxConnsPerHost = 1
	transport.MaxIdleConnsPerHost = 1
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	s := &Session{token: credentials, oauthEndpoint: "https://oauth2.googleapis.com/token", http: client}
	return s.googleToken(ctx, scope)
}

func parseGoogleSecret(raw string) (googleSecret, error) {
	if !strings.HasPrefix(strings.TrimSpace(raw), "{") {
		if validBearer(raw) {
			return googleSecret{AccessToken: raw}, nil
		}
		return googleSecret{}, adapter.ErrInvalid
	}
	var input map[string]json.RawMessage
	if operations.DecodeStrict([]byte(raw), &input, 32<<10) != nil {
		return googleSecret{}, adapter.ErrInvalid
	}
	get := func(key string) string { var s string; _ = json.Unmarshal(input[key], &s); return s }
	result := googleSecret{Type: get("type"), Email: get("client_email"), KeyID: get("private_key_id"), PrivateKey: get("private_key"), TokenURI: get("token_uri"), AccessToken: get("access_token")}
	if result.AccessToken != "" {
		if len(input) != 1 || !validBearer(result.AccessToken) {
			return result, adapter.ErrInvalid
		}
		return result, nil
	}
	if result.Type != "service_account" || !strings.HasSuffix(result.Email, ".gserviceaccount.com") || strings.ContainsAny(result.Email, "\x00\r\n ") || result.TokenURI != "https://oauth2.googleapis.com/token" {
		return result, adapter.ErrInvalid
	}
	if _, err := googlePrivateKey(result.PrivateKey); err != nil {
		return result, err
	}
	return result, nil
}
func googlePrivateKey(raw string) (*rsa.PrivateKey, error) {
	block, rest := pem.Decode([]byte(raw))
	if block == nil || len(strings.TrimSpace(string(rest))) != 0 {
		return nil, adapter.ErrInvalid
	}
	value, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, adapter.ErrInvalid
	}
	key, ok := value.(*rsa.PrivateKey)
	if !ok || key.N.BitLen() < 2048 || key.Validate() != nil {
		return nil, adapter.ErrInvalid
	}
	return key, nil
}
func validBearer(raw string) bool {
	return raw != "" && len(raw) <= 32<<10 && !strings.ContainsAny(raw, "\x00\r\n\t ")
}
func (s *Session) validateCredentials() error {
	switch s.engine {
	case "ga4", "google_ads":
		_, err := parseGoogleSecret(s.token)
		return err
	case "salesforce":
		if s.username != "" {
			_, _, err := salesforcePassword(s.token)
			return err
		}
	}
	if !validBearer(s.token) {
		return adapter.ErrInvalid
	}
	return nil
}
func (s *Session) googleToken(ctx context.Context, scope string) (string, error) {
	secret, err := parseGoogleSecret(s.token)
	if err != nil {
		return "", err
	}
	if secret.AccessToken != "" {
		return secret.AccessToken, nil
	}
	key, err := googlePrivateKey(secret.PrivateKey)
	if err != nil {
		return "", err
	}
	now := time.Now().Unix()
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT", "kid": secret.KeyID})
	claims, _ := json.Marshal(map[string]any{"iss": secret.Email, "scope": scope, "aud": "https://oauth2.googleapis.com/token", "iat": now, "exp": now + 3600})
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		return "", adapter.ErrInvalid
	}
	form := url.Values{"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"}, "assertion": {unsigned + "." + base64.RawURLEncoding.EncodeToString(signature)}}
	raw, err := s.request(ctx, "POST", s.oauthEndpoint, []byte(form.Encode()), http.Header{"Content-Type": {"application/x-www-form-urlencoded"}}, &budget{limits: adapter.Limits{MaxBytes: 1 << 20}})
	if err != nil {
		return "", err
	}
	response, err := envelope(raw)
	if err != nil {
		return "", err
	}
	var token, kind string
	if json.Unmarshal(response["access_token"], &token) != nil || !validBearer(token) || json.Unmarshal(response["token_type"], &kind) != nil || !strings.EqualFold(kind, "Bearer") {
		return "", errors.New("provider token response invalid")
	}
	return token, nil
}
func salesforcePassword(raw string) (string, string, error) {
	var credentials struct {
		Password      string `json:"password"`
		SecurityToken string `json:"security_token"`
	}
	if operations.DecodeStrict([]byte(raw), &credentials, 32<<10) != nil || credentials.Password == "" || credentials.SecurityToken == "" || strings.ContainsAny(credentials.Password+credentials.SecurityToken, "\x00\r\n") {
		return "", "", adapter.ErrInvalid
	}
	return credentials.Password, credentials.SecurityToken, nil
}
