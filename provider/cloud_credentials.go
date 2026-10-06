package provider

import (
	"bytes"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"strings"

	"github.com/SYNEHQ/kelvo-go/operations"
)

// CanonicalGoogleCredential accepts an explicit access token or service-account
// document. It cannot select a token endpoint, local file, or ambient identity.
func CanonicalGoogleCredential(raw string) (string, error) {
	var input map[string]json.RawMessage
	if operations.DecodeStrict([]byte(raw), &input, 32<<10) != nil || len(input) == 0 {
		return "", operations.ErrInvalid
	}
	text := func(key string) string { var value string; _ = json.Unmarshal(input[key], &value); return value }
	if token := text("access_token"); token != "" {
		if len(input) != 1 || !CloudBearer(token) {
			return "", operations.ErrInvalid
		}
	} else {
		for key := range input {
			switch key {
			case "type", "project_id", "private_key_id", "private_key", "client_email", "client_id", "auth_uri", "token_uri", "auth_provider_x509_cert_url", "client_x509_cert_url", "universe_domain":
			default:
				return "", operations.ErrInvalid
			}
		}
		if text("type") != "service_account" || !strings.HasSuffix(text("client_email"), ".gserviceaccount.com") || strings.ContainsAny(text("client_email"), "\x00\r\n\t ") || text("token_uri") != "https://oauth2.googleapis.com/token" || text("universe_domain") != "" && text("universe_domain") != "googleapis.com" {
			return "", operations.ErrInvalid
		}
		block, rest := pem.Decode([]byte(text("private_key")))
		if block == nil || block.Type != "PRIVATE KEY" || len(bytes.TrimSpace(rest)) != 0 {
			return "", operations.ErrInvalid
		}
		key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return "", operations.ErrInvalid
		}
		rsaKey, ok := key.(*rsa.PrivateKey)
		if !ok || rsaKey.N.BitLen() < 2048 || rsaKey.Validate() != nil {
			return "", operations.ErrInvalid
		}
	}
	canonical, err := json.Marshal(input)
	if err != nil || len(canonical) > 32<<10 {
		return "", operations.ErrInvalid
	}
	return string(canonical), nil
}

func CloudBearer(value string) bool {
	return value != "" && len(value) <= 16<<10 && !strings.ContainsAny(value, "\x00\r\n\t ")
}

// SnowflakeCredential names the credential type explicitly. A saved password
// cannot silently acquire SQL API bearer-token meaning.
type SnowflakeCredential struct {
	Type      string `json:"type"`
	Token     string `json:"token"`
	Warehouse string `json:"warehouse,omitempty"`
	Role      string `json:"role,omitempty"`
}

func ParseSnowflakeCredential(raw string) (SnowflakeCredential, error) {
	var c SnowflakeCredential
	if operations.DecodeStrict([]byte(raw), &c, 32<<10) != nil || !CloudBearer(c.Token) {
		return c, operations.ErrInvalid
	}
	switch c.Type {
	case "oauth", "keypair_jwt", "programmatic_access_token":
	default:
		return c, operations.ErrInvalid
	}
	return c, nil
}
