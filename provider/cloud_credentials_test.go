package provider

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"strings"
	"testing"
)

func TestGoogleSavedCredentialIsExplicitAndFixedOrigin(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	valid := map[string]string{"type": "service_account", "client_email": "fixture@project.iam.gserviceaccount.com", "private_key": string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})), "token_uri": "https://oauth2.googleapis.com/token"}
	raw, _ := json.Marshal(valid)
	if _, err := CanonicalGoogleCredential(string(raw)); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"ambient-default", `{"access_token":"explicit-token","access_token":"other"}`, `{"access_token":"a b"}`, `{"type":"external_account","credential_source":{"file":"/private/key"}}`, strings.Replace(string(raw), "https://oauth2.googleapis.com/token", "https://attacker.example/token", 1), strings.Replace(string(raw), `"service_account"`, `"authorized_user"`, 1)} {
		if _, err := CanonicalGoogleCredential(raw); err == nil {
			t.Fatal("unsafe Google identity accepted")
		}
	}
	canonical, err := CanonicalGoogleCredential("{\n \"access_token\": \"explicit-token\"\n}")
	if err != nil || canonical != `{"access_token":"explicit-token"}` {
		t.Fatal(canonical, err)
	}
}
func TestSnowflakePasswordNeverBecomesBearerToken(t *testing.T) {
	for _, raw := range []string{"saved-password", `{"type":"password","token":"secret"}`, `{"type":"oauth","token":"secret","password":"other"}`} {
		if _, err := ParseSnowflakeCredential(raw); err == nil {
			t.Fatal("ambiguous Snowflake auth accepted")
		}
	}
	for _, kind := range []string{"oauth", "keypair_jwt", "programmatic_access_token"} {
		raw, _ := json.Marshal(SnowflakeCredential{Type: kind, Token: "explicit-token"})
		if _, err := ParseSnowflakeCredential(string(raw)); err != nil {
			t.Fatal(err)
		}
	}
}
