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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestGoogleServiceAccountSignsExactBigQueryScope(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	secret, _ := json.Marshal(map[string]string{"type": "service_account", "client_email": "fixture@project.iam.gserviceaccount.com", "private_key_id": "fixture-key", "private_key": string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})), "token_uri": "https://oauth2.googleapis.com/token"})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.ParseForm() != nil || r.Form.Get("grant_type") != "urn:ietf:params:oauth:grant-type:jwt-bearer" {
			t.Error("invalid grant exchange")
			w.WriteHeader(400)
			return
		}
		pieces := strings.Split(r.Form.Get("assertion"), ".")
		if len(pieces) != 3 {
			t.Error("invalid JWT")
			w.WriteHeader(400)
			return
		}
		digest := sha256.Sum256([]byte(pieces[0] + "." + pieces[1]))
		signature, err := base64.RawURLEncoding.DecodeString(pieces[2])
		if err != nil || rsa.VerifyPKCS1v15(&key.PublicKey, crypto.SHA256, digest[:], signature) != nil {
			t.Error("JWT signature invalid")
			w.WriteHeader(400)
			return
		}
		payload, err := base64.RawURLEncoding.DecodeString(pieces[1])
		var claims struct {
			Issuer   string `json:"iss"`
			Scope    string `json:"scope"`
			Audience string `json:"aud"`
			Issued   int64  `json:"iat"`
			Expires  int64  `json:"exp"`
		}
		if err != nil || json.Unmarshal(payload, &claims) != nil || claims.Issuer != "fixture@project.iam.gserviceaccount.com" || claims.Scope != "https://www.googleapis.com/auth/bigquery" || claims.Audience != "https://oauth2.googleapis.com/token" || claims.Expires-claims.Issued != 3600 || claims.Issued < time.Now().Add(-time.Minute).Unix() {
			t.Error("JWT identity/scope/audience changed")
			w.WriteHeader(400)
			return
		}
		fmt.Fprint(w, `{"access_token":"fixture-issued-token","token_type":"Bearer","expires_in":3600}`)
	}))
	defer server.Close()
	session := &Session{token: string(secret), oauthEndpoint: server.URL, http: server.Client()}
	token, err := session.googleToken(context.Background(), "https://www.googleapis.com/auth/bigquery")
	if err != nil || token != "fixture-issued-token" {
		t.Fatal("explicit key exchange failed", err)
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/no/ambient/credentials")
	token, err = GoogleAccessToken(context.Background(), `{"access_token":"saved-token"}`, "https://www.googleapis.com/auth/bigquery")
	if err != nil || token != "saved-token" {
		t.Fatal("explicit saved token lost", err)
	}
}
