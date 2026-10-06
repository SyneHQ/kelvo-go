package cloudapi

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestResolvedCredentialsAreIsolatedAcrossConcurrentSources(t *testing.T) {
	t.Setenv("CLOUD_REQUEST_URL", "https://ambient.invalid")
	t.Setenv("CLOUD_REQUEST_TOKEN", "ambient-secret")
	var calls atomic.Int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer token"+r.URL.Path {
			t.Error("cross-source credentials")
		}
		fmt.Fprint(w, `{"ok":true}`)
	}))
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	trust := &tls.Config{MinVersion: tls.VersionTLS10, RootCAs: roots}
	var clients []*Client
	for _, id := range []string{"/one", "/two"} {
		client, err := NewResolved(catalog.Source{URLEnv: "CLOUD_REQUEST_URL", TokenEnv: "CLOUD_REQUEST_TOKEN"}, query.DefaultLimits(), Credentials{URL: server.URL, Token: "token" + id, TLS: trust})
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		clients = append(clients, client)
		transport := client.HTTP.Transport.(*http.Transport)
		if transport.Proxy != nil || transport.TLSClientConfig == trust || transport.TLSClientConfig.RootCAs == roots || transport.TLSClientConfig.MinVersion < tls.VersionTLS12 {
			t.Fatal("mutable or insecure trust shared")
		}
	}
	var group sync.WaitGroup
	for index, client := range clients {
		group.Add(1)
		go func(index int, client *Client) {
			defer group.Done()
			path := []string{"/one", "/two"}[index]
			for i := 0; i < 20; i++ {
				var response map[string]any
				if _, _, err := client.Do(context.Background(), http.MethodGet, path, nil, nil, &response); err != nil {
					t.Error(err)
				}
			}
		}(index, client)
	}
	group.Wait()
	if calls.Load() != 40 {
		t.Fatal(calls.Load())
	}
	if trust.MinVersion != tls.VersionTLS10 {
		t.Fatal("caller TLS configuration changed")
	}
}

func TestResolvedCredentialsNeverFallBackOrWeakenTLS(t *testing.T) {
	t.Setenv("CLOUD_REQUEST_URL", "https://valid.example")
	t.Setenv("CLOUD_REQUEST_TOKEN", "valid-token")
	source := catalog.Source{URLEnv: "CLOUD_REQUEST_URL", TokenEnv: "CLOUD_REQUEST_TOKEN"}
	for _, credentials := range []Credentials{
		{}, {URL: "https://valid.example"}, {Token: "token"}, {URL: "http://valid.example", Token: "token"},
		{URL: "https://user:password@valid.example", Token: "token"},
		{URL: "https://valid.example?", Token: "token"},
		{URL: "https://valid.example", Token: "token\r\nInjected: yes"},
		{URL: "https://valid.example", Token: "token", TLS: &tls.Config{InsecureSkipVerify: true}},
		{URL: "https://valid.example", Token: "token", TLS: &tls.Config{MaxVersion: tls.VersionTLS11}},
	} {
		if client, err := NewResolved(source, query.DefaultLimits(), credentials); err == nil {
			client.Close()
			t.Fatal("unsafe resolved credentials accepted")
		}
	}
}
