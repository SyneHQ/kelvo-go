// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package resolver

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/delegation"
	"github.com/SYNEHQ/kelvo-go/query"
)

// federatedFileFixture signs a federated query joining a PostgreSQL table
// with the uploaded file peer "files" (table "data").
func federatedFileFixture(t *testing.T, payload []byte) (*handlerFixture, QueryFileReadRequest) {
	t.Helper()
	f := newFixture(t)
	f.query.Query = query.Request{Mode: "federated", SQL: "SELECT * FROM pg.t JOIN files.data USING (id)", Sources: []string{"pg", "files"}}
	digest, err := delegation.QueryDigest(f.query.Query)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	claims := delegation.Claims{Version: 1, Issuer: "application", Audience: "kelvo", ClusterTenant: "cluster", ServicePrincipal: "service", AppTeam: "team-a", Subject: delegation.Subject{Kind: "user", ID: "user-a"}, ID: strings.Repeat("c", 48), IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Minute).Unix(), QuerySHA256: digest,
		Sources: []delegation.Source{{Alias: "pg", ConnectionID: "saved-a", Database: "app", Tables: []delegation.Table{{Name: "t", Table: "t", Schema: "public"}}}, {Alias: "files", ConnectionID: "saved-file", Tables: []delegation.Table{QueryFileTable}}}}
	if f.query.Delegation, err = delegation.Sign(claims, f.key); err != nil {
		t.Fatal(err)
	}
	snapshot := snapshotFor(payload)
	snapshot.Format = "csv"
	return f, QueryFileReadRequest{QueryRequest: f.query, Alias: "files", Snapshot: snapshot}
}

func TestQueryFileReadServesOnlySignedVerifiedFiles(t *testing.T) {
	for _, scenario := range []string{"valid", "unknown alias", "non-file alias", "digest mismatch", "wrong size", "bad binding", "unauthorized", "missing callback", "native query", "revoked after delivery"} {
		t.Run(scenario, func(t *testing.T) {
			payload := []byte("id,amount\n1,10\n")
			f, input := federatedFileFixture(t, payload)
			f.config.TempDir = t.TempDir()
			opened, auths := 0, 0
			f.config.AuthorizeQuery = func(context.Context, QueryRequest, delegation.Claims) (Authorization, error) {
				auths++
				if scenario == "unauthorized" || (scenario == "revoked after delivery" && auths >= 3) {
					return Authorization{}, ErrInvalid
				}
				return fixtureAuthorization(), nil
			}
			f.config.OpenQueryFile = func(_ context.Context, r QueryFileReadRequest, _ delegation.Claims, _ Authorization) (io.ReadCloser, error) {
				opened++
				data := bytes.Clone(payload)
				if scenario == "digest mismatch" {
					data[0] = 'X'
				}
				return io.NopCloser(bytes.NewReader(data)), nil
			}
			switch scenario {
			case "unknown alias":
				input.Alias = "other"
			case "non-file alias":
				input.Alias = "pg"
			case "wrong size":
				input.Snapshot.Bytes++
			case "bad binding":
				input.Owner = strings.Repeat("e", 32)
			case "missing callback":
				f.config.OpenQueryFile = nil
			case "native query":
				input.Query.Mode = "native"
			}
			w := serveFixture(t, f.config, f.request(t, QueryFileReadPath, input))
			response := w.Result()
			defer response.Body.Close()
			body, _ := io.ReadAll(response.Body)
			switch scenario {
			case "valid":
				if w.Code != http.StatusOK || !bytes.Equal(body, payload) || response.Header.Get(FileSHA256Header) != input.Snapshot.SHA256 || response.Trailer.Get(FileVerifiedTrailer) != input.Snapshot.SHA256 || response.Trailer.Get(SourceValidUntilTrailer) == "" {
					t.Fatalf("valid query file lost proof: status %d", w.Code)
				}
			case "revoked after delivery":
				if response.Trailer.Get(FileVerifiedTrailer) != "" {
					t.Fatal("revoked query file acquired a receipt")
				}
			default:
				if w.Code < 400 || bytes.Contains(body, []byte("amount")) {
					t.Fatalf("unverified query file escaped: status %d", w.Code)
				}
				if scenario != "digest mismatch" && scenario != "wrong size" && opened != 0 {
					t.Fatal("refused request reached storage")
				}
			}
			if entries, err := os.ReadDir(f.config.TempDir); err != nil || len(entries) != 0 {
				t.Fatal("query file staging was retained")
			}
		})
	}
}

func TestQueryMaterialAcceptsOnlyTheFileSourceShape(t *testing.T) {
	f, _ := federatedFileFixture(t, []byte("x"))
	claims, err := delegation.Verify(f.query.Delegation, f.config.QueryTrust[0], f.query.Query, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	snapshot := snapshotFor([]byte("x"))
	snapshot.Format = "csv"
	pg := Source{ID: "pg", Type: "postgres", DSNEnv: "KELVO_SOURCE_REQUEST_0_DSN", Federation: &Federation{Tables: claims.Sources[0].Tables}}
	good := func() Source {
		file := snapshot
		return Source{ID: "files", Type: "csv", File: &file, Federation: &Federation{Tables: []delegation.Table{QueryFileTable}}}
	}
	material := func(s Source) QueryMaterial {
		return QueryMaterial{Sources: []Source{pg, s}, Secrets: fixtureSecrets(), ValidUntil: time.Now().Add(time.Minute)}
	}
	if err := validateQueryMaterial(material(good()), claims, f.query); err != nil {
		t.Fatal("valid file source rejected")
	}
	for name, edit := range map[string]func(*Source){
		"format mismatch": func(s *Source) { s.Type = "parquet" },
		"sqlite":          func(s *Source) { s.Type, s.File.Format = "sqlite", "sqlite" },
		"no federation":   func(s *Source) { s.Federation = nil },
		"other table":     func(s *Source) { s.Federation.Tables = []delegation.Table{{Name: "x", Table: "x"}} },
		"schema": func(s *Source) {
			s.Federation.Tables = []delegation.Table{{Name: "data", Table: "data", Schema: "public"}}
		},
		"options":        func(s *Source) { s.Options = map[string]string{"path": "/etc/passwd"} },
		"credential":     func(s *Source) { s.URLEnv = "KELVO_SOURCE_REQUEST_1_URL" },
		"bad descriptor": func(s *Source) { s.File.SHA256 = "bad" },
	} {
		s := good()
		edit(&s)
		if validateQueryMaterial(material(s), claims, f.query) == nil {
			t.Fatal("invalid file source accepted:", name)
		}
	}
}
