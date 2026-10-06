package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestNativeReaderOnDemandCredentialsAndDenials(t *testing.T) {
	record, request, response := operationResolverFixture(t)
	request.Kind = operations.QueryRead
	record.Kind = request.Kind
	request.Spec = operations.Spec{Query: &operations.QuerySpec{SQL: "SELECT 1"}}
	record.RequestSHA256, _ = operations.Digest(request)
	response.RequestSHA256 = record.RequestSHA256
	response.Source = catalog.Source{ID: "source_1", Type: "trino", URLEnv: "KELVO_SOURCE_REQUEST_0_URL", UsernameEnv: "KELVO_SOURCE_REQUEST_0_USERNAME", PasswordEnv: "KELVO_SOURCE_REQUEST_0_PASSWORD", Options: map[string]string{"catalog": request.Connection.Database, "schema": request.Connection.Schema}}
	response.Secrets = map[string]string{response.Source.URLEnv: "https://trino.example", response.Source.UsernameEnv: "current-user", response.Source.PasswordEnv: "current-secret"}
	resolver := fixtureConnectionResolver(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	})
	resolver.url += "/internal/kelvo/resolve"
	e := &Executor{Limits: query.DefaultLimits(), connectionResolvers: map[string]*ConnectionResolver{"gateway": resolver}}
	input, err := e.ResolveOperationSource(context.Background(), record, request)
	if err != nil || input.Source.Password != "current-secret" || input.Source.Schema != request.Connection.Schema {
		t.Fatal("private read source lost binding", err)
	}
	response.Secrets[response.Source.PasswordEnv] = "rotated-secret"
	input, err = e.ResolveOperationSource(context.Background(), record, request)
	if err != nil || input.Source.Password != "rotated-secret" {
		t.Fatal("new operation reused old credentials", err)
	}
	response.Source.Options["catalog"] = "another"
	if _, err = e.ResolveOperationSource(context.Background(), record, request); err == nil {
		t.Fatal("foreign catalog accepted")
	}
	response.Source.Options["catalog"] = request.Connection.Database
	response.Secrets["UNRELATED"] = "secret"
	if _, err = e.ResolveOperationSource(context.Background(), record, request); err == nil {
		t.Fatal("unbound credential accepted")
	}
}
func TestMigrationServerUUIDMustBeCanonicalAndPrivate(t *testing.T) {
	for _, value := range []string{"00000000-0000-0000-0000-000000000000", "123E4567-e89b-12d3-a456-426614174000", "123e4567e89b12d3a456426614174000", "123e4567-e89b-12d3-a456-42661417400g"} {
		if validOperationServerUUID(value) {
			t.Fatal("invalid server pin accepted")
		}
	}
	if !validOperationServerUUID("123e4567-e89b-12d3-a456-426614174000") {
		t.Fatal("canonical server pin rejected")
	}
}
