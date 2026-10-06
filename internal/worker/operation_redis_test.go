package worker

import (
	"maps"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestRedisResolvedSourceRejectsAmbientCredentialsAndSQL(t *testing.T) {
	response := operationResolution{Source: catalog.Source{ID: "source_1", Type: "redis", URLEnv: "KELVO_SOURCE_REQUEST_0_URL", PasswordEnv: "KELVO_SOURCE_REQUEST_0_PASSWORD", UsernameEnv: "KELVO_SOURCE_REQUEST_0_USERNAME"}, Secrets: map[string]string{"KELVO_SOURCE_REQUEST_0_URL": "rediss://localhost:6380", "KELVO_SOURCE_REQUEST_0_PASSWORD": "fixture-secret", "KELVO_SOURCE_REQUEST_0_USERNAME": "tenant-user"}}
	request := operations.Request{Kind: operations.ConnectionTest, Connection: operations.ConnectionRef{Database: "7"}}
	if err := validateOperationRedisCatalog(response, request); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*operationResolution){"unknown secret": func(r *operationResolution) { r.Secrets["UNBOUND"] = "secret" }, "missing password": func(r *operationResolution) { delete(r.Secrets, r.Source.PasswordEnv) }, "ambient password": func(r *operationResolution) { r.Source.PasswordEnv = "REDIS_PASSWORD" }, "URL database": func(r *operationResolution) { r.Secrets[r.Source.URLEnv] += "/8" }, "plain socket": func(r *operationResolution) { r.Secrets[r.Source.URLEnv] = "redis://localhost:6380" }, "TLS bypass": func(r *operationResolution) { r.Source.Options = map[string]string{"tls_skip_verify": "true"} }} {
		t.Run(name, func(t *testing.T) {
			copy := response
			copy.Secrets = maps.Clone(response.Secrets)
			change(&copy)
			if validateOperationRedisCatalog(copy, request) == nil {
				t.Fatal("invalid resolved source accepted")
			}
		})
	}
	request.Kind = operations.QueryRead
	if validateOperationRedisCatalog(response, request) == nil {
		t.Fatal("generic SQL accepted for Redis")
	}
	request.Kind = operations.NativeRead
	request.Connection.Schema = "other"
	if validateOperationRedisCatalog(response, request) == nil {
		t.Fatal("foreign namespace accepted")
	}
}
