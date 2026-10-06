package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

type fakeDriver struct{ opened int }

func (d *fakeDriver) Capabilities() operations.Capabilities {
	return operations.Capabilities{Version: operations.Version, Engine: "test", Operations: []operations.Capability{{Kind: operations.QueryRead, Idempotency: "none", Cancellation: "best_effort"}}}
}
func (d *fakeDriver) Open(context.Context, adapter.Connection) (adapter.Session, error) {
	d.opened++
	return fakeSession{}, nil
}

type fakeSession struct{}

func (fakeSession) Close() error { return nil }

func TestRegistryValidatesScopeAndCapabilitiesBeforeOpen(t *testing.T) {
	driver := &fakeDriver{}
	registry, err := New(driver)
	if err != nil {
		t.Fatal(err)
	}
	request := operations.Request{Version: operations.Version, Kind: operations.QueryRead, Connection: operations.ConnectionRef{ID: "saved-1"}, Spec: operations.Spec{Query: &operations.QuerySpec{SQL: "SELECT 1"}}}
	connection := adapter.Connection{TenantID: "tenant-1", ConnectionID: "saved-1", Revision: "revision-1", Engine: "test", Namespace: "saved_default"}
	if _, err := registry.Open(context.Background(), connection, request); err != nil {
		t.Fatal("saved default rejected:", err)
	}
	request.Connection.Database = "other"
	if _, err := registry.Open(context.Background(), connection, request); !errors.Is(err, adapter.ErrInvalid) {
		t.Fatal("namespace mismatch accepted")
	}
	request.Connection.Database = ""
	connection.ConnectionID = "saved-2"
	if _, err := registry.Open(context.Background(), connection, request); !errors.Is(err, adapter.ErrInvalid) {
		t.Fatal("connection mismatch accepted")
	}
	if driver.opened != 1 {
		t.Fatalf("unexpected opens: %d", driver.opened)
	}
	if _, err := New(driver, driver); err == nil {
		t.Fatal("duplicate driver accepted")
	}
}
