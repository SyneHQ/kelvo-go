// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"context"
	"sync"
	"testing"
)

type registryDriver struct{}

func (*registryDriver) Validate(Source, Table) error { return nil }
func (*registryDriver) Open(context.Context, Source, Table, Limits) (Relation, error) {
	return nil, ErrUnsupported
}

func TestRegistryRejectsReservedInvalidAndDuplicateTypes(t *testing.T) {
	var r registry
	for _, name := range append(append([]string(nil), reserved...), "", "Postgres", "custom-db", "a.b", "_custom") {
		if err := r.register(name, &registryDriver{}); err == nil {
			t.Fatalf("registered reserved or invalid type %q", name)
		}
	}
	var typedNil *registryDriver
	if r.register("custom_nil", typedNil) == nil || r.register("custom_nil", nil) == nil {
		t.Fatal("nil driver accepted")
	}
	driver := &registryDriver{}
	if err := r.register("custom_one", driver); err != nil {
		t.Fatal(err)
	}
	if r.register("custom_one", &registryDriver{}) == nil {
		t.Fatal("duplicate registration replaced the driver")
	}
	if got, ok := r.lookup("custom_one"); !ok || got != driver {
		t.Fatal("exact registration was not preserved")
	}
	if _, ok := r.lookup("CUSTOM_ONE"); ok {
		t.Fatal("registry silently normalized a type")
	}
	if r.register("custom_late", driver) == nil {
		t.Fatal("registration changed after first lookup")
	}
}

func TestUnknownLookupAlsoFreezesRegistry(t *testing.T) {
	var r registry
	if _, ok := r.lookup("unknown"); ok {
		t.Fatal("unknown source had a fallback driver")
	}
	if r.register("custom_late", &registryDriver{}) == nil {
		t.Fatal("unknown lookup did not freeze registry")
	}
}

func TestRegistryConcurrentLookups(t *testing.T) {
	var r registry
	driver := &registryDriver{}
	if err := r.register("custom_one", driver); err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for i := 0; i < 32; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for j := 0; j < 100; j++ {
				if got, ok := r.lookup("custom_one"); !ok || got != driver {
					t.Error("concurrent lookup changed registration")
				}
			}
		}()
	}
	group.Wait()
}
