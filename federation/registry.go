// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"errors"
	"reflect"
	"regexp"
	"strings"
	"sync"
)

var sourceType = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

// Reserve built-in names and spelling aliases; keep aligned with the catalog.
var reserved = strings.Fields(`postgres postgresql mysql mariadb sqlite duckdb cockroach cockroachdb
sqlserver mssql msql clickhouse cosmosdb oracle dynamodb trino clickhouse_lambda alloydb presto
athena hive h2 ignite spanner db2 exasol sap_hana sap_ase salesforce google_ads facebook_ads spark
d1 snowflake bigquery databricks redshift mongodb mongo cassandra scylla elasticsearch csv parquet
posthog ga4 stripe redis arrow_flight google_sheets accelerated`)

type registry struct {
	mu      sync.RWMutex
	drivers map[string]Driver
	frozen  bool
}

var defaults = registry{drivers: make(map[string]Driver)}

// Register adds an exact custom type during initialization, before any Lookup.
// Built-ins, duplicates, nil drivers and later mutations are rejected. Link
// adapter packages into both the gateway and its re-executed worker binary.
func Register(kind string, driver Driver) error { return defaults.register(kind, driver) }

// MustRegister is Register for an adapter package's init function.
func MustRegister(kind string, driver Driver) {
	if err := Register(kind, driver); err != nil {
		panic(err)
	}
}

// Lookup returns an explicitly registered custom driver and permanently freezes
// registration. Access is synchronized; no fallback driver exists.
func Lookup(kind string) (Driver, bool) { return defaults.lookup(kind) }

func (r *registry) register(kind string, driver Driver) error {
	if !sourceType.MatchString(kind) || driver == nil {
		return errors.New("federation registration requires an exact custom type and driver")
	}
	v := reflect.ValueOf(driver)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if v.IsNil() {
			return errors.New("federation registration requires a non-nil driver")
		}
	}
	for _, builtin := range reserved {
		if kind == builtin {
			return errors.New("federation registration cannot replace a reserved source type")
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return errors.New("federation registration is frozen after first use")
	}
	if _, exists := r.drivers[kind]; exists {
		return errors.New("federation source type is already registered")
	}
	if r.drivers == nil {
		r.drivers = make(map[string]Driver)
	}
	r.drivers[kind] = driver
	return nil
}

func (r *registry) lookup(kind string) (Driver, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.frozen = true
	driver, ok := r.drivers[kind]
	return driver, ok
}
