// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/mongodb"
)

// The real connector must reject write stages before loading a connection URI.
// This covers the acceleration execution path without requiring a live source.
func TestMongoRefreshRetainsReadOnlyPipelineValidation(t *testing.T) {
	for _, stage := range []string{`{"$out":"copy"}`, `{"$merge":{"into":"copy"}}`, `{"$lookup":{"from":"x","pipeline":[{"$merge":"copy"}],"as":"joined"}}`} {
		c := catalog.Config{Sources: []catalog.Source{{ID: "mongo", Type: "mongodb", DSNEnv: "KELVO_SOURCE_TEST_MONGO_DSN", Options: map[string]string{"database": "analytics"}}}, Acceleration: &catalog.AccelerationConfig{Directory: filepath.Join(t.TempDir(), "snapshots"), TenantID: "tenant-a", Datasets: []catalog.Dataset{{ID: "orders", Query: query.Request{Mode: "native", ConnectionID: "mongo", Mongo: &query.MongoRequest{Collection: "orders", Pipeline: []json.RawMessage{json.RawMessage(stage)}}}, AuthorizationVersion: "v1", MaxAge: time.Hour, Limits: query.DefaultLimits()}}}}
		m, err := NewManager(c, func(config catalog.Config, limits query.Limits) (query.Executor, error) {
			return mongodb.New(config, limits)
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = m.Refresh(context.Background(), "orders", false)
		m.Close()
		var qe *query.Error
		if !errors.As(err, &qe) || qe.Code != "PERMISSION_DENIED" {
			t.Fatalf("write pipeline did not fail through connector authorization: %v", err)
		}
	}
}
