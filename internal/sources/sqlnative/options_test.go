// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package sqlnative

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestSourceOptionsRequireExplicitDialectHooks(t *testing.T) {
	source := catalog.Source{ID: "source", Type: "fake", DSNEnv: "KELVO_NATIVE_OPTION_DSN", Options: map[string]string{"trusted": "original"}}
	config := catalog.Config{Sources: []catalog.Source{source}}
	dialect := Dialect{SourceType: "fake", DriverName: fakeDriverName}
	if _, err := New(config, query.DefaultLimits(), dialect); err == nil {
		t.Fatal("default dialect accepted options")
	}
	var opened bool
	dialect.ValidateSource = func(s catalog.Source) error {
		if len(s.Options) != 1 || s.Options["trusted"] != "original" {
			return errors.New("invalid")
		}
		s.Options["trusted"] = "validator mutation"
		return nil
	}
	if _, err := New(config, query.DefaultLimits(), dialect); err == nil {
		t.Fatal("validator without option-aware opener accepted")
	}
	dialect.OpenSourceDB = func(s catalog.Source, dsn string) (*sql.DB, error) {
		opened = true
		if s.Options["trusted"] != "original" || dsn != "fixture" {
			t.Error("validated source options or DSN changed")
		}
		s.Options["trusted"] = "opener mutation"
		return nil, errors.New("private fixture diagnostic")
	}
	e, err := New(config, query.DefaultLimits(), dialect)
	if err != nil {
		t.Fatal(err)
	}
	source.Options["trusted"] = "external mutation"
	t.Setenv("KELVO_NATIVE_OPTION_DSN", "fixture")
	for range 2 {
		_, err = e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "source", SQL: "SELECT 1"}, &execSink{})
		if err == nil || strings.Contains(err.Error(), "private fixture") {
			t.Fatal("source opener failure not sanitized")
		}
	}
	if !opened {
		t.Fatal("source-aware opener was not used")
	}
}
