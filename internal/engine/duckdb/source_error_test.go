//go:build duckdb_arrow

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	duck "github.com/duckdb/duckdb-go/v2"
)

func TestWithSourceErrorKeepsOnlyStatementClassText(t *testing.T) {
	const workspace = "/var/folders/x/kelvo-duckdb-123"
	generic := publicError(errors.New("x"))
	check := func(typ duck.ErrorType, msg, want string) {
		t.Helper()
		err := withSourceError(generic, statementError{&duck.Error{Type: typ, Msg: msg}}, workspace)
		if query.PublicError(err).Code != "QUERY_FAILED" {
			t.Fatalf("public code changed: %v", err)
		}
		var sf *SourceFailure
		got := errors.As(err, &sf)
		if want == "" {
			if got {
				t.Fatalf("%q crossed as %+v", msg, sf.SourceError())
			}
			return
		}
		if !got || !strings.Contains(sf.SourceError().Message, want) || sf.SourceError().SQLState != "" {
			t.Fatalf("%q: source error = %v", msg, err)
		}
	}
	check(duck.ErrorTypeBinder, `Binder Error: Referenced column "nope" not found in FROM clause!`, "nope")
	check(duck.ErrorTypeParser, `Parser Error: syntax error at or near "FORM"`, "FORM")
	check(duck.ErrorTypeConversion, `Conversion Error: Could not convert string 'abc' to INT32`, "abc")
	check(duck.ErrorTypeIO, `IO Error: No files found that match the pattern "x"`, "")
	check(duck.ErrorTypeHTTP, `HTTP Error: HTTP GET error on 'host'`, "")
	check(duck.ErrorTypeBinder, `Binder Error: column in `+workspace+`/data.csv`, "")
	check(duck.ErrorTypeCatalog, `Catalog Error: Table with name '/etc/passwd' does not exist`, "")
	check(duck.ErrorTypeCatalog, `Catalog Error: Table https://h/x does not exist`, "")
	// Errors outside the caller's statement (setup, attach) never cross.
	if err := withSourceError(generic, &duck.Error{Type: duck.ErrorTypeBinder, Msg: "Binder Error: x"}, workspace); err != generic {
		t.Fatal("setup error crossed")
	}
}

func TestExecuteMissingColumnCarriesDuckDBMessage(t *testing.T) {
	csv := filepath.Join(t.TempDir(), "t.csv")
	if err := os.WriteFile(csv, []byte("id\n1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := makeEngine(t, csv).Execute(context.Background(), query.Request{Sources: []string{"events"}, SQL: "SELECT nope FROM events"}, new(captureSink))
	var sf *SourceFailure
	// Prepare failures keep their existing public code.
	if query.PublicError(err).Code != "PERMISSION_DENIED" || !errors.As(err, &sf) || !strings.Contains(sf.SourceError().Message, "nope") {
		t.Fatalf("err = %v", err)
	}
	t.Logf("source error: %+v", *sf.SourceError())
}
