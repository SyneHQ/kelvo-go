// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package sqlnative

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestTransactionConfigurationPrecedesQueryAndAlwaysRollsBack(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "configuration failure"}[fail], func(t *testing.T) {
			s := &fakeScenario{row: []driver.Value{int64(7), float64(1.25), time.Now().UTC(), []byte("12.34")}}
			fakeState.Lock()
			fakeState.s = s
			fakeState.Unlock()
			t.Setenv("KELVO_SOURCE_FAKE_DSN", "fixture")
			called := 0
			dialect := Dialect{SourceType: "fake", DriverName: fakeDriverName, ReadOnlyOption: true,
				ConfigureTransaction: func(ctx context.Context, tx *sql.Tx) error {
					called++
					if _, ok := ctx.Deadline(); !ok || tx == nil || s.queries != 0 {
						t.Fatal("transaction configuration ran without its deadline or after the query")
					}
					if fail {
						return errors.New("private source detail")
					}
					return nil
				}}
			limits := query.Limits{MaxRows: 10, MaxBytes: 1 << 20, Timeout: time.Second, MemoryMB: 16, Threads: 1, MaxTempMB: 1}
			engine, err := New(catalog.Config{Sources: []catalog.Source{{ID: "fake", Type: "fake", DSNEnv: "KELVO_SOURCE_FAKE_DSN"}}}, limits, dialect)
			if err != nil {
				t.Fatal(err)
			}
			sink := &execSink{}
			defer sink.close()
			_, err = engine.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "fake", SQL: "select 1"}, sink)
			if (err != nil) != fail || called != 1 || s.commits != 0 || s.rollbacks != 1 {
				t.Fatalf("error=%v configured=%d commits=%d rollbacks=%d", err, called, s.commits, s.rollbacks)
			}
			if fail && (s.queries != 0 || strings.Contains(err.Error(), "private source detail")) {
				t.Fatal("failed configuration executed SQL or exposed private source text")
			}
			if !fail && s.queries != 1 {
				t.Fatal("successful configuration did not execute exactly one source query")
			}
		})
	}
}
