package sheets

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func testLimits() query.Limits {
	return query.Limits{MaxRows: 100, MaxBytes: 1 << 20, Timeout: 5 * time.Second, MemoryMB: 64, Threads: 1, MaxTempMB: 32}
}
func testSession(t *testing.T, handler http.HandlerFunc) *Session {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	session, err := Open(context.Background(), "saved-sheet", "explicit-test-token", testLimits())
	if err != nil {
		t.Fatal(err)
	}
	session.origin = server.URL
	session.http = server.Client()
	session.http.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	t.Cleanup(func() { session.Close() })
	return session
}
func fixtureHandler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer explicit-test-token" {
			t.Error("missing explicit credential")
			w.WriteHeader(403)
			return
		}
		switch r.URL.Path {
		case "/v4/spreadsheets/saved-sheet":
			w.Write([]byte(`{"spreadsheetId":"saved-sheet","sheets":[{"properties":{"title":"Daily trips","gridProperties":{"columnCount":3}}}]}`))
		case "/v4/spreadsheets/saved-sheet/values/'Daily trips'!A1:GR10002":
			if r.URL.Query().Get("valueRenderOption") != "UNFORMATTED_VALUE" {
				t.Error("value rendering")
			}
			w.Write([]byte(`{"majorDimension":"ROWS","range":"'Daily trips'!A1:C3","values":[["id","fare","id"],[9007199254740993,10.000000000000000001,true],[2,"",false]]}`))
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}
}
func TestSnapshotUsesSavedSheetAndPreservesValues(t *testing.T) {
	s := testSession(t, fixtureHandler(t))
	tables, err := s.snapshot(context.Background())
	if err != nil || len(tables) != 1 {
		t.Fatal(tables, err)
	}
	if !reflect.DeepEqual(tables[0].Columns, []string{"id", "fare", "id_2"}) || !reflect.DeepEqual(tables[0].Rows[0], []any{"9007199254740993", "10.000000000000000001", "true"}) || tables[0].Rows[1][1] != nil {
		t.Fatal(tables)
	}
}
func TestSnapshotRejectsWrongSheetRedirectAndTruncation(t *testing.T) {
	for name, handler := range map[string]http.HandlerFunc{
		"wrong sheet": func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`{"spreadsheetId":"other-sheet","sheets":[]}`))
		},
		"redirect": func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "https://example.invalid/private", 302)
		},
		"truncated": func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"spreadsheetId":`)) },
		"too many tabs": func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`{"spreadsheetId":"saved-sheet","sheets":[` + strings.TrimSuffix(strings.Repeat(`{"properties":{"title":"x"}},`, 31), ",") + `]}`))
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := testSession(t, handler)
			if _, err := s.snapshot(context.Background()); err == nil {
				t.Fatal("invalid snapshot accepted")
			}
		})
	}
}
func TestColumnsRejectObjectsAndTooWideRows(t *testing.T) {
	for _, rows := range [][][]any{{{map[string]any{"x": 1}}}, {make([]any, 201)}} {
		if _, err := columnNames(rows); err == nil {
			t.Fatal("invalid columns accepted")
		}
	}
	if text, err := cellText(json.Number("1.234567890123456789e20")); err != nil || text != "1.234567890123456789e20" {
		t.Fatal(text, err)
	}
}
