package ignite

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sync/atomic"
	"testing"
)

func TestStatementPreservesExactCountAndDoesNotFetch(t *testing.T) {
	var calls atomic.Int32
	e, _ := setup(t, func(w http.ResponseWriter, _ *http.Request, form url.Values) {
		calls.Add(1)
		if form.Get("cmd") != "qryfldexe" || form.Get("qry") != "DELETE FROM events WHERE id=1" || form.Get("pageSize") != "1" || form.Get("cacheName") != "fixture cache" {
			t.Error("invalid statement request")
		}
		fmt.Fprint(w, pageJSON(`[{"fieldName":"UPDATED","fieldTypeName":"java.lang.Long"}]`, `[[9007199254740993]]`, true, 91))
	})
	count, err := e.ApplyStatement(context.Background(), "DELETE FROM events WHERE id=1")
	if err != nil || count == nil || *count != 9007199254740993 || calls.Load() != 1 {
		t.Fatal(count, err, calls.Load())
	}
}

func TestStatementInvalidAcknowledgementIsNeverReplayed(t *testing.T) {
	for _, response := range []string{
		`{"successStatus":1,"error":"private source details","response":null}`,
		pageJSON(longColumn, `[[1.5]]`, true, 92), pageJSON(longColumn, `[[-1]]`, true, 92),
		pageJSON(longColumn, `[[1],[2]]`, true, 92), pageJSON(longColumn, `[]`, true, 92),
		pageJSON(longColumn, `[[1]]`, false, 92), `{"successStatus":0`,
	} {
		t.Run(response, func(t *testing.T) {
			var writes atomic.Int32
			var closes atomic.Int32
			e, _ := setup(t, func(w http.ResponseWriter, _ *http.Request, form url.Values) {
				if form.Get("cmd") == "qrycls" {
					closes.Add(1)
					if form.Get("qryId") != "92" {
						t.Error("wrong cleanup cursor")
					}
					http.Error(w, "lost cleanup", http.StatusBadGateway)
					return
				}
				writes.Add(1)
				fmt.Fprint(w, response)
			})
			if count, err := e.ApplyStatement(context.Background(), "DELETE FROM events"); err == nil || count != nil || writes.Load() != 1 {
				t.Fatal(count, err, writes.Load())
			}
			if response == pageJSON(longColumn, `[[1]]`, false, 92) && closes.Load() != 1 {
				t.Fatal("unconsumed cursor leaked")
			}
		})
	}
}
