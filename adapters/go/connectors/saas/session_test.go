package saas

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/provider"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type capture struct {
	documents []string
	schema    *arrow.Schema
}

func (c *capture) Schema(s *arrow.Schema) error { c.schema = s; return nil }
func (c *capture) Write(r arrow.RecordBatch) error {
	a := r.Column(0).(*array.Binary)
	for i := 0; i < a.Len(); i++ {
		c.documents = append(c.documents, string(a.Value(i)))
	}
	return nil
}
func nativeQuery(t *testing.T, engine, sql string) adapter.Native {
	t.Helper()
	raw, _ := json.Marshal(provider.SaaSQuery{SQL: sql})
	kind, spec, err := provider.Invocation(engine, raw)
	if err != nil {
		t.Fatal(err)
	}
	return adapter.Native{Kind: kind, Spec: *spec, Limits: adapter.Limits{MaxRows: 100, MaxBytes: 2 << 20, BatchRows: 16}}
}
func sessionFixture(t *testing.T, engine string, handler http.HandlerFunc) *Session {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	s := &Session{engine: engine, endpoint: server.URL, oauthEndpoint: server.URL + "/token", namespace: "123", token: "fixture-token", username: "fixture-developer", http: server.Client()}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestStripeExactAggregationAndCompletePaging(t *testing.T) {
	var calls atomic.Int64
	s := sessionFixture(t, "stripe", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != "GET" || r.URL.Path != "/v1/charges" || r.Header.Get("Authorization") != "Bearer fixture-token" {
			t.Error("unexpected request")
		}
		if r.URL.Query().Get("starting_after") == "" {
			fmt.Fprint(w, `{"data":[{"id":"ch_1","amount":9007199254740993,"currency":"usd","paid":true}],"has_more":true}`)
		} else {
			if r.URL.Query().Get("starting_after") != "ch_1" {
				t.Error("cursor changed")
			}
			fmt.Fprint(w, `{"data":[{"id":"ch_2","amount":7,"currency":"usd","paid":true},{"id":"ch_3","amount":99,"currency":"eur","paid":false}],"has_more":false}`)
		}
	})
	s.namespace = ""
	sink := &capture{}
	n := nativeQuery(t, "stripe", "SELECT currency, SUM(amount) AS total, COUNT(*) AS n FROM charges WHERE paid = true GROUP BY currency HAVING total > 0 ORDER BY total DESC LIMIT 1")
	result, err := s.RunNative(context.Background(), n, sink)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != operations.Completed || calls.Load() != 2 || len(sink.documents) != 1 || !strings.Contains(sink.documents[0], `"total":9007199254741000`) || !strings.Contains(sink.documents[0], `"n":2`) {
		t.Fatalf("bad aggregation: %+v calls=%d documents=%v", result, calls.Load(), sink.documents)
	}
	if sink.schema == nil || sink.schema.Field(0).Name != "document" {
		t.Fatal("missing document format")
	}
}
func TestStripeNeverReturnsPartialOrIgnoredClauses(t *testing.T) {
	var calls atomic.Int64
	s := sessionFixture(t, "stripe", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		fmt.Fprint(w, `{"data":[{"id":"x"}],"has_more":true}`)
	})
	s.namespace = ""
	_, err := s.RunNative(context.Background(), nativeQuery(t, "stripe", "SELECT * FROM customers"), &capture{})
	if err == nil {
		t.Fatal("repeated page accepted")
	}
	before := calls.Load()
	for _, sql := range []string{"SELECT id FROM customers WHERE email LIKE 'a%'", "SELECT id FROM customers UNION SELECT id FROM charges", "SELECT id FROM customers LIMIT 1 FOR UPDATE", "SELECT id FROM customers WHERE id = 'x' OR id = 'y'", "SELECT id FROM customers ORDER BY missing", "SELECT AVG(balance) FROM customers", "SELECT id, balance FROM customers GROUP BY id"} {
		p, e := parseSelect(sql)
		if e == nil {
			e = validateLocalPlan(p, stripeEntities[p.table].fields)
		}
		if e == nil {
			t.Errorf("unsupported SQL accepted: %s", sql)
		}
	}
	if before != calls.Load() {
		t.Fatal("unsupported query performed network")
	}
}
func TestStripeLimitProjectionAndDecimalNull(t *testing.T) {
	s := sessionFixture(t, "stripe", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("limit") != "1" {
			t.Error("limit not pushed")
		}
		fmt.Fprint(w, `{"data":[{"id":"c","balance":null}],"has_more":true}`)
	})
	s.namespace = ""
	rows, err := s.stripe(context.Background(), "SELECT id, balance FROM customers LIMIT 1", adapter.Limits{MaxRows: 1, MaxBytes: 1024})
	if err != nil || len(rows) != 1 || rows[0]["balance"] != nil {
		t.Fatalf("null/limit: %v %v", rows, err)
	}
	p, err := parseSelect("SELECT SUM(amount) AS total FROM charges")
	if err != nil {
		t.Fatal(err)
	}
	rows, err = evaluate([]map[string]any{{"amount": json.Number("12345678901234567890.1234567")}, {"amount": json.Number("0.0000001")}}, p)
	if err != nil || rows[0]["total"] != json.Number("12345678901234567890.1234568") {
		t.Fatalf("decimal loss %v %v", rows, err)
	}
}
func TestGoogleAdsPreservesQueryAndPagination(t *testing.T) {
	query := "SELECT campaign.id, campaign.name FROM campaign WHERE campaign.name = 'Delete; Me'"
	var calls atomic.Int64
	s := sessionFixture(t, "google_ads", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/v25/customers/123/googleAds:search" || r.Header.Get("developer-token") != "fixture-developer" || r.Header.Get("login-customer-id") != "456" {
			t.Error("account or token scope changed")
		}
		var body map[string]any
		if json.NewDecoder(r.Body).Decode(&body) != nil || body["query"] != query {
			t.Error("query changed")
		}
		if body["pageToken"] == nil {
			fmt.Fprint(w, `{"results":[{"campaign":{"id":9007199254740993,"name":"Delete; Me"}}],"nextPageToken":"next"}`)
		} else {
			fmt.Fprint(w, `{"results":[{"campaign":{"id":2}}]}`)
		}
	})
	s.loginCustomer = "456"
	sink := &capture{}
	result, err := s.RunNative(context.Background(), nativeQuery(t, "google_ads", query), sink)
	if err != nil || result.Outcome != operations.Completed || len(sink.documents) != 2 || calls.Load() != 2 || !strings.Contains(sink.documents[0], "9007199254740993") {
		t.Fatalf("query: %+v %v %v", result, err, sink.documents)
	}
}
func TestGA4ReportsUseTypedPlansAndExactNumbers(t *testing.T) {
	var reports atomic.Int64
	s := sessionFixture(t, "ga4", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/metadata") {
			fmt.Fprint(w, `{"dimensions":[{"apiName":"country"},{"apiName":"date"}],"metrics":[{"apiName":"sessions"}]}`)
			return
		}
		if r.URL.Path != "/v1beta/properties/123:runReport" {
			t.Error("wrong property")
		}
		reports.Add(1)
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		raw, _ := json.Marshal(body)
		if !strings.Contains(string(raw), `"value":"United States"`) || !strings.Contains(string(raw), `"startDate":"2026-01-01"`) {
			t.Errorf("missing filters: %s", raw)
		}
		value := "9007199254740993"
		if body["offset"] == "1" {
			value = "2"
		}
		fmt.Fprintf(w, `{"dimensionHeaders":[{"name":"country"}],"metricHeaders":[{"name":"sessions","type":"TYPE_INTEGER"}],"rows":[{"dimensionValues":[{"value":"United States"}],"metricValues":[{"value":"%s"}]}],"rowCount":2}`, value)
	})
	s.username = ""
	sink := &capture{}
	result, err := s.RunNative(context.Background(), nativeQuery(t, "ga4", "SELECT country, sessions AS visits FROM ga4 WHERE date BETWEEN '2026-01-01' AND '2026-01-31' AND country = 'United States' GROUP BY country ORDER BY visits DESC"), sink)
	if err != nil || result.Outcome != operations.Completed || reports.Load() != 2 || !strings.Contains(sink.documents[0], `"visits":9007199254740993`) {
		t.Fatalf("report %+v %v %v", result, err, sink.documents)
	}
}
func TestFacebookAccountFiltersPagingAndExactSpend(t *testing.T) {
	var calls atomic.Int64
	var endpoint string
	s := sessionFixture(t, "facebook_ads", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/v24.0/act_123/insights" || r.URL.Query().Get("access_token") != "" {
			t.Error("scope changed")
		}
		if !strings.Contains(r.URL.Query().Get("filtering"), "Keep Case") {
			t.Error("filter lost")
		}
		if r.URL.Query().Get("after") == "" {
			fmt.Fprintf(w, `{"data":[{"campaign_name":"Keep Case","spend":"123456789012345.12345"}],"paging":{"next":%q,"cursors":{"after":"second"}}}`, endpoint+"/v24.0/act_123/insights?after=second")
		} else {
			fmt.Fprint(w, `{"data":[{"campaign_name":"Keep Case","spend":"2.00"}]}`)
		}
	})
	endpoint = s.endpoint
	s.username = ""
	sink := &capture{}
	result, err := s.RunNative(context.Background(), nativeQuery(t, "facebook_ads", "SELECT campaign_name, spend AS total FROM fb_ads WHERE campaign_name = 'Keep Case' ORDER BY total DESC LIMIT 1"), sink)
	if err != nil || result.Outcome != operations.Completed || calls.Load() != 2 || len(sink.documents) != 1 || !strings.Contains(sink.documents[0], `"total":123456789012345.12345`) {
		t.Fatalf("insights: %+v %v %v", result, err, sink.documents)
	}
}
func TestSalesforcePreservesSOQLAndRejectsCrossOriginPage(t *testing.T) {
	query := "SELECT Id, Name FROM Account WHERE Name = 'Keep Case'"
	var seen string
	s := sessionFixture(t, "salesforce", func(w http.ResponseWriter, r *http.Request) {
		seen = r.URL.Query().Get("q")
		fmt.Fprint(w, `{"records":[{"Id":"001"}],"done":false,"nextRecordsUrl":"https://attacker.example/services/data/v65.0/query/next"}`)
	})
	s.username = ""
	_, err := s.salesforce(context.Background(), query, adapter.Limits{MaxRows: 10, MaxBytes: 4096})
	if err == nil || seen != query {
		t.Fatalf("SOQL scope: %s %v", seen, err)
	}
}
func TestCredentialAndTransportBoundaries(t *testing.T) {
	for _, raw := range []string{"", "token\nheader", `{"access_token":"ok","extra":"ignored"}`, `{"type":"service_account","client_email":"x.gserviceaccount.com","token_uri":"https://attacker.example/token","private_key":"x"}`} {
		if _, err := parseGoogleSecret(raw); err == nil {
			t.Errorf("invalid Google credential accepted %q", raw)
		}
	}
	if _, _, err := salesforcePassword("ambiguous-password-and-token"); err == nil {
		t.Fatal("ambiguous Salesforce password accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := GoogleAccessToken(ctx, "token", "https://www.googleapis.com/auth/spreadsheets.readonly"); err == nil {
		t.Fatal("cancelled token request accepted")
	}
	if _, err := GoogleAccessToken(context.Background(), "token", "https://www.googleapis.com/auth/cloud-platform"); err == nil {
		t.Fatal("unapproved scope accepted")
	}
	for _, engine := range []string{"stripe", "ga4", "google_ads", "facebook_ads", "salesforce"} {
		c := adapter.Connection{TenantID: "team", ConnectionID: "saved", Revision: "1", Engine: engine, Namespace: "123", Token: "token", Host: "attacker.example"}
		if _, err := (Driver{Engine: engine}).Open(context.Background(), c); err == nil {
			t.Fatalf("host override accepted %s", engine)
		}
	}
	var redirected atomic.Int64
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1) }))
	defer target.Close()
	s := sessionFixture(t, "stripe", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", target.URL)
		w.WriteHeader(http.StatusTemporaryRedirect)
	})
	s.http.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	_, err := s.request(context.Background(), "GET", s.endpoint, nil, bearer(s.token), &budget{limits: adapter.Limits{MaxBytes: 4096}})
	if err == nil || redirected.Load() != 0 {
		t.Fatal("redirect followed")
	}
	delay := sessionFixture(t, "stripe", func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() })
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = delay.request(ctx, "GET", delay.endpoint, nil, bearer(delay.token), &budget{limits: adapter.Limits{MaxBytes: 4096}})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancellation lost: %v", err)
	}
}
func TestKeywordPlanRejectsIgnoredClausesAndKeepsCase(t *testing.T) {
	body, err := keywordRequest("FIND KEYWORDS WHERE keywords IN ('Data Platform','Keep Case') AND page_url = 'https://example.com/Case' AND start_year = '2025' AND start_month = 'JANUARY' AND end_year = '2025' AND end_month = 'DECEMBER'")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(body)
	if !strings.Contains(string(raw), "Keep Case") || !strings.Contains(string(raw), "/Case") {
		t.Fatal("keyword values lowercased")
	}
	for _, q := range []string{"FIND KEYWORDS WHERE keywords IN ('one') AND ignored = 'x'", "FIND KEYWORDS WHERE keywords IN ('one') LIMIT 1", "FIND KEYWORDS WHERE keywords IN ('one') AND start_year = '2025'"} {
		if _, err := keywordRequest(q); err == nil {
			t.Errorf("ignored clause accepted %s", q)
		}
	}
}
