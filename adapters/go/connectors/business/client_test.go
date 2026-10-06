package business

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEndpointsAndCredentials(t *testing.T) {
	cases := map[string]string{"salesforce_data360": "https://api.salesforce.com/platform/mcp/v1/data/data360", "salesforce_tableau_next": "https://api.salesforce.com/platform/mcp/v1/analytics/tableau-next", "motherduck": "https://api.motherduck.com/mcp", "daloopa": "https://mcp.daloopa.com/server/mcp", "ramp": "https://api.ramp.com/developer/v1"}
	for kind, want := range cases {
		t.Run(kind, func(t *testing.T) {
			c, e := New(Config{Type: kind, Token: "secret"})
			if e != nil || c.endpoint != want {
				t.Fatalf("endpoint %v %v", c, e)
			}
			if kind == "daloopa" {
				if c.headers().Get("X-API-KEY") != "secret" || c.headers().Get("Authorization") != "" {
					t.Fatal("wrong Daloopa auth")
				}
			} else if c.headers().Get("Authorization") != "Bearer secret" {
				t.Fatal("wrong bearer auth")
			}
			for _, env := range []string{"https://attacker.example", "http://127.0.0.1"} {
				if _, e = New(Config{Type: kind, Token: "secret", Environment: env}); e == nil {
					t.Fatal("caller endpoint accepted")
				}
			}
			if _, e = New(Config{Type: kind}); e == nil {
				t.Fatal("missing credentials accepted")
			}
		})
	}
}
func TestReadOnlyBoundary(t *testing.T) {
	for _, tc := range []struct {
		kind, q string
		want    bool
	}{
		{"salesforce_data360", `{"operation":"list_tools"}`, true},
		{"salesforce_data360", `{"tool":"execute","arguments":{"toolName":"delete"}}`, false},
		{"motherduck", `{"tool":"query","arguments":{"query":"SELECT 1"}}`, true},
		{"motherduck", `{"tool":"query_rw","arguments":{"query":"DELETE FROM x"}}`, false},
		{"ramp", `{"resource":"transactions","params":{"page_size":"1"}}`, true},
		{"ramp", `{"resource":"../token"}`, false},
		{"daloopa", `{"operation":"list_tools","tool":"delete"}`, false},
		{"ramp", `{"resource":"transactions","method":"POST"}`, false},
		{"motherduck", `{"operation":"list_tools"} {"tool":"query_rw"}`, false},
	} {
		if ReadOnly(tc.kind, tc.q) != tc.want {
			t.Errorf("read boundary: %s %s", tc.kind, tc.q)
		}
	}
}
func TestMCPHandshakeDiscoveryAndContent(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			calls := []string{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer secret" {
					t.Error("lost auth")
				}
				if r.Method == "DELETE" {
					calls = append(calls, "close")
					return
				}
				var body struct {
					ID     int            `json:"id"`
					Method string         `json:"method"`
					Params map[string]any `json:"params"`
				}
				json.NewDecoder(r.Body).Decode(&body)
				calls = append(calls, body.Method)
				var result any
				switch body.Method {
				case "initialize":
					w.Header().Set("Mcp-Session-Id", "session")
					result = map[string]any{"protocolVersion": "2025-03-26"}
				case "notifications/initialized":
					w.WriteHeader(202)
					return
				case "tools/list":
					if r.Header.Get("Mcp-Session-Id") != "session" || r.Header.Get("MCP-Protocol-Version") != "2025-03-26" {
						t.Error("session/protocol missing")
					}
					result = map[string]any{"tools": []any{map[string]any{"name": "query", "inputSchema": map[string]any{"type": "object"}}}}
					if body.Params["cursor"] == nil {
						result.(map[string]any)["nextCursor"] = "page2"
					}
				case "tools/call":
					if body.Params["name"] != "query" {
						t.Error("tool altered")
					}
					args := body.Params["arguments"].(map[string]any)
					if args["query"] != "SELECT '\\x';" {
						t.Error("query altered")
					}
					result = map[string]any{"content": []any{map[string]any{"type": "text", "text": "data with citation"}}, "structuredContent": map[string]any{"nextCursor": "more", "rows": []int{1, 2}}}
				}
				b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": body.ID, "result": result})
				if stream {
					w.Header().Set("Content-Type", "text/event-stream")
					fmt.Fprintf(w, "event: message\ndata: %s\n\n", b)
				} else {
					w.Header().Set("Content-Type", "application/json")
					w.Write(b)
				}
			}))
			defer server.Close()
			c, _ := New(Config{Type: "motherduck", Token: "secret"})
			c.endpoint = server.URL
			rows, e := c.Query(context.Background(), `{"operation":"list_tools"}`)
			if e != nil || len(rows) != 1 || len(rows[0]["tools"].([]map[string]any)) != 2 {
				t.Fatalf("discovery: %v %v", rows, e)
			}
			q, _ := json.Marshal(Query{Tool: "query", Arguments: map[string]json.RawMessage{"query": json.RawMessage(`"SELECT '\\x';"`)}})
			rows, e = c.Query(context.Background(), string(q))
			if e != nil || rows[0]["structuredContent"] == nil || rows[0]["content"] == nil {
				t.Fatalf("content: %v %v", rows, e)
			}
			if calls[len(calls)-1] != "close" {
				t.Fatal("session not closed")
			}
		})
	}
}
func TestProviderErrorsAndRedirects(t *testing.T) {
	for _, failure := range []string{"redirect", "http", "rpc", "tool", "invalid", "oversized"} {
		t.Run(failure, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var q map[string]any
				json.NewDecoder(r.Body).Decode(&q)
				if failure == "redirect" {
					http.Redirect(w, r, "https://attacker.example", 302)
					return
				}
				if failure == "http" {
					http.Error(w, "secret", 401)
					return
				}
				if q["method"] == "initialize" {
					fmt.Fprintf(w, `{"id":1,"result":{"protocolVersion":"2025-03-26"}}`)
					return
				}
				if q["method"] == "notifications/initialized" {
					w.WriteHeader(202)
					return
				}
				switch failure {
				case "rpc":
					fmt.Fprintf(w, `{"id":3,"error":{"message":"secret"}}`)
				case "tool":
					fmt.Fprintf(w, `{"id":3,"result":{"isError":true,"content":[{"text":"secret"}]}}`)
				case "invalid":
					fmt.Fprint(w, `garbage`)
				case "oversized":
					fmt.Fprint(w, strings.Repeat("x", maxResponse+1))
				}
			}))
			defer srv.Close()
			c, _ := New(Config{Type: "motherduck", Token: "secret"})
			c.endpoint = srv.URL
			_, e := c.Query(context.Background(), `{"tool":"query","arguments":{}}`)
			if e == nil {
				t.Fatal("false success")
			}
			if strings.Contains(e.Error(), "secret") {
				t.Fatal("credential/body leaked")
			}
		})
	}
}
func TestRampPaginationAndTokenExchange(t *testing.T) {
	pages := 0
	tokens := 0
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/developer/v1/token" {
			tokens++
			id, secret, ok := r.BasicAuth()
			r.ParseForm()
			if !ok || id != "client" || secret != "secret" || r.Form.Get("scope") != "transactions:read" {
				t.Error("bad token exchange")
			}
			fmt.Fprint(w, `{"access_token":"short-lived"}`)
			return
		}
		if r.Header.Get("Authorization") != "Bearer short-lived" {
			t.Error("wrong Ramp token")
		}
		pages++
		if r.URL.Query().Get("start") == "next" {
			fmt.Fprint(w, `{"data":[{"id":"second"}],"page":{"next":null}}`)
		} else {
			json.NewEncoder(w).Encode(map[string]any{"data": []any{map[string]string{"id": "first"}}, "page": map[string]string{"next": srv.URL + "/developer/v1/transactions?start=next"}})
		}
	}))
	defer srv.Close()
	c, _ := New(Config{Type: "ramp", Token: "secret", ClientID: "client"})
	c.endpoint = srv.URL + "/developer/v1"
	rows, e := c.Query(context.Background(), `{"resource":"transactions","params":{"page_size":"1"}}`)
	if e != nil || len(rows) != 1 || rows[0]["rowCount"] != 2 || pages != 2 || tokens != 1 {
		t.Fatalf("pagination: %v %v pages=%d tokens=%d", rows, e, pages, tokens)
	}
}
func TestRampRejectsEscapingAndRepeatingPages(t *testing.T) {
	for _, next := range []string{"https://attacker.example/developer/v1/transactions", "/developer/v1/users", "/developer/v1/transactions"} {
		t.Run(next, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]any{"data": []any{}, "page": map[string]string{"next": next}})
			}))
			defer srv.Close()
			c, _ := New(Config{Type: "ramp", Token: "secret"})
			c.endpoint = srv.URL + "/developer/v1"
			if _, e := c.Query(context.Background(), `{"resource":"transactions"}`); e == nil {
				t.Fatal("unsafe pagination accepted")
			}
		})
	}
}
