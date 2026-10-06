package provider

import (
	"encoding/json"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/operations"
)

// ElasticsearchQuery keeps the original HTTP body, including NDJSON bulk data.
// The destination always comes from the freshly resolved saved connection.
type ElasticsearchQuery struct {
	Method string `json:"method"`
	Path   string `json:"path"`
	Body   string `json:"body,omitempty"`
}

func ParseElasticsearchInvocation(input string) (operations.Kind, *operations.NativeSpec, error) {
	var q ElasticsearchQuery
	if strings.HasPrefix(strings.TrimSpace(input), "{") {
		if operations.DecodeStrict([]byte(input), &q, operations.MaxRequestBytes) != nil {
			return "", nil, operations.ErrInvalid
		}
	} else {
		input = strings.TrimLeft(input, " \t\r\n")
		i := strings.IndexAny(input, " \t\r\n")
		if i < 0 {
			return "", nil, operations.ErrInvalid
		}
		q.Method = strings.ToUpper(input[:i])
		rest := strings.TrimLeft(input[i:], " \t\r\n")
		i = strings.IndexAny(rest, " \t\r\n")
		if i < 0 {
			q.Path = rest
		} else {
			q.Path = rest[:i]
			q.Body = strings.TrimLeft(rest[i:], " \t\r\n")
		}
	}
	if strings.HasSuffix(strings.SplitN(q.Path, "?", 2)[0], "/_bulk") && q.Body != "" && !strings.HasSuffix(q.Body, "\n") {
		q.Body += "\n"
	}
	raw, err := json.Marshal(q)
	if err != nil {
		return "", nil, operations.ErrInvalid
	}
	_, kind, err := ParseElasticsearch(raw)
	if err != nil {
		return "", nil, err
	}
	return kind, &operations.NativeSpec{Provider: "elasticsearch", Command: "query", Parameters: []operations.Parameter{{Type: "json", Value: raw}}, ReturnResult: kind == operations.NativeExecute}, nil
}

func ParseElasticsearch(raw []byte) (ElasticsearchQuery, operations.Kind, error) {
	var q ElasticsearchQuery
	if operations.DecodeStrict(raw, &q, operations.MaxRequestBytes) != nil || len(q.Path) > 4096 || len(q.Body) > 256<<10 || !utf8.ValidString(q.Body) || strings.ContainsRune(q.Body, 0) {
		return q, "", operations.ErrInvalid
	}
	u, err := url.Parse(q.Path)
	if err != nil || u.IsAbs() || u.Host != "" || u.User != nil || u.Fragment != "" || u.Opaque != "" || u.RawPath != "" || !strings.HasPrefix(q.Path, "/") || strings.ContainsAny(u.Path, "%\\\x00\r\n\t ") || strings.Contains(u.Path, "//") {
		return q, "", operations.ErrInvalid
	}
	for _, segment := range strings.Split(u.Path, "/") {
		if segment == "." || segment == ".." {
			return q, "", operations.ErrInvalid
		}
	}
	params, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return q, "", operations.ErrInvalid
	}
	for key, values := range params {
		if len(values) != 1 || len(key) > 128 || len(values[0]) > 8192 || strings.ContainsAny(key+values[0], "\x00\r\n") {
			return q, "", operations.ErrInvalid
		}
	}
	if q.Method != "GET" && q.Method != "HEAD" && q.Method != "POST" && q.Method != "PUT" && q.Method != "DELETE" {
		return q, "", operations.ErrUnsupported
	}
	path := strings.Trim(u.Path, "/")
	parts := strings.Split(path, "/")
	kind := operations.Kind("")
	// Cluster administration, scripts, snapshots, remote reindex and settings are
	// not part of a tenant database operation. These need separate operator APIs.
	if path == "" && q.Method == "GET" || path == "_cluster/health" && q.Method == "GET" || path == "_cat/indices" && q.Method == "GET" {
		kind = operations.NativeRead
	}
	action := parts[len(parts)-1]
	if (q.Method == "GET" || q.Method == "POST") && (action == "_search" || action == "_count" || action == "_mget" || action == "_field_caps" || action == "_sql") && (len(parts) == 1 || len(parts) == 2 && !strings.HasPrefix(parts[0], "_")) {
		kind = operations.NativeRead
	}
	if len(parts) == 1 && !strings.HasPrefix(parts[0], "_") && parts[0] != "" {
		switch q.Method {
		case "GET", "HEAD":
			kind = operations.NativeRead
		case "PUT", "DELETE":
			kind = operations.NativeExecute
		}
	}
	if len(parts) == 2 && !strings.HasPrefix(parts[0], "_") && parts[0] != "" {
		if action == "_mapping" && q.Method == "GET" {
			kind = operations.NativeRead
		}
		if action == "_mapping" && q.Method == "PUT" || action == "_doc" && q.Method == "POST" || action == "_bulk" && q.Method == "POST" {
			kind = operations.NativeExecute
		}
	}
	if path == "_bulk" && q.Method == "POST" {
		kind = operations.NativeExecute
	}
	if len(parts) == 3 && !strings.HasPrefix(parts[0], "_") && parts[2] != "" {
		if parts[1] == "_doc" {
			switch q.Method {
			case "GET", "HEAD":
				kind = operations.NativeRead
			case "PUT", "POST", "DELETE":
				kind = operations.NativeExecute
			}
		}
		if (parts[1] == "_create" && (q.Method == "PUT" || q.Method == "POST")) || (parts[1] == "_update" && q.Method == "POST") {
			kind = operations.NativeExecute
		}
	}
	if kind == "" {
		return q, "", operations.ErrUnsupported
	}
	if q.Method == "HEAD" && q.Body != "" || q.Method == "DELETE" && q.Body != "" {
		return q, "", operations.ErrInvalid
	}
	if action == "_bulk" {
		if _, err := elasticsearchBulk(q.Body, ""); err != nil {
			return q, "", err
		}
	} else if q.Body != "" {
		var body map[string]json.RawMessage
		if DecodeDocument([]byte(q.Body), &body, 256<<10) != nil || body == nil {
			return q, "", operations.ErrInvalid
		}
	}
	return q, kind, nil
}

// ElasticsearchScope rejects paths and bulk action metadata outside a selected
// index. An empty saved database means the source credentials own index scope.
func ElasticsearchScope(q ElasticsearchQuery, index string) error {
	u, err := url.Parse(q.Path)
	if err != nil {
		return operations.ErrInvalid
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	target := ""
	if len(parts) > 0 && !strings.HasPrefix(parts[0], "_") {
		target = parts[0]
	}
	if index != "" && target != "" && target != index {
		return operations.ErrUnsupported
	}
	if strings.HasSuffix(u.Path, "/_bulk") {
		bound := index
		if target != "" {
			bound = target
		}
		_, err = elasticsearchBulk(q.Body, bound)
		return err
	}
	if index != "" && strings.HasSuffix(u.Path, "/_mget") && q.Body != "" {
		var body struct {
			Docs []map[string]json.RawMessage `json:"docs"`
		}
		if json.Unmarshal([]byte(q.Body), &body) != nil {
			return operations.ErrInvalid
		}
		for _, doc := range body.Docs {
			if raw, ok := doc["_index"]; ok {
				var selected string
				if json.Unmarshal(raw, &selected) != nil || selected != index {
					return operations.ErrUnsupported
				}
			}
		}
	}
	if index != "" && target == "" && u.Path != "/" && u.Path != "/_cluster/health" {
		return operations.ErrUnsupported
	}
	return nil
}
func elasticsearchBulk(body, index string) (int, error) {
	if body == "" || !strings.HasSuffix(body, "\n") {
		return 0, operations.ErrInvalid
	}
	lines := strings.Split(strings.TrimSuffix(body, "\n"), "\n")
	count := 0
	for i := 0; i < len(lines); i++ {
		var action map[string]json.RawMessage
		if DecodeDocument([]byte(lines[i]), &action, 64<<10) != nil || len(action) != 1 {
			return 0, operations.ErrInvalid
		}
		for name, raw := range action {
			if name != "index" && name != "create" && name != "update" && name != "delete" {
				return 0, operations.ErrUnsupported
			}
			var metadata map[string]json.RawMessage
			if DecodeDocument(raw, &metadata, 64<<10) != nil || metadata == nil {
				return 0, operations.ErrInvalid
			}
			if rawIndex, ok := metadata["_index"]; ok {
				var selected string
				if json.Unmarshal(rawIndex, &selected) != nil || selected == "" || index != "" && selected != index {
					return 0, operations.ErrUnsupported
				}
			}
			if name != "delete" {
				i++
				if i >= len(lines) {
					return 0, operations.ErrInvalid
				}
				var doc map[string]json.RawMessage
				if DecodeDocument([]byte(lines[i]), &doc, 64<<10) != nil || doc == nil {
					return 0, operations.ErrInvalid
				}
			}
		}
		count++
		if count > 1000 {
			return 0, operations.ErrInvalid
		}
	}
	return count, nil
}
