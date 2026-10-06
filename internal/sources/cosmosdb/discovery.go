package cosmosdb

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/rowarrow"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
)

// Inspect exposes database/container metadata. Schemaless documents are marked
// explicitly; key discovery does not pretend to describe every document field.
func (e *Engine) Inspect(parent context.Context, spec operations.MetadataSpec, sink query.Sink) (stats query.Stats, err error) {
	if e == nil || parent == nil || sink == nil || spec.ObjectKind != "" || spec.Limit < 1 || int64(spec.Limit) > e.limits.MaxRows || spec.Limit > 10000 {
		return stats, query.NewError("INVALID_ARGUMENT", "Invalid Cosmos metadata request")
	}
	db, container := e.source.Options["database"], e.source.Options["container"]
	if spec.Target.Catalog != "" && spec.Target.Catalog != db || spec.Target.Schema != "" && container != "" && spec.Target.Schema != container {
		return stats, query.NewError("PERMISSION_DENIED", "Cosmos metadata namespace differs")
	}
	if spec.Target.Schema != "" {
		container = spec.Target.Schema
	}
	if spec.Target.Name != "" {
		if container != "" && container != spec.Target.Name {
			return stats, query.NewError("PERMISSION_DENIED", "Cosmos metadata container differs")
		}
		container = spec.Target.Name
	}
	if container != "" && !component.MatchString(container) {
		return stats, query.NewError("INVALID_ARGUMENT", "Invalid Cosmos container")
	}
	offset := 0
	if spec.Cursor != "" {
		offset, err = strconv.Atoi(spec.Cursor)
		if err != nil || offset < 0 || offset > 10000 || strconv.Itoa(offset) != spec.Cursor {
			return stats, query.NewError("INVALID_ARGUMENT", "Invalid Cosmos metadata cursor")
		}
	}
	if offset+spec.Limit > 10000 {
		return stats, query.NewError("RESOURCE_EXHAUSTED", "Cosmos metadata window exceeds limit")
	}
	ctx, cancel := context.WithTimeout(parent, e.limits.Timeout)
	defer cancel()
	path, kind, resource := "/dbs/"+db+"/colls", "colls", "dbs/"+db
	switch spec.Object {
	case "catalogs", "databases":
		if spec.Target.Name != "" {
			return stats, query.NewError("INVALID_ARGUMENT", "Database metadata does not accept a container target")
		}
		path, kind, resource = "/dbs/"+db, "dbs", "dbs/"+db
	case "schemas", "tables":
		if container != "" {
			path += "/" + container
			resource += "/colls/" + container
		}
	case "columns":
		if container == "" {
			return stats, query.NewError("INVALID_ARGUMENT", "Cosmos columns require a container")
		}
		path += "/" + container
		resource += "/colls/" + container
	default:
		return stats, query.NewError("UNSUPPORTED", "Cosmos metadata object is unsupported")
	}
	names := []string{}
	next := ""
	seen := map[string]bool{}
	charge := new(big.Rat)
	for page := 0; page < 1000; page++ {
		raw, headers, n, callErr := e.discoveryGet(ctx, path, kind, resource, next)
		stats.WireBytes += n
		if callErr != nil {
			return stats, callErr
		}
		if stats.WireBytes > e.limits.MaxBytes {
			return stats, query.NewError("RESOURCE_EXHAUSTED", "Cosmos metadata response budget exceeded")
		}
		ru, headerErr := singleHeader(headers, "x-ms-request-charge")
		if headerErr != nil || !chargeNumber.MatchString(ru) {
			return stats, query.NewError("QUERY_FAILED", "Invalid Cosmos metadata request charge")
		}
		value, ok := new(big.Rat).SetString(ru)
		if !ok {
			return stats, query.NewError("QUERY_FAILED", "Invalid Cosmos request charge")
		}
		charge.Add(charge, value)
		if charge.Cmp(new(big.Rat).SetInt64(e.maxRequestUnits)) > 0 {
			return stats, query.NewError("RESOURCE_EXHAUSTED", "Cosmos metadata request charge exceeded")
		}
		if path == "/dbs/"+db || container != "" {
			var item struct {
				ID string `json:"id"`
			}
			if json.Unmarshal(raw, &item) != nil || item.ID == "" || path == "/dbs/"+db && item.ID != db || container != "" && path != "/dbs/"+db && item.ID != container {
				return stats, query.NewError("QUERY_FAILED", "Cosmos metadata identity differs")
			}
			names = append(names, item.ID)
			break
		}
		var result struct {
			Count *int `json:"_count"`
			Items []struct {
				ID string `json:"id"`
			} `json:"DocumentCollections"`
		}
		if json.Unmarshal(raw, &result) != nil || result.Count == nil || *result.Count != len(result.Items) || len(result.Items) > 100 {
			return stats, query.NewError("QUERY_FAILED", "Invalid Cosmos container page")
		}
		for _, item := range result.Items {
			if !component.MatchString(item.ID) || seen[item.ID] {
				return stats, query.NewError("QUERY_FAILED", "Invalid Cosmos container identity")
			}
			seen[item.ID] = true
			names = append(names, item.ID)
		}
		following, err := singleHeader(headers, "x-ms-continuation")
		if err != nil || following != "" && following == next {
			return stats, query.NewError("QUERY_FAILED", "Invalid Cosmos metadata continuation")
		}
		if following == "" || len(names) >= offset+spec.Limit {
			break
		}
		if page == 999 {
			return stats, query.NewError("RESOURCE_EXHAUSTED", "Cosmos metadata page limit exceeded")
		}
		next = following
	}
	fields := []arrow.Field{{Name: "catalog", Type: arrow.BinaryTypes.String}}
	if spec.Object == "schemas" || spec.Object == "tables" {
		fields = append(fields, arrow.Field{Name: "schema_name", Type: arrow.BinaryTypes.String})
	}
	if spec.Object == "tables" {
		fields = append(fields, arrow.Field{Name: "name", Type: arrow.BinaryTypes.String}, arrow.Field{Name: "type", Type: arrow.BinaryTypes.String})
	}
	if spec.Object == "columns" {
		fields = []arrow.Field{{Name: "schema_name", Type: arrow.BinaryTypes.String}, {Name: "table_name", Type: arrow.BinaryTypes.String}, {Name: "name", Type: arrow.BinaryTypes.String}, {Name: "type", Type: arrow.BinaryTypes.String}, {Name: "representation", Type: arrow.BinaryTypes.String}}
	}
	writer, err := rowarrow.NewWriter(arrow.NewSchema(fields, nil), e.limits, sink)
	if err != nil {
		return stats, err
	}
	defer writer.Close()
	if spec.Object == "columns" {
		if offset == 0 {
			err = writer.Write([]any{container, container, "document", "JSON", "schemaless_document"})
		}
	} else {
		for i := offset; i < len(names) && i < offset+spec.Limit; i++ {
			row := []any{db}
			if spec.Object == "schemas" || spec.Object == "tables" {
				row = append(row, names[i])
			}
			if spec.Object == "tables" {
				row = append(row, names[i], "CONTAINER")
			}
			if err = writer.Write(row); err != nil {
				break
			}
		}
	}
	if err != nil {
		return stats, err
	}
	out, err := writer.Finish()
	out.WireBytes = stats.WireBytes
	out.Backend = "cosmosdb"
	return out, err
}
func (e *Engine) discoveryGet(ctx context.Context, path, kind, resource, continuation string) (json.RawMessage, http.Header, int64, error) {
	origin := *e.client.Origin
	origin.Path = path
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, origin.String(), nil)
	if err != nil {
		return nil, nil, 0, query.NewError("QUERY_FAILED", "Cannot prepare Cosmos metadata")
	}
	date := e.now().UTC().Format(http.TimeFormat)
	authorization := url.QueryEscape("type=aad&ver=1.0&sig=" + e.client.Token)
	if e.source.Options["auth"] == "master_key" {
		authorization = masterAuthorization(http.MethodGet, kind, resource, date, e.key)
	}
	request.Header.Set("Authorization", authorization)
	request.Header.Set("x-ms-date", date)
	request.Header.Set("x-ms-version", "2018-12-31")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("x-ms-max-item-count", "100")
	if continuation != "" {
		request.Header.Set("x-ms-continuation", continuation)
	}
	response, err := e.client.HTTP.Do(request)
	if err != nil {
		return nil, nil, 0, query.NewError("QUERY_FAILED", "Cosmos metadata request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return nil, nil, 0, query.NewError("QUERY_FAILED", "Cosmos metadata rejected")
	}
	if encoding := response.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return nil, nil, 0, query.NewError("QUERY_FAILED", "Unsupported Cosmos metadata encoding")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, e.client.Limit+1))
	n := int64(len(body))
	if err != nil || n > e.client.Limit {
		return nil, nil, n, query.NewError("RESOURCE_EXHAUSTED", "Cosmos metadata response exceeds budget")
	}
	var raw json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(body))
	if decoder.Decode(&raw) != nil || decoder.Decode(new(any)) != io.EOF || !strings.HasPrefix(strings.TrimSpace(string(raw)), "{") {
		return nil, nil, n, query.NewError("QUERY_FAILED", "Invalid Cosmos metadata JSON")
	}
	return raw, response.Header, n, nil
}
