package nativereader

import (
	"context"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/sources/awsapi"
	"github.com/SYNEHQ/kelvo-go/internal/sources/rowarrow"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
)

var elasticIndex = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,254}$`)
var dynamoTable = regexp.MustCompile(`^[A-Za-z0-9_.-]{3,255}$`)

func metadataOffset(spec operations.MetadataSpec) (int, error) {
	if spec.Limit < 1 || spec.Limit > 10000 || spec.ObjectKind != "" {
		return 0, adapter.ErrUnsupported
	}
	if spec.Cursor == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(spec.Cursor)
	if err != nil || n < 0 || n > 1000000 || strconv.Itoa(n) != spec.Cursor {
		return 0, adapter.ErrInvalid
	}
	return n, nil
}
func (s *Session) emitMetadata(ctx context.Context, spec operations.MetadataSpec, limits adapter.Limits, sink adapter.Sink, fields []arrow.Field, rows [][]any) (stats adapter.QueryStats, err error) {
	started := time.Now()
	defer func() { stats.Elapsed = time.Since(started) }()
	offset, err := metadataOffset(spec)
	if err != nil {
		return stats, err
	}
	if int64(spec.Limit) > limits.MaxRows {
		return stats, adapter.ErrLimit
	}
	l, err := s.limits(ctx, limits.MaxRows, limits.MaxBytes)
	if err != nil {
		return stats, err
	}
	writer, err := rowarrow.NewWriter(arrow.NewSchema(fields, nil), l, splitSink{next: sink, rows: int64(limits.BatchRows)})
	if err != nil {
		return stats, err
	}
	defer writer.Close()
	for i := offset; i < len(rows) && i < offset+spec.Limit; i++ {
		if err = ctx.Err(); err != nil {
			return stats, err
		}
		if err = writer.Write(rows[i]); err != nil {
			return stats, err
		}
	}
	out, err := writer.Finish()
	stats.Rows, stats.Bytes = out.Rows, out.Bytes
	return stats, err
}
func textField(name string) arrow.Field {
	return arrow.Field{Name: name, Type: arrow.BinaryTypes.String}
}
func (s *Session) inspectElastic(ctx context.Context, spec operations.MetadataSpec, limits adapter.Limits, sink adapter.Sink) (adapter.QueryStats, error) {
	if _, err := metadataOffset(spec); err != nil {
		return adapter.QueryStats{}, err
	}
	if spec.Target.Catalog != "" && spec.Target.Catalog != s.spec.Database || spec.Target.Schema != "" {
		return adapter.QueryStats{}, adapter.ErrUnsupported
	}
	c, headers, err := s.elasticClient(ctx, limits.MaxRows, limits.MaxBytes)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	defer c.Close()
	index := s.spec.Database
	if spec.Target.Name != "" {
		if index != "" && index != spec.Target.Name {
			return adapter.QueryStats{}, adapter.ErrUnsupported
		}
		index = spec.Target.Name
	}
	if index != "" && !elasticIndex.MatchString(index) {
		return adapter.QueryStats{}, adapter.ErrInvalid
	}
	switch spec.Object {
	case "catalogs", "databases", "schemas", "tables":
		if spec.Object == "schemas" && (index == "" || spec.Target.Name != "") {
			return adapter.QueryStats{}, adapter.ErrUnsupported
		}
		path := "/_cat/indices"
		if index != "" {
			path += "/" + url.PathEscape(index)
		}
		path += "?format=json&h=index"
		var result []struct {
			Name string `json:"index"`
		}
		_, _, err = c.Do(ctx, http.MethodGet, path, nil, headers, &result)
		if err != nil {
			return adapter.QueryStats{}, err
		}
		if len(result) > 10000 {
			return adapter.QueryStats{}, adapter.ErrLimit
		}
		sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
		rows := make([][]any, 0, len(result))
		last := ""
		for _, item := range result {
			if !elasticIndex.MatchString(item.Name) || index != "" && item.Name != index || item.Name == last {
				return adapter.QueryStats{}, adapter.ErrInvalid
			}
			last = item.Name
			switch spec.Object {
			case "catalogs", "databases":
				rows = append(rows, []any{item.Name})
			case "schemas":
				rows = append(rows, []any{item.Name, ""})
			case "tables":
				rows = append(rows, []any{item.Name, "", item.Name, "INDEX"})
			}
		}
		fields := []arrow.Field{textField("catalog")}
		if spec.Object == "schemas" || spec.Object == "tables" {
			fields = append(fields, textField("schema_name"))
		}
		if spec.Object == "tables" {
			fields = append(fields, textField("name"), textField("type"))
		}
		return s.emitMetadata(ctx, spec, limits, sink, fields, rows)
	case "columns":
		if index == "" {
			return adapter.QueryStats{}, adapter.ErrInvalid
		}
		var result struct {
			Fields map[string]map[string]struct {
				Type         string `json:"type"`
				Searchable   bool   `json:"searchable"`
				Aggregatable bool   `json:"aggregatable"`
			} `json:"fields"`
		}
		_, _, err = c.Do(ctx, http.MethodGet, "/"+url.PathEscape(index)+"/_field_caps?fields=*", nil, headers, &result)
		if err != nil {
			return adapter.QueryStats{}, err
		}
		if result.Fields == nil || len(result.Fields) > 10000 {
			return adapter.QueryStats{}, adapter.ErrLimit
		}
		names := make([]string, 0, len(result.Fields))
		for name := range result.Fields {
			names = append(names, name)
		}
		sort.Strings(names)
		rows := [][]any{}
		for _, name := range names {
			if len(name) > 4096 || strings.ContainsAny(name, "\x00\r\n") {
				return adapter.QueryStats{}, adapter.ErrInvalid
			}
			types := result.Fields[name]
			if len(types) != 1 {
				return adapter.QueryStats{}, adapter.ErrUnsupported
			}
			for _, field := range types {
				if len(field.Type) == 0 || len(field.Type) > 128 {
					return adapter.QueryStats{}, adapter.ErrInvalid
				}
				rows = append(rows, []any{"", index, name, field.Type, "mapped_field", field.Searchable, field.Aggregatable})
			}
		}
		return s.emitMetadata(ctx, spec, limits, sink, []arrow.Field{textField("schema_name"), textField("table_name"), textField("name"), textField("type"), textField("representation"), {Name: "searchable", Type: arrow.FixedWidthTypes.Boolean}, {Name: "aggregatable", Type: arrow.FixedWidthTypes.Boolean}}, rows)
	default:
		return adapter.QueryStats{}, adapter.ErrUnsupported
	}
}
func (s *Session) inspectDynamo(ctx context.Context, spec operations.MetadataSpec, limits adapter.Limits, sink adapter.Sink) (adapter.QueryStats, error) {
	if _, err := metadataOffset(spec); err != nil {
		return adapter.QueryStats{}, err
	}
	if spec.Target.Catalog != "" && spec.Target.Catalog != s.spec.Database || spec.Target.Schema != "" {
		return adapter.QueryStats{}, adapter.ErrUnsupported
	}
	table := s.spec.Database
	if spec.Target.Name != "" {
		if table != "" && table != spec.Target.Name {
			return adapter.QueryStats{}, adapter.ErrUnsupported
		}
		table = spec.Target.Name
	}
	if table != "" && !dynamoTable.MatchString(table) {
		return adapter.QueryStats{}, adapter.ErrInvalid
	}
	l, err := s.limits(ctx, limits.MaxRows, limits.MaxBytes)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	c, err := awsapi.NewResolved(s.source, l, "dynamodb", s.awsCredentials())
	if err != nil {
		return adapter.QueryStats{}, err
	}
	defer c.Close()
	if spec.Object == "tables" || spec.Object == "databases" || spec.Object == "catalogs" || spec.Object == "schemas" {
		if spec.Object == "schemas" && (table == "" || spec.Target.Name != "") {
			return adapter.QueryStats{}, adapter.ErrUnsupported
		}
		names := []string{}
		if table != "" {
			var result struct {
				Table *struct {
					Name string `json:"TableName"`
				} `json:"Table"`
			}
			_, err = c.Do(ctx, "DynamoDB_20120810.DescribeTable", map[string]string{"TableName": table}, &result)
			if err != nil {
				return adapter.QueryStats{}, err
			}
			if result.Table == nil || result.Table.Name != table {
				return adapter.QueryStats{}, adapter.ErrInvalid
			}
			names = append(names, table)
		} else {
			offset, _ := metadataOffset(spec)
			needed := offset + spec.Limit
			if needed > 10000 {
				return adapter.QueryStats{}, adapter.ErrLimit
			}
			next := ""
			bytes := int64(0)
			for page := 0; page < 100; page++ {
				var result struct {
					Names *[]string `json:"TableNames"`
					Last  string    `json:"LastEvaluatedTableName"`
				}
				body := map[string]any{"Limit": min(100, needed-len(names))}
				if next != "" {
					body["ExclusiveStartTableName"] = next
				}
				received, err := c.Do(ctx, "DynamoDB_20120810.ListTables", body, &result)
				bytes += received
				if err != nil {
					return adapter.QueryStats{}, err
				}
				if bytes > limits.MaxBytes || result.Names == nil || len(*result.Names) > 100 {
					return adapter.QueryStats{}, adapter.ErrLimit
				}
				for _, name := range *result.Names {
					if !dynamoTable.MatchString(name) || name <= next {
						return adapter.QueryStats{}, adapter.ErrInvalid
					}
					names = append(names, name)
					next = name
				}
				if result.Last == "" || len(names) >= needed {
					break
				}
				if result.Last != next || page == 99 {
					return adapter.QueryStats{}, adapter.ErrLimit
				}
			}
		}
		sort.Strings(names)
		rows := make([][]any, 0, len(names))
		for _, name := range names {
			switch spec.Object {
			case "catalogs", "databases":
				rows = append(rows, []any{name})
			case "schemas":
				rows = append(rows, []any{name, ""})
			case "tables":
				rows = append(rows, []any{name, "", name, "TABLE"})
			}
		}
		fields := []arrow.Field{textField("catalog")}
		if spec.Object == "schemas" || spec.Object == "tables" {
			fields = append(fields, textField("schema_name"))
		}
		if spec.Object == "tables" {
			fields = append(fields, textField("name"), textField("type"))
		}
		return s.emitMetadata(ctx, spec, limits, sink, fields, rows)
	}
	if spec.Object != "columns" || table == "" {
		return adapter.QueryStats{}, adapter.ErrUnsupported
	}
	var result struct {
		Table *struct {
			Name       string `json:"TableName"`
			Attributes []struct {
				Name string `json:"AttributeName"`
				Type string `json:"AttributeType"`
			} `json:"AttributeDefinitions"`
			Keys []struct {
				Name string `json:"AttributeName"`
				Type string `json:"KeyType"`
			} `json:"KeySchema"`
		} `json:"Table"`
	}
	_, err = c.Do(ctx, "DynamoDB_20120810.DescribeTable", map[string]string{"TableName": table}, &result)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	if result.Table == nil || result.Table.Name != table || len(result.Table.Attributes) > 1000 {
		return adapter.QueryStats{}, adapter.ErrInvalid
	}
	keys := map[string]string{}
	for _, key := range result.Table.Keys {
		keys[key.Name] = key.Type
	}
	sort.Slice(result.Table.Attributes, func(i, j int) bool { return result.Table.Attributes[i].Name < result.Table.Attributes[j].Name })
	rows := [][]any{}
	for _, attribute := range result.Table.Attributes {
		if attribute.Type != "S" && attribute.Type != "N" && attribute.Type != "B" || attribute.Name == "" || len(attribute.Name) > 1024 {
			return adapter.QueryStats{}, adapter.ErrInvalid
		}
		rows = append(rows, []any{"", table, attribute.Name, attribute.Type, keys[attribute.Name], "declared_key_attribute"})
	}
	return s.emitMetadata(ctx, spec, limits, sink, []arrow.Field{textField("schema_name"), textField("table_name"), textField("name"), textField("type"), textField("key_type"), textField("representation")}, rows)
}
