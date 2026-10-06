package oracle

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/SYNEHQ/kelvo-go/adapter"
	"strings"
)

const Limit = 500

var ErrUnsupported = adapter.ErrUnsupported

type Relation struct {
	ID           string `json:"id"`
	Kind         string `json:"kind"`
	Schema       string `json:"schema"`
	Name         string `json:"name"`
	Role         string `json:"role"`
	ObjectType   string `json:"object_type,omitempty"`
	DatabaseLink string `json:"database_link,omitempty"`
	Unavailable  bool   `json:"unavailable,omitempty"`
}
type Result struct {
	Objects            []map[string]any `json:"objects"`
	Relations          []Relation       `json:"relations"`
	Kinds              []string         `json:"kinds"`
	Truncated          bool             `json:"truncated"`
	RelationsTruncated bool             `json:"relations_truncated,omitempty"`
}

func Kinds(engine string) []string {
	if strings.ToLower(engine) == "oracle" {
		return []string{"function", "procedure", "package", "package_body", "trigger", "view", "materialized_view"}
	}
	return nil
}
func read(ctx context.Context, tx *sql.Tx, query string, args ...any) ([]map[string]any, error) {
	return readLimit(ctx, tx, query, Limit+1, args...)
}

func readLimit(ctx context.Context, tx *sql.Tx, query string, maxRows int, args ...any) ([]map[string]any, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	result := []map[string]any{}
	size := 0
	for rows.Next() {
		if len(result) >= maxRows {
			return nil, errors.New("catalog row limit exceeded")
		}
		values, targets := make([]any, len(columns)), make([]any, len(columns))
		for i := range values {
			targets[i] = &values[i]
		}
		if err := rows.Scan(targets...); err != nil {
			return nil, err
		}
		row := map[string]any{}
		for i, name := range columns {
			value := values[i]
			if b, ok := value.([]byte); ok {
				value = string(b)
			}
			row[name] = value
		}
		encoded, err := json.Marshal(row)
		if err != nil {
			return nil, err
		}
		size += len(encoded)
		if size > 4<<20 {
			return nil, errors.New("catalog response exceeds size limit")
		}
		result = append(result, row)
	}
	return result, rows.Err()
}
