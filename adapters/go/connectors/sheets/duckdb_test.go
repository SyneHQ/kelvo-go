//go:build duckdb_arrow

package sheets

import (
	"context"
	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/apache/arrow-go/v18/arrow"
	"strings"
	"testing"
)

type capture struct{ rows [][]any }

func (*capture) Schema(*arrow.Schema) error { return nil }
func (s *capture) Write(record arrow.RecordBatch) error {
	for i := 0; i < int(record.NumRows()); i++ {
		row := make([]any, record.NumCols())
		for j, col := range record.Columns() {
			row[j] = col.GetOneForMarshal(i)
			if value, ok := row[j].(string); ok {
				row[j] = strings.Clone(value)
			}
		}
		s.rows = append(s.rows, row)
	}
	return nil
}
func TestRealSheetSnapshotCTEAndExactDecimal(t *testing.T) {
	s := testSession(t, fixtureHandler(t))
	sink := &capture{}
	q := adapter.Query{Statement: `WITH selected AS (SELECT * FROM "Daily trips") SELECT id,CAST(CAST(fare AS DECIMAL(30,18)) AS VARCHAR) AS fare FROM selected WHERE fare IS NOT NULL`, MaxRows: 100, MaxBytes: 1 << 20, BatchRows: 2}
	stats, err := s.Query(context.Background(), q, sink)
	if err != nil || stats.Rows != 1 || len(sink.rows) != 1 {
		t.Fatal(stats, sink.rows, err)
	}
	if sink.rows[0][0] != "9007199254740993" || sink.rows[0][1] != "10.000000000000000001" {
		t.Fatal(sink.rows)
	}
}
