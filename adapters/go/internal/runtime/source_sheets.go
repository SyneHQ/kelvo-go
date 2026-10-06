package runtime

import (
	"context"
	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/sheets"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"time"
)

func openSheetsSource(ctx context.Context, input adapter.ProcessRequest) (adapter.Session, error) {
	if input.Validate() != nil || adapter.ValidateSheetsProcessSource(input.Source) != nil {
		return nil, adapter.ErrInvalid
	}
	limits := query.Limits{MaxRows: input.Limits.MaxRows, MaxBytes: input.Limits.MaxBytes, Timeout: time.Duration(input.Limits.TimeoutMS) * time.Millisecond, MemoryMB: input.Limits.MemoryMB, Threads: input.Limits.Threads, MaxTempMB: input.Limits.MaxTempMB}
	return sheets.Open(ctx, input.Source.Options["spreadsheet_id"], input.Source.Token, limits)
}
