// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/SYNEHQ/kelvo-go/internal/transportbroker/diagnostic"
)

const privateDiagnosticQueueSize = 16
const privateDiagnosticSummaryBytes = 16 << 10

type privateDiagnosticSummary struct {
	OperationID      string `json:"operation_id"`
	CleanupConfirmed bool   `json:"cleanup_confirmed"`
	diagnostic.Snapshot
}

// Only ledger-generated operation IDs can enter operator diagnostics.
func validDiagnosticOperationID(id string) bool {
	if len(id) != 37 || id[4] != '-' {
		return false
	}
	for i, c := range id {
		if i == 4 {
			continue
		}
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func privateOpenDiagnosticContext(ctx context.Context, enabled bool, operationID string) (context.Context, *diagnostic.Recorder) {
	if !enabled || !validDiagnosticOperationID(operationID) {
		return ctx, nil
	}
	recorder := diagnostic.New()
	return diagnostic.WithRecorder(ctx, recorder), recorder
}

// Preserve the runtime lifetime. Carry only the recorder from this operation.
func privateOpenDiagnosticLifetime(life, operation context.Context) context.Context {
	if recorder := diagnostic.FromContext(operation); recorder != nil {
		return diagnostic.WithRecorder(life, recorder)
	}
	return life
}

func privateOpenDiagnosticJSON(operationID string, recorder *diagnostic.Recorder, cleaned bool) []byte {
	if recorder == nil || !validDiagnosticOperationID(operationID) {
		return nil
	}
	snapshot := recorder.Snapshot()
	if len(snapshot.Events) == 0 {
		return nil
	}
	body, err := json.Marshal(privateDiagnosticSummary{OperationID: operationID, CleanupConfirmed: cleaned, Snapshot: snapshot})
	if err != nil || len(body) > privateDiagnosticSummaryBytes {
		return nil
	}
	return body
}

// One process-local writer owns the potentially blocking log sink. Enqueue
// never waits for that sink. A full queue drops the diagnostic, not the request.
type privateDiagnosticWriter struct {
	once  sync.Once
	queue chan []byte
	write func([]byte)
}

func (w *privateDiagnosticWriter) enqueue(body []byte) bool {
	if w == nil || len(body) == 0 || len(body) > privateDiagnosticSummaryBytes {
		return false
	}
	w.once.Do(func() {
		w.queue = make(chan []byte, privateDiagnosticQueueSize)
		go func() {
			for next := range w.queue {
				w.write(next)
			}
		}()
	})
	select {
	case w.queue <- body:
		return true
	default:
		return false
	}
}

var privateOpenDiagnosticWriter = privateDiagnosticWriter{write: func(body []byte) {
	log.Printf("kelvo_private_open_diagnostic %s", body)
}}

func emitPrivateOpenDiagnostic(operationID string, recorder *diagnostic.Recorder, cleaned bool) {
	privateOpenDiagnosticWriter.enqueue(privateOpenDiagnosticJSON(operationID, recorder, cleaned))
}
