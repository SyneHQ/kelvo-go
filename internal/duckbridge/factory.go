//go:build duckbridge && duckdb_arrow && cgo

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckbridge

/*
#cgo CXXFLAGS: -std=c++17 -DDUCKDB_STATIC_BUILD
#cgo LDFLAGS: -lstdc++
#include "shim.h"
#include <stdlib.h>
*/
import "C"

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"runtime"
	"runtime/cgo"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"
	"unsafe"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
	_ "github.com/duckdb/duckdb-go/v2" // Link the exact pinned core used by the shim.
)

// Factory owns native callbacks for one registered source. Its lifetime must
// cover every database/view referencing it: close the connection and database
// before Close. No Go pointer is stored in C; callbacks use cgo.Handle integers.
type Factory struct {
	ctx      context.Context
	cancel   context.CancelFunc
	schema   *arrow.Schema
	producer Producer
	mu       sync.Mutex
	err      error
	pointer  unsafe.Pointer
	handle   cgo.Handle
	once     sync.Once
}

var activeStreams atomic.Int64
var activePins atomic.Int64

func Available() bool { return C.GoString(C.kelvo_runtime_version()) == "v1.5.6" }

func New(ctx context.Context, schema *arrow.Schema, producer Producer) (*Factory, error) {
	if C.GoString(C.kelvo_runtime_version()) != "v1.5.6" {
		return nil, query.NewError("UNAVAILABLE", "Native bridge requires DuckDB v1.5.6")
	}
	if ctx == nil || schema == nil || schema.NumFields() == 0 || schema.NumFields() > 1024 || producer == nil {
		return nil, query.NewError("INVALID_ARGUMENT", "Native bridge requires a bounded schema and producer")
	}
	seen := make(map[string]bool)
	for _, field := range schema.Fields() {
		if !validName(field.Name) || seen[field.Name] || field.Type == nil {
			return nil, query.NewError("INVALID_ARGUMENT", "Native bridge schema has invalid or duplicate columns")
		}
		seen[field.Name] = true
	}
	child, cancel := context.WithCancel(ctx)
	factory := &Factory{ctx: child, cancel: cancel, schema: schema, producer: producer}
	factory.handle = cgo.NewHandle(factory)
	factory.pointer = C.kelvo_factory_create(C.uint64_t(factory.handle))
	if factory.pointer == nil {
		factory.handle.Delete()
		cancel()
		return nil, query.NewError("RESOURCE_EXHAUSTED", "Native bridge could not allocate its factory")
	}
	return factory, nil
}

func validName(name string) bool {
	return name != "" && len(name) <= 1024 && utf8.ValidString(name) && !strings.ContainsRune(name, 0)
}

// Register must run within sql.Conn.Raw. The opt-in, pinned driver patch exposes
// this lifetime-scoped method; no driver struct-layout assumptions are used.
func (f *Factory) Register(raw driver.Conn, schemaName, viewName string) error {
	if !validName(schemaName) || !validName(viewName) || f.pointer == nil {
		return query.NewError("INVALID_ARGUMENT", "Native bridge view name is invalid")
	}
	connection, ok := raw.(interface {
		WithNativeConnection(func(unsafe.Pointer) error) error
	})
	if !ok {
		return query.NewError("UNAVAILABLE", "Native bridge requires its pinned driver accessor")
	}
	schema, name := C.CString(schemaName), C.CString(viewName)
	defer C.free(unsafe.Pointer(schema))
	defer C.free(unsafe.Pointer(name))
	return connection.WithNativeConnection(func(pointer unsafe.Pointer) error {
		if err := f.ctx.Err(); err != nil {
			return err
		}
		if C.kelvo_factory_register(f.pointer, pointer, schema, name) != 0 {
			if err := f.Err(); err != nil {
				return err
			}
			return query.NewError("QUERY_FAILED", "Native bridge could not register its view")
		}
		return nil
	})
}

func (f *Factory) fail(err error) {
	if err == nil {
		return
	}
	f.mu.Lock()
	if f.err == nil {
		f.err = err
	}
	f.mu.Unlock()
	f.cancel()
}

func (f *Factory) Err() error { f.mu.Lock(); defer f.mu.Unlock(); return f.err }

func (f *Factory) Close() {
	if f == nil {
		return
	}
	f.once.Do(func() {
		f.cancel()
		C.kelvo_factory_destroy(f.pointer)
		f.pointer = nil
		f.handle.Delete()
	})
}

//export kelvo_go_factory_schema
func kelvo_go_factory_schema(handle C.uint64_t, out unsafe.Pointer) (result C.int) {
	f := cgo.Handle(handle).Value().(*Factory)
	defer func() {
		if recover() != nil {
			cdata.ReleaseCArrowSchema((*cdata.CArrowSchema)(out))
			f.fail(callbackPanic())
			result = 1
		}
	}()
	if err := f.ctx.Err(); err != nil {
		f.fail(err)
		return 1
	}
	C.kelvo_schema_zero(out)
	cdata.ExportArrowSchema(f.schema, (*cdata.CArrowSchema)(out))
	return 0
}

type streamState struct {
	factory *Factory
	reader  array.RecordReader
	schema  *arrow.Schema
	mu      sync.Mutex
}

//export kelvo_go_produce
func kelvo_go_produce(handle C.uint64_t, input *C.char, length C.size_t, out unsafe.Pointer) (result C.int) {
	f := cgo.Handle(handle).Value().(*Factory)
	var reader array.RecordReader
	transferred := false
	defer func() {
		if recover() != nil {
			if reader != nil && !transferred {
				_ = releaseReader(reader)
			}
			f.fail(callbackPanic())
			result = 1
		}
	}()
	if err := f.ctx.Err(); err != nil {
		f.fail(err)
		return 1
	}
	if length == 0 || length > 1<<20 {
		f.fail(query.NewError("UNSUPPORTED", "Native bridge scan plan exceeds its bound"))
		return 1
	}
	var plan ScanPlan
	decoder := json.NewDecoder(bytes.NewReader(C.GoBytes(unsafe.Pointer(input), C.int(length))))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&plan); err != nil {
		f.fail(query.NewError("UNSUPPORTED", "Native bridge scan plan is invalid"))
		return 1
	}
	var err error
	reader, err = f.producer(f.ctx, plan)
	if err != nil {
		if reader != nil {
			_ = releaseReader(reader)
		}
		f.fail(err)
		return 1
	}
	if reader == nil || reader.Schema() == nil {
		if reader != nil {
			_ = releaseReader(reader)
		}
		f.fail(query.NewError("QUERY_FAILED", "Native source returned no Arrow schema"))
		return 1
	}
	if err := validateProjection(f.schema, reader.Schema(), plan.Columns); err != nil {
		_ = releaseReader(reader)
		f.fail(err)
		return 1
	}
	state := &streamState{factory: f, reader: reader, schema: reader.Schema()}
	streamHandle := cgo.NewHandle(state)
	activeStreams.Add(1)
	C.kelvo_stream_init(out, C.uint64_t(streamHandle))
	transferred = true
	return 0
}

func validateProjection(full, projected *arrow.Schema, columns []string) error {
	invalid := query.NewError("QUERY_FAILED", "Native source projected schema does not match the scan plan")
	if len(columns) == 0 {
		if projected.NumFields() != 1 {
			return invalid
		}
		return nil
	}
	if projected.NumFields() != len(columns) {
		return invalid
	}
	for i, name := range columns {
		indexes := full.FieldIndices(name)
		field := projected.Field(i)
		if len(indexes) != 1 || field.Name != name || !arrow.TypeEqual(full.Field(indexes[0]).Type, field.Type) {
			return invalid
		}
	}
	return nil
}

//export kelvo_go_stream_schema
func kelvo_go_stream_schema(handle C.uint64_t, out unsafe.Pointer) (result C.int) {
	s := cgo.Handle(handle).Value().(*streamState)
	defer func() {
		if recover() != nil {
			cdata.ReleaseCArrowSchema((*cdata.CArrowSchema)(out))
			s.factory.fail(callbackPanic())
			result = 1
		}
	}()
	s.mu.Lock()
	defer s.mu.Unlock()
	C.kelvo_schema_zero(out)
	cdata.ExportArrowSchema(s.schema, (*cdata.CArrowSchema)(out))
	return 0
}

//export kelvo_go_next
func kelvo_go_next(handle C.uint64_t, out unsafe.Pointer) (result C.int) {
	s := cgo.Handle(handle).Value().(*streamState)
	var pinner *runtime.Pinner
	var pinHandle cgo.Handle
	counted, exported, wrapped := false, false, false
	defer func() {
		if recover() == nil {
			return
		}
		if wrapped {
			cdata.ReleaseCArrowArray((*cdata.CArrowArray)(out))
		} else {
			if exported {
				cdata.ReleaseCArrowArray((*cdata.CArrowArray)(out))
			}
			if counted {
				pinHandle.Delete()
				activePins.Add(-1)
			}
			if pinner != nil {
				pinner.Unpin()
			}
		}
		s.factory.fail(callbackPanic())
		result = 1
	}()
	s.mu.Lock()
	defer s.mu.Unlock()
	C.kelvo_array_zero(out)
	if err := s.factory.ctx.Err(); err != nil {
		s.factory.fail(err)
		return 1
	}
	if !s.reader.Next() {
		if err := s.reader.Err(); err != nil {
			s.factory.fail(err)
			return 1
		}
		return 0
	}
	record := s.reader.RecordBatch()
	if record == nil || record.NumRows() < 0 || !record.Schema().Equal(s.schema) {
		s.factory.fail(query.NewError("QUERY_FAILED", "Native source changed its Arrow batch schema"))
		return 1
	}
	// C Data exports reference the source buffers after this callback returns.
	// Retention alone is insufficient for cgo: pin every underlying Go buffer
	// until the exported ArrowArray's release callback runs, including nested
	// and dictionary buffers. The standard exporter owns its own data Retains.
	pinner = new(runtime.Pinner)
	for _, column := range record.Columns() {
		pinData(pinner, column.Data())
	}
	exported = true
	cdata.ExportArrowRecordBatch(record, (*cdata.CArrowArray)(out), nil)
	pinHandle = cgo.NewHandle(pinner)
	activePins.Add(1)
	counted = true
	if C.kelvo_array_pin_release(out, C.uint64_t(pinHandle)) != 0 {
		cdata.ReleaseCArrowArray((*cdata.CArrowArray)(out))
		kelvo_go_unpin(C.uint64_t(pinHandle))
		exported, counted, pinner = false, false, nil
		s.factory.fail(query.NewError("RESOURCE_EXHAUSTED", "Native bridge could not retain its Arrow batch"))
		return 1
	}
	wrapped = true
	return 0
}

func pinData(pinner *runtime.Pinner, data arrow.ArrayData) {
	// Arrow Data.Dictionary returns a typed nil *array.Data for primitives.
	if data == nil || (reflect.ValueOf(data).Kind() == reflect.Pointer && reflect.ValueOf(data).IsNil()) {
		return
	}
	for _, buffer := range data.Buffers() {
		if buffer != nil && buffer.Len() > 0 {
			pinner.Pin(&buffer.Bytes()[0])
		}
	}
	for _, child := range data.Children() {
		if child != nil {
			pinData(pinner, child)
		}
	}
	if dictionary := data.Dictionary(); dictionary != nil {
		pinData(pinner, dictionary)
	}
}

//export kelvo_go_stream_release
func kelvo_go_stream_release(handle C.uint64_t) {
	h := cgo.Handle(handle)
	s := h.Value().(*streamState)
	defer h.Delete()
	defer activeStreams.Add(-1)
	defer func() {
		if recover() != nil {
			s.factory.fail(callbackPanic())
		}
	}()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := releaseReader(s.reader); err != nil {
		s.factory.fail(err)
	}
	s.reader = nil
}

func callbackPanic() error { return query.NewError("QUERY_FAILED", "Native source callback failed") }

func releaseReader(reader array.RecordReader) (err error) {
	defer func() {
		if recover() != nil {
			err = callbackPanic()
		}
	}()
	reader.Release()
	return nil
}

//export kelvo_go_unpin
func kelvo_go_unpin(handle C.uint64_t) {
	h := cgo.Handle(handle)
	h.Value().(*runtime.Pinner).Unpin()
	h.Delete()
	activePins.Add(-1)
}

//export kelvo_go_fail
func kelvo_go_fail(handle C.uint64_t, code C.int) {
	f := cgo.Handle(handle).Value().(*Factory)
	if code == 1 {
		f.fail(query.NewError("UNSUPPORTED", "Native source pushed filter is unsupported"))
		return
	}
	f.fail(errors.New("native source bridge failed"))
}
