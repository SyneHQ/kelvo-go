package cdc

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestDecodeTextExactTypes(t *testing.T) {
	ts := time.Date(2024, 2, 29, 23, 59, 59, 123456000, time.UTC)
	for _, c := range []struct {
		oid  uint32
		in   string
		want any
	}{
		{16, "t", true}, {17, `\x00ff`, []byte{0, 255}},
		{20, "-9223372036854775808", int64(math.MinInt64)}, {21, "32767", int16(32767)}, {23, "-2147483648", int32(math.MinInt32)},
		{701, "0.1", 0.1}, {700, "1.5", float32(1.5)},
		{1700, "12345678901234567890.000000000001", Numeric("12345678901234567890.000000000001")},
		{1184, "2024-02-29 23:59:59.123456+00", ts}, {1114, "2024-02-29 23:59:59.123456", ts},
		{1082, "2024-02-29", time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)}, {1042, "a  ", "a  "},
	} {
		got, err := decodeText(c.oid, c.in)
		if err != nil || !reflect.DeepEqual(got, c.want) {
			t.Errorf("decode(%d,%q) = %#v, %v; want %#v", c.oid, c.in, got, err, c.want)
		}
	}
	for _, c := range []struct {
		oid uint32
		in  string
	}{{21, "32768"}, {1184, "infinity"}, {1082, "0001-01-01 BC"}, {600, "(1,2)"}} {
		if _, err := decodeText(c.oid, c.in); !errors.Is(err, ErrUnsupported) {
			t.Errorf("decode(%d,%q) err = %v, want ErrUnsupported", c.oid, c.in, err)
		}
	}
}

func TestFileStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	f := FileStore{Path: filepath.Join(t.TempDir(), "cp.json")}
	if _, ok, err := f.Load(ctx); ok || err != nil {
		t.Fatalf("empty load: %v %v", ok, err)
	}
	cp := Checkpoint{Slot: "s", LSN: 42, Schemas: map[uint32]Schema{7: {Namespace: "public", Name: "t", Version: 2, Columns: []Column{{Name: "id", TypeOID: 20, TypeMod: -1, Key: true}}}}}
	for range 2 {
		if err := f.Save(ctx, cp); err != nil {
			t.Fatal(err)
		}
	}
	got, ok, err := f.Load(ctx)
	if !ok || err != nil || !reflect.DeepEqual(got, cp) {
		t.Fatalf("load = %#v %v %v", got, ok, err)
	}
	if m, _ := filepath.Glob(filepath.Join(filepath.Dir(f.Path), ".checkpoint-*")); len(m) != 0 {
		t.Fatalf("temp files left: %v", m)
	}
}

func TestSlotErrorBootstrapRequired(t *testing.T) {
	for _, code := range []string{"55000", "42704", "58P01"} {
		if err := slotError(&pgconn.PgError{Code: code}); !errors.Is(err, ErrBootstrapRequired) {
			t.Errorf("%s: %v", code, err)
		}
	}
	if err := slotError(&pgconn.PgError{Code: "08006"}); errors.Is(err, ErrBootstrapRequired) {
		t.Error("connection failure mapped to bootstrap required")
	}
}

func TestEvolve(t *testing.T) {
	base := Schema{Namespace: "public", Name: "t", Version: 1, Columns: []Column{{Name: "id", TypeOID: 20, TypeMod: -1, Key: true}}}
	schemas := map[uint32]Schema{1: base}
	rel := func(cols ...*pglogrepl.RelationMessageColumn) *pglogrepl.RelationMessage {
		return &pglogrepl.RelationMessage{RelationID: 1, Namespace: "public", RelationName: "t", ReplicaIdentity: 'd', Columns: cols}
	}
	id := &pglogrepl.RelationMessageColumn{Flags: 1, Name: "id", DataType: 20, TypeModifier: -1}
	if _, changed, err := evolve(schemas, rel(id)); changed || err != nil {
		t.Fatalf("same: %v %v", changed, err)
	}
	s, changed, err := evolve(schemas, rel(id, &pglogrepl.RelationMessageColumn{Name: "v", DataType: 25, TypeModifier: -1}))
	if !changed || err != nil || s.Version != 2 || len(s.Columns) != 2 {
		t.Fatalf("add: %+v %v %v", s, changed, err)
	}
	for name, m := range map[string]*pglogrepl.RelationMessage{
		"type":    rel(&pglogrepl.RelationMessageColumn{Flags: 1, Name: "id", DataType: 23, TypeModifier: -1}),
		"dropped": rel(),
		"badtype": rel(id, &pglogrepl.RelationMessageColumn{Name: "p", DataType: 600, TypeModifier: -1}),
	} {
		if _, _, err := evolve(schemas, m); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
