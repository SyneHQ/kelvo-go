package adapter

import (
	"encoding/json"
	"github.com/SYNEHQ/kelvo-go/operations"
	"testing"
	"time"
)

func TestPrivateTransportDescriptorContainsOnlyFixedChannel(t *testing.T) {
	now := time.Now()
	r := processFixture(t, now)
	r.PrivateTransport = &PrivateTransport{ControlFD: 7}
	if err := r.ValidateAt(now); err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeProcessRequest(r)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || string(fields["private_transport"]) != `{"control_fd":7}` {
		t.Fatal("private descriptor contains parent authority")
	}
	for _, fd := range []int{-1, 0, 3, 5, 6, 8, 1024} {
		copy := r
		copy.PrivateTransport = &PrivateTransport{ControlFD: fd}
		if copy.ValidateAt(now) == nil {
			t.Fatal("arbitrary descriptor accepted")
		}
	}
	copy := r
	copy.Source.Engine = "oracle"
	if copy.ValidateAt(now) == nil {
		t.Fatal("unsupported private engine accepted")
	}
	fields["private_transport"] = json.RawMessage(`{"control_fd":7,"grant":"forbidden"}`)
	raw, _ = json.Marshal(fields)
	if _, err := ParseProcessRequest(raw); err == nil {
		t.Fatal("parent authority accepted in descriptor")
	}
}

func TestPostgresCleanupDescriptorRejectsOtherEnginesAndWrites(t *testing.T) {
	for _, tc := range []struct {
		engine string
		kind   operations.Kind
		want   bool
	}{
		{"postgresql", operations.QueryRead, true},
		{"mysql", operations.QueryRead, false},
		{"postgresql", operations.StatementExecute, false},
	} {
		r := ProcessRequest{Source: ConnectionSpec{Engine: tc.engine}, Request: operations.Request{Kind: tc.kind}, PrivateTransport: &PrivateTransport{ControlFD: 7, PostgresCleanup: true}}
		if got := r.validatePrivateTransport() == nil; got != tc.want {
			t.Fatalf("engine %s kind %s valid %v", tc.engine, tc.kind, got)
		}
	}
}

func TestPrivateTransportDataOpenTimeoutBounds(t *testing.T) {
	for _, value := range []int64{-1, MaxPrivateDataOpenTimeoutMS + 1, 1 << 62} {
		request := ProcessRequest{Source: ConnectionSpec{Engine: "postgresql"}, PrivateTransport: &PrivateTransport{ControlFD: 7, DataOpenTimeoutMS: value}}
		if request.validatePrivateTransport() == nil {
			t.Fatalf("invalid data open milliseconds accepted: %d", value)
		}
	}
	for _, value := range []int64{0, 1, MaxPrivateDataOpenTimeoutMS} {
		request := ProcessRequest{Source: ConnectionSpec{Engine: "postgresql"}, PrivateTransport: &PrivateTransport{ControlFD: 7, DataOpenTimeoutMS: value}}
		if err := request.validatePrivateTransport(); err != nil {
			t.Fatal("valid data open milliseconds rejected", err)
		}
	}
}
