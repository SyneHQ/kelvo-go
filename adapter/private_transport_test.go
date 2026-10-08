package adapter

import (
	"encoding/json"
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
