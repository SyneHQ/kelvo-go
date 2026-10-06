package exasol

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
)

func TestAutocommitStatementAcknowledgementAndDisconnectFailure(t *testing.T) {
	var enabled atomic.Bool
	var writes atomic.Int32
	e, f, _ := setup(t, func(c *websocket.Conn, in map[string]any) bool {
		switch in["command"] {
		case "setAttributes":
			if in["attributes"].(map[string]any)["autocommit"] != true {
				t.Error("autocommit not requested")
			}
			enabled.Store(true)
			ok(c)
			return true
		case "getAttributes":
			if enabled.Load() {
				_ = c.WriteMessage(websocket.TextMessage, []byte(`{"status":"ok","attributes":{"autocommit":true,"timestampUtcEnabled":true}}`))
				return true
			}
		case "execute":
			writes.Add(1)
			if in["sqlText"] != "DELETE FROM events WHERE id=1" || in["attributes"].(map[string]any)["autocommit"] != true || !enabled.Load() {
				t.Error("unexpected statement or commit mode")
			}
			reply(c, `{"numResults":1,"results":[{"resultType":"rowCount","rowCount":9007199254740993}]}`)
			return true
		case "disconnect":
			c.Close()
			return true
		}
		return false
	})
	count, err := e.ApplyStatement(context.Background(), "DELETE FROM events WHERE id=1")
	if err != nil || count == nil || *count != 9007199254740993 || writes.Load() != 1 || strings.Contains(f.snapshot(), "ROLLBACK") {
		t.Fatal(count, err, f.snapshot())
	}
}

func TestAutocommitMustBeConfirmedBeforeDispatch(t *testing.T) {
	var writes atomic.Int32
	e, _, _ := setup(t, func(c *websocket.Conn, in map[string]any) bool {
		if in["command"] == "setAttributes" {
			ok(c) // Server ignores requested autocommit; getAttributes stays false.
			return true
		}
		if in["command"] == "execute" {
			writes.Add(1)
		}
		return false
	})
	if count, err := e.ApplyStatement(context.Background(), "DELETE FROM events"); err == nil || count != nil || writes.Load() != 0 {
		t.Fatal(count, err, writes.Load())
	}
}

func TestAutocommitInvalidOrLostAcknowledgementDoesNotReplay(t *testing.T) {
	for _, response := range []string{"", `{"numResults":1,"results":[{"resultType":"rowCount"}]}`, `{"numResults":1,"results":[{"resultType":"rowCount","rowCount":-1}]}`, `{"numResults":2,"results":[]}`} {
		t.Run(response, func(t *testing.T) {
			var enabled bool
			var writes atomic.Int32
			e, f, _ := setup(t, func(c *websocket.Conn, in map[string]any) bool {
				switch in["command"] {
				case "setAttributes":
					enabled = true
					ok(c)
					return true
				case "getAttributes":
					if enabled {
						_ = c.WriteMessage(websocket.TextMessage, []byte(`{"status":"ok","attributes":{"autocommit":true,"timestampUtcEnabled":true}}`))
						return true
					}
				case "execute":
					writes.Add(1)
					if response == "" {
						c.Close()
					} else {
						reply(c, response)
					}
					return true
				}
				return false
			})
			if count, err := e.ApplyStatement(context.Background(), "DELETE FROM events"); err == nil || count != nil || writes.Load() != 1 || f.connects.Load() != 1 {
				t.Fatal(count, err, writes.Load(), f.connects.Load())
			}
		})
	}
}
