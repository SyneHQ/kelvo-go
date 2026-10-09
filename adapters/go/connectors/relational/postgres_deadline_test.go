package relational

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestPostgresReadInstallsRemainingServerDeadline(t *testing.T) {
	s, state := newReadSession(t, "postgresql")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sink := &readSink{}
	defer sink.Close()
	if _, err := s.Query(ctx, readRequest(), sink); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.executed) != 1 || state.executed[0] != postgresReadDeadlineSQL || len(state.execArgs[0]) != 1 {
		t.Fatal("transaction-local server timeout not installed")
	}
	value, ok := state.execArgs[0][0].Value.(string)
	if !ok {
		t.Fatal("timeout is not a bound parameter")
	}
	milliseconds, err := strconv.ParseInt(strings.TrimSuffix(value, "ms"), 10, 64)
	if err != nil || milliseconds < 1 || milliseconds > 1000 {
		t.Fatal("timeout exceeds the remaining admitted budget")
	}
	if !state.readOnly.Load() || state.queries.Load() != 1 || state.rollbacks.Load() != 1 || state.closes.Load() != 1 {
		t.Fatal("read isolation or cleanup changed")
	}
}
func TestPostgresReadDeadlineSetupFailurePreventsQuery(t *testing.T) {
	s, state := newReadSession(t, "postgresql")
	state.execErr = errors.New("fixture denies timeout configuration")
	if _, err := s.Query(context.Background(), readRequest(), &readSink{}); !errors.Is(err, state.execErr) {
		t.Fatal("timeout configuration failure was lost")
	}
	if state.queries.Load() != 0 || state.rollbacks.Load() != 1 || state.closes.Load() != 1 {
		t.Fatal("query ran without a server timeout or leaked its session")
	}
}
func TestPostgresTimeoutNeverDisablesServerTimer(t *testing.T) {
	for _, remaining := range []time.Duration{-time.Second, 0, time.Nanosecond, time.Millisecond - time.Nanosecond} {
		if value, err := postgresTimeoutValue(remaining); err == nil || value != "" {
			t.Fatal("nonpositive or sub-millisecond budget accepted")
		}
	}
	for _, tc := range []struct {
		remaining time.Duration
		want      string
	}{{time.Millisecond, "1ms"}, {1500 * time.Microsecond, "1ms"}, {15 * time.Second, "15000ms"}} {
		value, err := postgresTimeoutValue(tc.remaining)
		if err != nil || value != tc.want {
			t.Fatal("timeout rounded beyond the remaining budget")
		}
	}
}
