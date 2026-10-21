// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestResultCacheRuleMatching(t *testing.T) {
	c := newResultCache(10*time.Second, 16, 1<<20)
	c.rules = []CacheRule{
		{Tenant: "a", Connection: "c1", TTL: time.Minute},
		{Team: "t", TTL: 0},
		{Tenant: "a", TTL: time.Hour},
	}
	for _, tc := range []struct {
		tenant, team, conn string
		want               time.Duration
	}{
		{"a", "x", "c1", time.Minute},      // first rule
		{"a", "t", "c1", time.Minute},      // first match wins over the later team rule
		{"b", "t", "c2", 0},                // empty tenant and connection match any
		{"a", "x", "c2", time.Hour},        // falls past the first two
		{"b", "x", "c1", 10 * time.Second}, // no match takes the default
	} {
		if got := c.ttlFor(tc.tenant, tc.team, tc.conn); got != tc.want {
			t.Errorf("%+v: ttl %s, want %s", tc, got, tc.want)
		}
	}
}

func TestResultCacheRuleTTLOverridesDefault(t *testing.T) {
	c := newResultCache(40*time.Millisecond, 16, 1<<20)
	read := &fakeRead{data: "result"}
	_, _ = c.doTTL(context.Background(), "long", "t", time.Minute, false, nil, read.execute, noReplay)
	_, _ = c.doTTL(context.Background(), "none", "t", 0, false, nil, read.execute, noReplay)
	time.Sleep(80 * time.Millisecond)
	_, _ = c.doTTL(context.Background(), "long", "t", time.Minute, false, nil, read.execute, noReplay)
	if read.runs.Load() != 2 {
		t.Fatalf("long-TTL entry did not outlive the default: %d runs", read.runs.Load())
	}
	_, _ = c.doTTL(context.Background(), "none", "t", 0, false, nil, read.execute, noReplay)
	if read.runs.Load() != 3 {
		t.Fatalf("ttl 0 kept an entry: %d runs", read.runs.Load())
	}
	// ttl 0 still collapses concurrent identical reads.
	slow := &fakeRead{data: "result", delay: 30 * time.Millisecond}
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = c.doTTL(context.Background(), "burst", "t", 0, false, nil, slow.execute, noReplay)
		}()
	}
	wg.Wait()
	if slow.runs.Load() != 1 || len(c.entries) != 1 {
		t.Fatalf("ttl 0 burst ran %d times, %d entries kept", slow.runs.Load(), len(c.entries))
	}
}

func TestWarmPoolConfigCacheRulesValidation(t *testing.T) {
	base := DefaultWarmPoolConfig()
	base.Enabled = true
	for name, mutate := range map[string]func(*WarmPoolConfig){
		"negative ttl": func(c *WarmPoolConfig) { c.CacheRules = []CacheRule{{TTL: -time.Second}} },
		"ttl over 1h":  func(c *WarmPoolConfig) { c.CacheRules = []CacheRule{{TTL: 2 * time.Hour}} },
		"65 rules":     func(c *WarmPoolConfig) { c.CacheRules = make([]CacheRule, 65) },
		"cache off":    func(c *WarmPoolConfig) { c.CacheDisabled, c.CacheRules = true, []CacheRule{{TTL: time.Minute}} },
	} {
		c := base
		mutate(&c)
		if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "cache_rules") {
			t.Errorf("%s: %v", name, err)
		}
	}
	c := base
	c.CacheRules = []CacheRule{{Tenant: "a", TTL: time.Hour}, {TTL: 0}}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}
