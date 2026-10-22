// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime/pprof"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	stallDumpInterval = 10 * time.Minute
	stallDumpKeep     = 5
	stallDumpPrefix   = "kelvo-stall-"
)

// stallDetector writes one goroutine dump (debug=2) when the observer reports
// reads in flight with no completion for After. Files are 0600 in a 0700
// directory, at most one per stallDumpInterval, newest stallDumpKeep kept.
type stallDetector struct {
	cfg   StallDumpConfig
	obs   *readObserver
	write func(io.Writer) error

	mu       sync.Mutex
	lastDump time.Time
	dumps    uint64
}

func newStallDetector(cfg StallDumpConfig, obs *readObserver) *stallDetector {
	return &stallDetector{cfg: cfg, obs: obs, write: func(w io.Writer) error { return pprof.Lookup("goroutine").WriteTo(w, 2) }}
}

func (s *stallDetector) count() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dumps
}

// check runs one detection pass and reports whether a dump was written.
func (s *stallDetector) check() bool {
	if !s.obs.stalled(s.cfg.After) {
		return false
	}
	now := s.obs.now()
	s.mu.Lock()
	if !s.lastDump.IsZero() && now.Sub(s.lastDump) < stallDumpInterval {
		s.mu.Unlock()
		return false
	}
	s.lastDump = now // a failed attempt also waits out the window
	s.mu.Unlock()
	if err := s.dump(now); err != nil {
		log.Printf("kelvo stall dump failed: %v", err)
		return false
	}
	s.mu.Lock()
	s.dumps++
	s.mu.Unlock()
	log.Printf("kelvo stall dump written: no read completed for %s with reads in flight", s.cfg.After)
	return true
}

func (s *stallDetector) dump(now time.Time) error {
	if err := os.MkdirAll(s.cfg.Dir, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(s.cfg.Dir, 0o700); err != nil {
		return err
	}
	name := filepath.Join(s.cfg.Dir, stallDumpPrefix+now.UTC().Format("20060102T150405.000000000Z")+".txt")
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	err = s.write(f)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(name)
		return err
	}
	return s.prune()
}

func (s *stallDetector) prune() error {
	entries, err := os.ReadDir(s.cfg.Dir)
	if err != nil {
		return err
	}
	var names []string
	for _, e := range entries {
		if e.Type().IsRegular() && strings.HasPrefix(e.Name(), stallDumpPrefix) {
			names = append(names, e.Name())
		}
	}
	slices.Sort(names) // timestamped names sort oldest first
	for len(names) > stallDumpKeep {
		_ = os.Remove(filepath.Join(s.cfg.Dir, names[0]))
		names = names[1:]
	}
	return nil
}

func (s *stallDetector) run(ctx context.Context) {
	interval := min(max(s.cfg.After/4, 100*time.Millisecond), 5*time.Second)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.check()
		}
	}
}
