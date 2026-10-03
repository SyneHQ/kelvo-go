//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package exports

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"go.yaml.in/yaml/v3"
	"golang.org/x/sys/unix"
)

type Store struct {
	mu                     sync.RWMutex
	config                 Config
	root                   *os.File
	references             atomic.Int64
	fault                  func(string) error
	encoderAllocationLimit int64
}

func (s *Store) point(stage string) error {
	if s.fault != nil {
		return s.fault(stage)
	}
	return nil
}
func (s *Store) atomicWrite(dir *os.File, name string, raw []byte, immutable, noReplace bool) (bool, error) {
	return atomicWriteWithHook(dir, name, raw, immutable, noReplace, s.fault)
}

func Open(config Config) (*Store, error) {
	if !tenantPattern.MatchString(config.Tenant) || config.MaxEntries < 1 || config.MaxEntries > maximumEntries || config.MaxStoredBytes < metadataReservation+stateLimit || config.MaxStoredBytes > 1<<50 || config.MaxTTL < time.Second || config.MaxTTL > 30*24*time.Hour {
		return nil, ErrInvalid
	}
	root, err := openRoot(config.Directory)
	if err != nil {
		return nil, err
	}
	s := &Store{config: config, root: root}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lock, err := lockRoot(ctx, root)
	if err != nil {
		root.Close()
		return nil, err
	}
	defer lock.Close()
	var stored struct {
		Version int    `yaml:"version"`
		Config  Config `yaml:"config"`
	}
	raw, err := readFile(root, "store.yml", stateLimit, true)
	if errors.Is(err, os.ErrNotExist) {
		names, e := directoryNames(root, 2)
		if e != nil {
			root.Close()
			return nil, e
		}
		for _, name := range names {
			if name != ".lock" {
				root.Close()
				return nil, ErrCorrupt
			}
		}
		stored.Version, stored.Config = 1, config
		stored.Config.Directory = ""
		raw, err = yaml.Marshal(stored)
		if err == nil {
			_, err = atomicWrite(root, "store.yml", raw, true, true)
		}
	} else if err == nil {
		err = strictYAML(raw, &stored)
		expected := config
		expected.Directory = ""
		if err == nil && (stored.Version != 1 || stored.Config != expected) {
			err = ErrInvalid
		}
	}
	if err == nil {
		_, _, err = s.scan()
	}
	if err != nil {
		root.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) guard() (func(), error) {
	s.mu.RLock()
	if s.root == nil {
		s.mu.RUnlock()
		return nil, ErrClosed
	}
	return s.mu.RUnlock, nil
}
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.root == nil {
		return nil
	}
	if s.references.Load() != 0 {
		return ErrBusy
	}
	err := s.root.Close()
	s.root = nil
	return err
}

func (s *Store) loadState(dir *os.File, id string) (state, error) {
	var st state
	raw, err := readFile(dir, "state.yml", stateLimit, false)
	if err != nil {
		return st, err
	}
	if strictYAML(raw, &st) != nil || !st.valid(s.config, id) {
		return st, ErrCorrupt
	}
	// A complete, unrenamed state slot means its operation never published.
	// Discard only a valid transition of this exact existing reservation.
	nextRaw, nextErr := readFile(dir, ".state.next.yml", stateLimit, false)
	if nextErr == nil {
		var next state
		if strictYAML(nextRaw, &next) != nil || !next.valid(s.config, id) {
			return st, ErrCorrupt
		}
		same := next
		same.Status = st.Status
		binding := st.Status == "reserved" && next.Status == "active"
		transition := binding || (st.Status == "active" && next.Status == "ready") ||
			((st.Status == "reserved" || st.Status == "active" || st.Status == "ready") && next.Status == "cancelled")
		if binding {
			same.SchemaSHA256 = st.SchemaSHA256
		} else if st.Status == "active" && next.Status == "ready" {
			same.ManifestSHA256 = st.ManifestSHA256
		}
		if same != st || !transition {
			return st, ErrCorrupt
		}
		if binding {
			// A bind slot is ours only with the exact immutable bounded schema
			// that was synced before it. Never adopt an interrupted binding.
			schema, e := readFile(dir, "schema.arrow", schemaLimit, true)
			if e != nil || checksum(schema) != next.SchemaSHA256 {
				return st, ErrCorrupt
			}
			if _, e = validatePart(context.Background(), bytes.NewReader(schema), int64(len(schema)), st.Limits, next.SchemaSHA256); e != nil {
				return st, ErrCorrupt
			}
		}
		if err = unix.Unlinkat(int(dir.Fd()), ".state.next.yml", 0); err == nil {
			err = dir.Sync()
		}
		if err != nil {
			return st, err
		}
	} else if !errors.Is(nextErr, os.ErrNotExist) {
		return st, nextErr
	}
	return st, nil
}
func (s *Store) writeState(dir *os.File, st state) (bool, error) {
	raw, err := yaml.Marshal(st)
	if err != nil || len(raw) > stateLimit {
		return false, ErrCorrupt
	}
	return s.atomicWrite(dir, "state.yml", raw, false, false)
}
func (s *Store) intent(id, suffix string) (state, error) {
	var st state
	raw, err := readFile(s.root, id+suffix, stateLimit, true)
	if err != nil {
		return st, err
	}
	if strictYAML(raw, &st) != nil || !st.valid(s.config, id) {
		return st, ErrCorrupt
	}
	initial := suffix == ".initializing.yml"
	if (initial && !((st.Version == 1 && st.Status == "active") || (st.Version == 2 && st.Status == "reserved"))) || (!initial && st.Status != "cancelled") {
		return st, ErrCorrupt
	}
	return st, nil
}
func (s *Store) marker(id string) (state, error)       { return s.intent(id, ".deleting.yml") }
func (s *Store) initializing(id string) (state, error) { return s.intent(id, ".initializing.yml") }
func (s *Store) unavailableIntent(id string) error {
	for _, suffix := range []string{".initializing.yml", ".deleting.yml"} {
		if _, err := s.intent(id, suffix); err == nil {
			return ErrUnavailable
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func allowedEntry(name string, limits Limits) (limit int64, immutable bool, ok bool) {
	switch name {
	case ".lease":
		return 0, false, true
	case "state.yml":
		return stateLimit, false, true
	case "schema.arrow":
		return schemaLimit, true, true
	case "manifest.yml":
		return manifestLimit, true, true
	}
	if name == ".state.next.yml" {
		return stateLimit, false, true
	}
	base := strings.TrimPrefix(name, ".pending-")
	if base == name {
		base = strings.TrimPrefix(name, "part-")
	}
	if len(base) != 10 || !strings.HasSuffix(base, ".arrow") {
		return 0, false, false
	}
	i, err := strconv.Atoi(strings.TrimSuffix(base, ".arrow"))
	if err != nil || i < 0 || i >= limits.MaxParts || fmt.Sprintf("%04d.arrow", i) != base {
		return 0, false, false
	}
	return limits.MaxPartBytes, strings.HasPrefix(name, "part-"), strings.HasPrefix(name, "part-") || strings.HasPrefix(name, ".pending-")
}

func entryFiles(dir *os.File, st state) ([]string, int64, error) {
	names, err := directoryNames(dir, 2*st.Limits.MaxParts+8)
	if err != nil {
		return nil, 0, err
	}
	var total int64
	for _, name := range names {
		limit, immutable, ok := allowedEntry(name, st.Limits)
		if !ok || (st.SchemaSHA256 == "" && (name == "manifest.yml" || strings.HasPrefix(name, "part-") || strings.HasPrefix(name, ".pending-"))) {
			return nil, 0, ErrCorrupt
		}
		f, e := openFile(dir, name, os.O_RDONLY, immutable)
		if e != nil {
			return nil, 0, e
		}
		info, e := checkFile(f, false, immutable)
		f.Close()
		if e != nil || info.Size < 0 || info.Size > limit || info.Size > st.ReservedBytes-total {
			return nil, 0, ErrCorrupt
		}
		total += info.Size
	}
	return names, total, nil
}

// scan holds the root lock. Reservations remain at their original upper bound
// until cleanup, including committed, expired, cancelled and crashed fills.
func (s *Store) scan() ([]string, int64, error) {
	names, err := directoryNames(s.root, 3*s.config.MaxEntries+2)
	if err != nil {
		return nil, 0, err
	}
	ids := map[string]bool{}
	markers := map[string]bool{}
	initials := map[string]bool{}
	for _, name := range names {
		if name == ".lock" || name == "store.yml" {
			continue
		}
		id := strings.TrimSuffix(strings.TrimSuffix(name, ".deleting.yml"), ".initializing.yml")
		if !idPattern.MatchString(id) {
			return nil, 0, ErrCorrupt
		}
		if strings.HasSuffix(name, ".deleting.yml") {
			markers[id] = true
		}
		if strings.HasSuffix(name, ".initializing.yml") {
			initials[id] = true
		}
		ids[id] = true
	}
	if len(ids) > s.config.MaxEntries {
		return nil, 0, ErrLimit
	}
	ordered := make([]string, 0, len(ids))
	var reserved int64 = stateLimit
	for id := range ids {
		var st, marker, initial state
		if initials[id] {
			initial, err = s.initializing(id)
			if err != nil {
				return nil, 0, err
			}
			st = initial
		}
		if markers[id] {
			marker, err = s.marker(id)
			if err != nil {
				return nil, 0, err
			}
			st = marker
			if initials[id] {
				expected := initial
				expected.Status = "cancelled"
				if expected != marker {
					return nil, 0, ErrCorrupt
				}
			}
		}
		dir, e := childDir(s.root, id, false)
		if e == nil {
			actual, e := s.loadState(dir, id)
			if e == nil {
				if markers[id] {
					expected := actual
					expected.Status = "cancelled"
					if expected != marker {
						dir.Close()
						return nil, 0, ErrCorrupt
					}
				}
				if initials[id] && actual != initial {
					dir.Close()
					return nil, 0, ErrCorrupt
				}
				st = actual
			}
			if e != nil && (!(markers[id] || initials[id]) || !errors.Is(e, os.ErrNotExist)) {
				dir.Close()
				return nil, 0, e
			}
			if e != nil {
				if next, nextErr := openFile(dir, ".state.next.yml", os.O_RDONLY, false); nextErr == nil {
					next.Close()
					dir.Close()
					return nil, 0, ErrCorrupt
				} else if !errors.Is(nextErr, os.ErrNotExist) {
					dir.Close()
					return nil, 0, nextErr
				}
			}
			_, _, e = entryFiles(dir, st)
			dir.Close()
			if e != nil {
				return nil, 0, e
			}
		} else if !(markers[id] || initials[id]) || !errors.Is(e, os.ErrNotExist) {
			return nil, 0, e
		}
		if st.ReservedBytes > s.config.MaxStoredBytes-reserved {
			return nil, 0, ErrLimit
		}
		reserved += st.ReservedBytes
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	return ordered, reserved, nil
}

// Begin validates a known schema before reserving capacity, then binds it using
// the supplied trusted identity. Call Reserve before executing a source query
// when its schema is not known yet.
func (s *Store) Begin(ctx context.Context, request Request, schema *arrow.Schema) (*Writer, error) {
	unlock, err := s.guard()
	if err != nil {
		return nil, err
	}
	valid := s.validRequest(request, time.Now().UTC())
	unlock()
	if !valid {
		return nil, ErrInvalid
	}
	raw, err := prepareSchema(ctx, schema, request.Limits)
	if err != nil {
		return nil, err
	}
	w, err := s.reserve(ctx, request, "begin")
	if err != nil {
		return nil, err
	}
	w.mu.Lock()
	err = w.bindSchema(ctx, request.Identity, schema, raw, "begin:after_schema")
	w.mu.Unlock()
	if err != nil {
		return nil, errors.Join(err, w.Close())
	}
	return w, nil
}

func (s *Store) validRequest(request Request, now time.Time) bool {
	return request.Identity.valid() && request.Limits.valid() && request.ExpiresAt.After(now) && request.ExpiresAt.Sub(now) <= s.config.MaxTTL
}

// Reserve durably charges the full budget without needing a schema. The caller
// must reserve before starting source work, then BindSchema with current trusted
// authorization. Until binding, Write and Commit return ErrSchemaUnbound.
func (s *Store) Reserve(ctx context.Context, request Request) (*Writer, error) {
	return s.reserve(ctx, request, "reserve")
}

func (s *Store) reserve(ctx context.Context, request Request, stage string) (*Writer, error) {
	unlock, err := s.guard()
	if err != nil {
		return nil, err
	}
	defer unlock()
	now := time.Now().UTC()
	if !s.validRequest(request, now) {
		return nil, ErrInvalid
	}
	lock, err := lockRoot(ctx, s.root)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	ids, used, err := s.scan()
	if err != nil {
		return nil, err
	}
	reserve := request.Limits.MaxEncodedBytes + metadataReservation
	if len(ids) >= s.config.MaxEntries || reserve > s.config.MaxStoredBytes-used {
		return nil, ErrLimit
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if !time.Now().Before(request.ExpiresAt) {
		return nil, ErrUnavailable
	}
	id, err := randomID()
	if err != nil {
		return nil, err
	}
	fence, err := randomID()
	if err != nil {
		return nil, err
	}
	st := state{Version: 2, ID: id, Fence: fence, Tenant: s.config.Tenant, Identity: request.Identity, CreatedAt: now, ExpiresAt: request.ExpiresAt.UTC(), Limits: request.Limits, ReservedBytes: reserve, Status: "reserved"}
	stateRaw, err := yaml.Marshal(st)
	if err != nil || len(stateRaw) > stateLimit {
		return nil, ErrCorrupt
	}
	if _, err = s.atomicWrite(s.root, id+".initializing.yml", stateRaw, true, true); err != nil {
		return nil, err
	}
	if err = s.point(stage + ":after_intent"); err != nil {
		return nil, err
	}
	dir, err := childDir(s.root, id, true)
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			dir.Close()
		}
	}()
	if err = s.point(stage + ":after_mkdir"); err != nil {
		return nil, err
	}
	lease, err := tryLease(dir, true)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !keep {
			lease.Close()
		}
	}()
	if err = s.point(stage + ":after_lease"); err != nil {
		return nil, err
	}
	if _, err = s.atomicWrite(dir, "state.yml", stateRaw, false, true); err != nil {
		return nil, err
	}
	if err = s.point(stage + ":after_state"); err != nil {
		return nil, err
	}
	if err = unix.Unlinkat(int(s.root.Fd()), id+".initializing.yml", 0); err == nil {
		err = s.point(stage + ":root_sync")
		if err == nil {
			err = s.root.Sync()
		}
	}
	if err != nil {
		return nil, err
	}
	keep = true
	s.references.Add(1)
	return &Writer{store: s, dir: dir, lease: lease, state: st}, nil
}

func (s *Store) Cancel(ctx context.Context, id string, identity Identity) error {
	unlock, err := s.guard()
	if err != nil {
		return err
	}
	defer unlock()
	if !idPattern.MatchString(id) || !identity.valid() {
		return ErrInvalid
	}
	lock, err := lockRoot(ctx, s.root)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err = s.unavailableIntent(id); err != nil {
		return err
	}
	dir, err := childDir(s.root, id, false)
	if err != nil {
		return err
	}
	defer dir.Close()
	st, err := s.loadState(dir, id)
	if err != nil {
		return err
	}
	if !st.Identity.equal(identity) {
		return ErrUnavailable
	}
	if st.Status == "cancelled" {
		return nil
	}
	st.Status = "cancelled"
	published, err := s.writeState(dir, st)
	if err != nil && published {
		return errors.Join(ErrPublicationUncertain, err)
	}
	return err
}

// Cleanup removes only eligible generated entries and validated deletion
// markers. An immutable root-level marker survives interruption of each delete.
func (s *Store) Cleanup(ctx context.Context, maximum int) (result CleanupResult, err error) {
	unlock, err := s.guard()
	if err != nil {
		return result, err
	}
	defer unlock()
	if maximum < 1 || maximum > s.config.MaxEntries {
		return result, ErrInvalid
	}
	lock, err := lockRoot(ctx, s.root)
	if err != nil {
		return result, err
	}
	defer lock.Close()
	ids, _, err := s.scan()
	if err != nil {
		return result, err
	}
	for _, id := range ids {
		if result.Removed >= maximum {
			break
		}
		if err = ctx.Err(); err != nil {
			return result, err
		}
		marker, markerErr := s.marker(id)
		marked := markerErr == nil
		if markerErr != nil && !errors.Is(markerErr, os.ErrNotExist) {
			return result, markerErr
		}
		initial, initialErr := s.initializing(id)
		initializing := initialErr == nil
		if initialErr != nil && !errors.Is(initialErr, os.ErrNotExist) {
			return result, initialErr
		}
		dir, e := childDir(s.root, id, false)
		if errors.Is(e, os.ErrNotExist) && (marked || initializing) {
			if e = s.removeIntents(id); e != nil {
				return result, e
			}
			result.Removed++
			continue
		}
		if e != nil {
			return result, e
		}
		lease, e := tryLease(dir, true)
		if errors.Is(e, ErrBusy) {
			dir.Close()
			result.Busy++
			continue
		}
		if e != nil {
			dir.Close()
			return result, e
		}
		st, e := s.loadState(dir, id)
		if e != nil && errors.Is(e, os.ErrNotExist) {
			if marked {
				st = marker
				e = nil
			} else if initializing {
				st = initial
				e = nil
			}
		}
		if e != nil {
			lease.Close()
			dir.Close()
			return result, e
		}
		if !marked && !initializing && st.Status != "cancelled" && time.Now().Before(st.ExpiresAt) {
			lease.Close()
			dir.Close()
			continue
		}
		names, _, e := entryFiles(dir, st)
		if e != nil {
			lease.Close()
			dir.Close()
			return result, e
		}
		if !marked {
			st.Status = "cancelled"
			raw, marshalErr := yaml.Marshal(st)
			if marshalErr != nil || len(raw) > stateLimit {
				lease.Close()
				dir.Close()
				return result, ErrCorrupt
			}
			_, e = s.atomicWrite(s.root, id+".deleting.yml", raw, true, true)
		}
		if e == nil {
			for _, name := range names {
				if name == ".lease" || name == "state.yml" {
					continue
				}
				if e = unix.Unlinkat(int(dir.Fd()), name, 0); e != nil {
					break
				}
			}
		}
		if e == nil {
			e = dir.Sync()
		}
		if e == nil {
			for _, name := range []string{"state.yml", ".lease"} {
				e = unix.Unlinkat(int(dir.Fd()), name, 0)
				if errors.Is(e, os.ErrNotExist) {
					e = nil
				}
				if e != nil {
					break
				}
			}
		}
		lease.Close()
		dir.Close()
		if e == nil {
			e = unix.Unlinkat(int(s.root.Fd()), id, unix.AT_REMOVEDIR)
		}
		if e == nil {
			e = s.root.Sync()
		}
		if e == nil {
			e = s.removeIntents(id)
		}
		if e != nil {
			return result, e
		}
		result.Removed++
	}
	return result, nil
}

func (s *Store) removeIntents(id string) error {
	for _, suffix := range []string{".initializing.yml", ".deleting.yml"} {
		err := unix.Unlinkat(int(s.root.Fd()), id+suffix, 0)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return s.root.Sync()
}
