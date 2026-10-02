// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/nats-io/nats.go/jetstream"
)

const sourceQuotaBucket = "KELVO_SOURCE_QUOTAS"
const sourceQuotaStoreBytes = 8 << 20

// NATS MaxValueSize includes CAS headers as well as the 32-byte owner token.
// Stored values are still validated as exactly one token or an empty marker.
const sourceQuotaValueBytes = 256

var (
	ErrSourceQuotaBusy        = errors.New("source quota capacity unavailable")
	ErrSourceQuotaUnavailable = errors.New("source quota store unavailable")
	ErrSourceQuotaLeaseLost   = errors.New("source quota lease lost")
)

type sourceQuotaKV interface {
	Get(context.Context, string) (jetstream.KeyValueEntry, error)
	Create(context.Context, string, []byte, ...jetstream.KVCreateOpt) (uint64, error)
	Update(context.Context, string, []byte, uint64) (uint64, error)
}

// SourceQuotaPool reserves tenant-account-scoped fixed slots across nodes. It
// limits admitted Kelvo operations, not the database's internal query count.
// Database-side cancellation remains best effort: a KV lease cannot fence SQL
// already accepted by a remote database.
type SourceQuotaPool struct {
	kv     sourceQuotaKV
	quotas map[string]int
	lease  time.Duration
}

// OpenSourceQuotas creates resources only during explicit initialization. Policy
// equality is already enforced by OpenStore; quotas cannot differ across nodes.
// Server TTL, rather than node clocks, decides when a crashed owner's slot is
// reclaimable. A live owner cancels at half the TTL to leave cleanup headroom.
func (s *NATSStore) OpenSourceQuotas(ctx context.Context, initialize bool) (*SourceQuotaPool, error) {
	if len(s.policy.SourceQuotas) == 0 {
		return &SourceQuotaPool{}, nil
	}
	if err := ValidateSourceQuotas(s.policy.SourceQuotas); err != nil {
		return nil, err
	}
	if s.policy.LeaseDuration < 5*time.Second || s.policy.LeaseDuration > time.Minute {
		return nil, errors.New("invalid source quota lease duration")
	}
	kv, err := s.js.KeyValue(ctx, sourceQuotaBucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) && initialize {
		kv, err = s.js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: sourceQuotaBucket, History: 1, TTL: 2 * s.policy.LeaseDuration, MaxValueSize: sourceQuotaValueBytes, MaxBytes: sourceQuotaStoreBytes, Replicas: s.policy.Replicas})
		if err != nil {
			kv, err = s.js.KeyValue(ctx, sourceQuotaBucket)
		}
	}
	if err != nil {
		return nil, ErrSourceQuotaUnavailable
	}
	if err = configureKVDirect(ctx, s.js, sourceQuotaBucket, initialize); err != nil {
		return nil, err
	}
	kv, err = s.js.KeyValue(ctx, sourceQuotaBucket)
	if err != nil {
		return nil, ErrSourceQuotaUnavailable
	}
	status, err := kv.Status(ctx)
	if err != nil {
		return nil, ErrSourceQuotaUnavailable
	}
	cfg := status.Config()
	if cfg.History != 1 || cfg.TTL != 2*s.policy.LeaseDuration || cfg.MaxValueSize != sourceQuotaValueBytes || cfg.MaxBytes != sourceQuotaStoreBytes || cfg.Replicas != s.policy.Replicas || cfg.Storage != jetstream.FileStorage {
		return nil, errors.New("source quota configuration mismatch")
	}
	stream, err := s.js.Stream(ctx, "KV_"+sourceQuotaBucket)
	if err != nil {
		return nil, ErrSourceQuotaUnavailable
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, ErrSourceQuotaUnavailable
	}
	if info.Config.Discard != jetstream.DiscardNew || info.Config.Retention != jetstream.LimitsPolicy || info.Config.MaxAge != 2*s.policy.LeaseDuration || info.Config.AllowDirect {
		return nil, errors.New("source quota retention configuration mismatch")
	}
	quotas := make(map[string]int, len(s.policy.SourceQuotas))
	for id, count := range s.policy.SourceQuotas {
		quotas[id] = count
	}
	return &SourceQuotaPool{kv: kv, quotas: quotas, lease: s.policy.LeaseDuration}, nil
}

func ValidateSourceQuotas(quotas map[string]int) error {
	if len(quotas) > 64 {
		return errors.New("source quotas support at most 64 source identities")
	}
	for id, count := range quotas {
		if !catalog.ValidID(id) || count < 1 || count > 64 {
			return errors.New("invalid source quota identity or capacity")
		}
	}
	return nil
}

func sourceQuotaKey(id string, slot int) string {
	sum := sha256.Sum256([]byte(id))
	return fmt.Sprintf("source.%s.%02x", hex.EncodeToString(sum[:]), slot)
}

type sourceQuotaSlot struct {
	key      string
	revision uint64
}

// Acquire reserves the intersection of selected sources and configured quotas.
// Duplicate IDs consume one slot. On contention every partial reservation is
// rolled back before waiting, so joins cannot deadlock opposing acquisitions.
// The caller must release only after its subprocess/result cleanup is complete.
func (p *SourceQuotaPool) Acquire(ctx context.Context, ids []string) (context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if p == nil || len(p.quotas) == 0 {
		return ctx, func() {}, nil
	}
	if len(ids) > 64 {
		return nil, nil, errors.New("source quota selection exceeds 64 identities")
	}
	selected := make([]string, 0, len(ids))
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if p.quotas[id] > 0 && !seen[id] {
			seen[id] = true
			selected = append(selected, id)
		}
	}
	if len(selected) == 0 {
		return ctx, func() {}, nil
	}
	sort.Strings(selected)
	owner, err := randomToken()
	if err != nil {
		return nil, nil, err
	}
	for {
		started := time.Now()
		attemptCtx, stop := context.WithTimeout(ctx, p.lease)
		slots, claimErr := p.tryAcquire(attemptCtx, selected, owner)
		stop()
		if claimErr == nil && time.Since(started) < p.lease && ctx.Err() == nil {
			return p.hold(ctx, slots, owner, started.Add(p.lease))
		}
		p.release(slots)
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		if claimErr == nil {
			return nil, nil, ErrSourceQuotaLeaseLost
		}
		if !errors.Is(claimErr, ErrSourceQuotaBusy) {
			return nil, nil, claimErr
		}
		timer := time.NewTimer(100*time.Millisecond + time.Duration(owner[0]%50)*time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (p *SourceQuotaPool) tryAcquire(ctx context.Context, ids []string, owner string) ([]sourceQuotaSlot, error) {
	slots := make([]sourceQuotaSlot, 0, len(ids))
	for _, id := range ids {
		claimed := false
		for slot := 0; slot < p.quotas[id]; slot++ {
			key := sourceQuotaKey(id, slot)
			entry, err := p.kv.Get(ctx, key)
			var revision uint64
			if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrKeyDeleted) {
				revision, err = p.kv.Create(ctx, key, []byte(owner))
			} else if err == nil {
				value := string(entry.Value())
				if value != "" {
					if !validOwner(value) {
						return slots, ErrSourceQuotaUnavailable
					}
					continue
				}
				revision, err = p.kv.Update(ctx, key, []byte(owner), entry.Revision())
			}
			if err == nil {
				slots = append(slots, sourceQuotaSlot{key, revision})
				claimed = true
				break
			}
			if !errors.Is(err, jetstream.ErrKeyExists) && !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
				return slots, ErrSourceQuotaUnavailable
			}
		}
		if !claimed {
			return slots, ErrSourceQuotaBusy
		}
	}
	return slots, nil
}

// Release writes a free marker with the last owned revision. It never deletes
// or overwrites a successor, even following a delayed RPC or expired lease.
func (p *SourceQuotaPool) release(slots []sourceQuotaSlot) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, slot := range slots {
		_, _ = p.kv.Update(ctx, slot.key, nil, slot.revision)
	}
}

func (p *SourceQuotaPool) hold(parent context.Context, slots []sourceQuotaSlot, owner string, deadline time.Time) (context.Context, func(), error) {
	ctx, cancel := context.WithCancelCause(parent)
	watchdog := time.AfterFunc(max(0, time.Until(deadline)), func() { cancel(ErrSourceQuotaLeaseLost) })
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer watchdog.Stop()
		ticker := time.NewTicker(p.lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			nextDeadline := time.Now().Add(p.lease)
			renewCtx, stop := context.WithDeadline(ctx, deadline)
			failed := false
			for i := range slots {
				revision, err := p.kv.Update(renewCtx, slots[i].key, []byte(owner), slots[i].revision)
				if err != nil {
					failed = true
					break
				}
				slots[i].revision = revision
			}
			expired := renewCtx.Err() != nil
			stop()
			if failed || expired || !time.Now().Before(deadline) {
				cancel(ErrSourceQuotaLeaseLost)
				return
			}
			deadline = nextDeadline
			watchdog.Reset(max(0, time.Until(deadline)))
		}
	}()
	var once sync.Once
	release := func() { once.Do(func() { cancel(context.Canceled); <-done; p.release(slots) }) }
	if ctx.Err() != nil {
		release()
		return nil, nil, context.Cause(ctx)
	}
	return ctx, release, nil
}
