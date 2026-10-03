//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package audit

import (
	"container/heap"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

type diskIO struct {
	write    func(*os.File, []byte, int64) error
	sync     func(*os.File) error
	allocate func(int, int64) error
	now      func() time.Time
}

func defaultIO() diskIO {
	return diskIO{write: writeExact, sync: func(f *os.File) error { return f.Sync() }, allocate: func(fd int, size int64) error { return unix.Fallocate(fd, 0, 0, size) }, now: time.Now}
}

type slotMeta struct {
	id       string
	started  int64
	startSHA [32]byte
	active   bool
}
type expiry struct {
	slot int
	at   int64
}
type expiryQueue []expiry

func (q expiryQueue) Len() int           { return len(q) }
func (q expiryQueue) Less(i, j int) bool { return q[i].at < q[j].at }
func (q expiryQueue) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }
func (q *expiryQueue) Push(x any)        { *q = append(*q, x.(expiry)) }
func (q *expiryQueue) Pop() any          { old := *q; x := old[len(old)-1]; *q = old[:len(old)-1]; return x }

// Journal owns one I/O worker and one lifetime writer lock. A blocked syscall
// cannot be interrupted by Go contexts: timeout fences its result, not its file
// descriptor. No replacement worker is launched and Close never closes early.
type Journal struct {
	cfg                                     Config
	scope                                   Scope
	directory, lock, file                   *os.File
	io                                      diskIO
	slots                                   []slotMeta
	free                                    []int
	expiry                                  expiryQueue
	jobs                                    chan *operation
	pending                                 chan struct{}
	stop, done                              chan struct{}
	lifecycle                               sync.Mutex
	closing                                 atomic.Bool
	fault                                   atomic.Bool
	active, retained, freeCount, nextExpiry atomic.Int64
	closeErr                                error // read only after done closes
}

func randomID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", ErrUnavailable
	}
	return hex.EncodeToString(id[:]), nil
}
func slotOffset(slot int) int64 { return int64(headerSize) + int64(slot)*int64(slotSize) }

// Open completes preallocation, validation and crash recovery before admitting
// work. Startup filesystem syscalls are synchronous and are never retried by a
// background loop. Runtime writes use the bounded, timed worker below.
func Open(cfg Config, scope Scope) (*Journal, error) { return openWithIO(cfg, scope, defaultIO()) }

func openWithIO(cfg Config, scope Scope, operations diskIO) (journal *Journal, err error) {
	if err = cfg.Validate(); err != nil {
		return nil, err
	}
	cfg.Directory = strings.Clone(cfg.Directory)
	scope, err = normalizedScope(scope)
	if err != nil {
		return nil, err
	}
	j := &Journal{cfg: cfg, scope: scope, io: operations, jobs: make(chan *operation, cfg.MaxPending), pending: make(chan struct{}, cfg.MaxPending), stop: make(chan struct{}), done: make(chan struct{})}
	defer func() {
		if journal == nil {
			if j.file != nil {
				j.file.Close()
			}
			if j.lock != nil {
				j.lock.Close()
			}
			if j.directory != nil {
				j.directory.Close()
			}
		}
	}()
	j.directory, err = openDirectory(cfg.Directory, true)
	if err != nil {
		return nil, err
	}
	j.lock, err = lockWriter(j.directory, false)
	if err != nil {
		return nil, err
	}
	j.file, err = openChild(j.directory, journalName, os.O_RDWR|os.O_CREATE|os.O_EXCL)
	fresh := err == nil
	if errors.Is(err, unix.EEXIST) {
		j.file, err = openChild(j.directory, journalName, os.O_RDWR)
	}
	if err != nil {
		return nil, ErrUnavailable
	}
	if fresh {
		size := int64(headerSize) + int64(cfg.MaxEntries)*int64(slotSize)
		if operations.allocate(int(j.file.Fd()), size) != nil {
			return nil, ErrUnavailable
		}
		id, e := randomID()
		if e != nil {
			return nil, e
		}
		h := journalHeader{Version: 1, ID: id, Config: cfg, Scope: scope}
		raw, e := encodeFrame(headerFrame, id, h, headerSize)
		if e != nil {
			return nil, e
		}
		if operations.write(j.file, raw, 0) != nil || operations.sync(j.file) != nil || j.directory.Sync() != nil {
			return nil, ErrUncertain
		}
	} else {
		raw := make([]byte, headerSize)
		if readExact(j.file, raw, 0) != nil {
			return nil, ErrCorrupt
		}
		h, e := decodeHeader(raw, cfg.Directory)
		if e != nil || !reflect.DeepEqual(h.Config, cfg) || !reflect.DeepEqual(h.Scope, scope) {
			return nil, ErrCorrupt
		}
	}
	st, e := checkFile(j.file, false)
	if e != nil || st.Size != int64(headerSize)+int64(cfg.MaxEntries)*int64(slotSize) {
		return nil, ErrCorrupt
	}
	j.slots = make([]slotMeta, cfg.MaxEntries)
	j.free = make([]int, 0, cfg.MaxEntries)
	raw := make([]byte, slotSize)
	recovered := false
	for slot := range cfg.MaxEntries {
		if fresh {
			j.free = append(j.free, slot)
			continue
		}
		if readExact(j.file, raw, slotOffset(slot)) != nil {
			return nil, ErrCorrupt
		}
		start, finish, e := decodeSlot(raw, scope, cfg.Retention)
		if e != nil {
			return nil, e
		}
		if start.ID == "" {
			j.free = append(j.free, slot)
			continue
		}
		j.slots[slot] = slotMeta{id: start.ID, started: start.StartedAt, startSHA: sha256.Sum256(raw[:frameSize])}
		if finish == nil {
			// Missing terminal state says nothing about actual external effects.
			finish = &finishRecord{ID: start.ID, StartSHA256: hex.EncodeToString(j.slots[slot].startSHA[:]), FinishedAt: max(start.StartedAt, operations.now().UnixNano()), Outcome: Unknown, Category: Interrupted}
			if finish.FinishedAt > math.MaxInt64-int64(cfg.Retention) {
				return nil, ErrCorrupt
			}
			frame, e := encodeFrame(finishFrame, start.ID, finish, frameSize)
			if e != nil {
				return nil, e
			}
			if operations.write(j.file, frame, slotOffset(slot)+frameSize) != nil {
				return nil, ErrUncertain
			}
			recovered = true
		}
		heap.Push(&j.expiry, expiry{slot: slot, at: finish.FinishedAt + int64(cfg.Retention)})
	}
	if recovered && operations.sync(j.file) != nil {
		return nil, ErrUncertain
	}
	j.publishState()
	go j.run()
	return j, nil
}

func (j *Journal) publishState() {
	j.freeCount.Store(int64(len(j.free)))
	j.retained.Store(int64(len(j.expiry)))
	next := int64(0)
	if len(j.expiry) > 0 {
		next = j.expiry[0].at
	}
	j.nextExpiry.Store(next)
}

func (j *Journal) Ready() bool {
	if j == nil || j.closing.Load() || j.fault.Load() {
		return false
	}
	next := j.nextExpiry.Load()
	return j.freeCount.Load() > 0 || (next > 0 && !j.io.now().Before(time.Unix(0, next)))
}

func (j *Journal) Snapshot() State {
	if j == nil {
		return State{}
	}
	return State{MaxEntries: j.cfg.MaxEntries, MaxPending: j.cfg.MaxPending, Active: j.active.Load(), Retained: j.retained.Load(), Queued: len(j.jobs), Healthy: !j.fault.Load() && !j.closing.Load(), Closing: j.closing.Load()}
}

func (j *Journal) Begin(ctx context.Context, binding Binding, kind Kind) (*Receipt, error) {
	if j == nil {
		return nil, ErrInvalid
	}
	if err := validateBinding(j.scope, binding, kind); err != nil {
		return nil, err
	}
	if binding.TenantID == "" {
		return nil, ErrInvalid
	}
	op := newOperation(ctx, j.cfg.WriteTimeout)
	op.kind = beginOperation
	op.binding = cloneBinding(binding)
	op.eventKind = Kind(strings.Clone(string(kind)))
	if err := j.enqueue(ctx, op, true); err != nil {
		return nil, err
	}
	return j.await(ctx, op)
}

// Record stores a complete fixed event, for example an authentication denial.
// A failed Record must not authorize effects; denial can still be returned when
// the finite audit journal is unavailable. It has no silent-drop mode.
func (j *Journal) Record(ctx context.Context, binding Binding, kind Kind, outcome Outcome, category Category) error {
	if j == nil || !validOutcome(outcome, category) {
		return ErrInvalid
	}
	if err := validateBinding(j.scope, binding, kind); err != nil {
		return err
	}
	if binding.TenantID == "" && (kind != Authentication || outcome != Denied) {
		return ErrInvalid
	}
	op := newOperation(ctx, j.cfg.WriteTimeout)
	op.kind = recordOperation
	op.binding = cloneBinding(binding)
	op.eventKind = Kind(strings.Clone(string(kind)))
	op.outcome = Outcome(strings.Clone(string(outcome)))
	op.category = Category(strings.Clone(string(category)))
	if err := j.enqueue(ctx, op, true); err != nil {
		return err
	}
	_, err := j.await(ctx, op)
	return err
}

func (j *Journal) enqueueFinish(ctx context.Context, r *Receipt, outcome Outcome, category Category) (*operation, error) {
	op := newOperation(ctx, j.cfg.WriteTimeout)
	op.kind = finishOperation
	op.receipt = r
	op.outcome = Outcome(strings.Clone(string(outcome)))
	op.category = Category(strings.Clone(string(category)))
	if err := j.enqueue(ctx, op, false); err != nil {
		return nil, err
	}
	return op, nil
}

func (j *Journal) enqueue(ctx context.Context, op *operation, newReservation bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	j.lifecycle.Lock()
	defer j.lifecycle.Unlock()
	if j.closing.Load() {
		return ErrClosed
	}
	if newReservation && j.fault.Load() {
		return ErrUncertain
	}
	if newReservation {
		select {
		case j.pending <- struct{}{}:
		default:
			return ErrBusy
		}
	}
	// Each new operation or finishing receipt owns one pending token, so this
	// queue cannot grow beyond MaxPending even while the writer is blocked.
	select {
	case j.jobs <- op:
		return nil
	default:
		if newReservation {
			<-j.pending
		}
		return ErrBusy
	}
}

func (j *Journal) await(ctx context.Context, op *operation) (*Receipt, error) {
	remaining := time.Until(op.deadline)
	if remaining < 0 {
		remaining = 0
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-op.done:
	case <-ctx.Done():
	case <-timer.C:
	}
	op.mu.Lock()
	defer op.mu.Unlock()
	if op.completed {
		return op.result.receipt, op.result.err
	}
	op.fenced = true
	j.fault.Store(true)
	return nil, ErrUncertain
}

func (j *Journal) run() {
	defer close(j.done)
	for {
		select {
		case op := <-j.jobs:
			j.process(op)
		case <-j.stop:
			for {
				select {
				case op := <-j.jobs:
					j.process(op)
				default:
					goto drained
				}
			}
		drained:
			j.shutdown()
			return
		}
	}
}

func (j *Journal) process(op *operation) {
	op.mu.Lock()
	fenced := op.fenced || op.cancellationObserved() || !time.Now().Before(op.deadline)
	op.mu.Unlock()
	var result operationResult
	if op.kind != finishOperation && (j.closing.Load() || j.fault.Load()) {
		<-j.pending
		result.err = ErrUncertain
		if j.closing.Load() {
			result.err = ErrClosed
		}
	} else if fenced {
		j.fault.Store(true)
		result.err = ErrUncertain
		if op.kind != finishOperation {
			<-j.pending
		}
	} else if op.kind == finishOperation {
		result.err = j.finish(op.receipt, op.outcome, op.category)
	} else {
		result.receipt, result.err = j.begin(op.binding, op.eventKind)
		if result.err == nil && op.kind == recordOperation {
			result.err = j.finish(result.receipt, op.outcome, op.category)
			result.receipt = nil
		}
	}
	op.mu.Lock()
	if op.fenced || op.cancellationObserved() || !time.Now().Before(op.deadline) {
		j.fault.Store(true)
		result = operationResult{err: ErrUncertain}
	}
	op.result = result
	op.completed = true
	close(op.done)
	op.mu.Unlock()
}

func (j *Journal) begin(binding Binding, kind Kind) (*Receipt, error) {
	st, statErr := checkFile(j.file, false)
	if statErr != nil || st.Size != int64(headerSize)+int64(j.cfg.MaxEntries)*slotSize {
		j.fault.Store(true)
		<-j.pending
		return nil, ErrCorrupt
	}
	started := j.io.now().UnixNano()
	if started <= 0 || started > math.MaxInt64-int64(j.cfg.Retention) {
		j.fault.Store(true)
		<-j.pending
		return nil, ErrUnavailable
	}
	id, err := randomID()
	if err != nil {
		<-j.pending
		return nil, err
	}
	var slot int
	if len(j.free) > 0 {
		slot = j.free[len(j.free)-1]
		j.free = j.free[:len(j.free)-1]
	} else if len(j.expiry) > 0 && j.expiry[0].at <= j.io.now().UnixNano() {
		slot = heap.Pop(&j.expiry).(expiry).slot
	} else {
		<-j.pending
		return nil, ErrFull
	}
	start := startRecord{ID: id, Binding: binding, Kind: kind, StartedAt: started}
	frame, err := encodeFrame(startFrame, id, start, frameSize)
	if err != nil {
		j.free = append(j.free, slot)
		j.publishState()
		<-j.pending
		return nil, err
	}
	j.slots[slot] = slotMeta{id: id, started: started, startSHA: sha256.Sum256(frame), active: true}
	j.active.Add(1)
	j.publishState()
	// The old entry, if any, is already expired. Clear its terminal frame
	// before publishing a new generation; mismatched/torn generations fail closed.
	if j.io.write(j.file, make([]byte, slotSize), slotOffset(slot)) != nil || j.io.write(j.file, frame, slotOffset(slot)) != nil || j.io.sync(j.file) != nil {
		j.fault.Store(true)
		return nil, ErrUncertain
	}
	return &Receipt{journal: j, id: id, slot: slot}, nil
}

func (j *Journal) finish(receipt *Receipt, outcome Outcome, category Category) error {
	if receipt == nil || receipt.slot < 0 || receipt.slot >= len(j.slots) {
		j.fault.Store(true)
		return ErrCorrupt
	}
	meta := j.slots[receipt.slot]
	if !meta.active || meta.id != receipt.id {
		return ErrClosed
	}
	st, statErr := checkFile(j.file, false)
	if statErr != nil || st.Size != int64(headerSize)+int64(j.cfg.MaxEntries)*slotSize {
		j.fault.Store(true)
		return ErrCorrupt
	}
	raw := make([]byte, slotSize)
	if readExact(j.file, raw, slotOffset(receipt.slot)) != nil {
		j.fault.Store(true)
		return ErrCorrupt
	}
	start, existing, err := decodeSlot(raw, j.scope, j.cfg.Retention)
	if err != nil || start.ID != meta.id || existing != nil || sha256.Sum256(raw[:frameSize]) != meta.startSHA {
		j.fault.Store(true)
		return ErrCorrupt
	}
	finished := max(meta.started, j.io.now().UnixNano())
	if finished > math.MaxInt64-int64(j.cfg.Retention) {
		j.fault.Store(true)
		return ErrUnavailable
	}
	terminal := finishRecord{ID: meta.id, StartSHA256: hex.EncodeToString(meta.startSHA[:]), FinishedAt: finished, Outcome: outcome, Category: category}
	frame, err := encodeFrame(finishFrame, meta.id, terminal, frameSize)
	if err != nil {
		return err
	}
	if j.io.write(j.file, frame, slotOffset(receipt.slot)+frameSize) != nil || j.io.sync(j.file) != nil {
		j.fault.Store(true)
		return ErrUncertain
	}
	j.slots[receipt.slot].active = false
	j.active.Add(-1)
	<-j.pending
	heap.Push(&j.expiry, expiry{slot: receipt.slot, at: finished + int64(j.cfg.Retention)})
	j.publishState()
	return nil
}

func (j *Journal) shutdown() {
	for slot, meta := range j.slots {
		if !meta.active {
			continue
		}
		if err := j.finish(&Receipt{journal: j, id: meta.id, slot: slot}, Unknown, Interrupted); err != nil {
			j.closeErr = ErrUncertain
		}
	}
	if j.file.Close() != nil {
		j.closeErr = ErrUncertain
	}
	if j.lock.Close() != nil {
		j.closeErr = ErrUncertain
	}
	if j.directory.Close() != nil {
		j.closeErr = ErrUncertain
	}
	if j.fault.Load() && j.closeErr == nil {
		j.closeErr = ErrUncertain
	}
}

// Close fences new calls and joins the same writer. A deadline does not release
// its lock/fd while a syscall is pending; later Close can wait for final cleanup.
// Any unfinished receipts become Unknown, never successful or automatically replayed.
func (j *Journal) Close(ctx context.Context) error {
	if j == nil {
		return nil
	}
	j.lifecycle.Lock()
	if !j.closing.Load() {
		j.closing.Store(true)
		close(j.stop)
	}
	j.lifecycle.Unlock()
	select {
	case <-j.done:
		return j.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
