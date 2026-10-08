// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"

	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// commandIO gives a process concrete inherited files. Process launch and wait
// own no implicit os/exec copy goroutines. This owner retains its custody until
// launch returns, both explicit pumps stop, and the stdout consumer returns.
type commandIO struct {
	mu                                     sync.Mutex
	ctx                                    context.Context
	input                                  []byte
	diagnostic                             io.Writer
	child                                  []*os.File
	inputWrite, outputRead, diagnosticRead *os.File
	launching, attempted, started          bool
	consuming, consumed, finishing         bool
	pumps, callbacks                       int
	stopOutput                             func() bool
	outputOnce                             sync.Once
	complete, released                     bool
	ioErr                                  error
	done                                   chan struct{}
	release                                func()
	quarantine                             func()
}

func newCommandIO(ctx context.Context, command *exec.Cmd, payload []byte, diagnostic io.Writer, custody *containment.Custody, quarantine func()) (_ *commandIO, resultErr error) {
	if ctx == nil || command == nil || command.Process != nil || command.Stdin != nil || command.Stdout != nil || command.Stderr != nil || diagnostic == nil {
		return nil, containment.ErrInvalid
	}
	p := &commandIO{ctx: ctx, diagnostic: diagnostic, done: make(chan struct{}), release: func() {}, quarantine: quarantine}
	defer func() {
		if resultErr != nil {
			p.closeFiles()
		}
	}()
	inputRead, inputWrite, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	p.child, p.inputWrite = []*os.File{inputRead}, inputWrite
	outputRead, outputWrite, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	p.outputRead = outputRead
	p.child = append(p.child, outputWrite)
	if diagnostic == io.Discard {
		null, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
		if err != nil {
			return nil, err
		}
		p.child = append(p.child, null)
	} else {
		read, write, err := os.Pipe()
		if err != nil {
			return nil, err
		}
		p.diagnosticRead = read
		p.child = append(p.child, write)
	}
	// Fd prepares the solely owned child endpoints as blocking. The opposite
	// parent endpoints remain pollable and can be interrupted by Close.
	for _, file := range p.child {
		_ = file.Fd()
	}
	if custody != nil {
		p.release, err = custody.Hold()
		if err != nil {
			return nil, err
		}
	}
	// The caller may return on an uncertain cleanup deadline. Keep a private
	// input copy until its actual writer stops, then erase that copy.
	p.input = bytes.Clone(payload)
	command.Stdin, command.Stdout, command.Stderr = p.child[0], p.child[1], p.child[2]
	return p, nil
}

func (p *commandIO) closeFiles() {
	for _, file := range append([]*os.File{p.inputWrite, p.outputRead, p.diagnosticRead}, p.child...) {
		if file != nil {
			_ = file.Close()
		}
	}
}

func (p *commandIO) maybeCompleteLocked() func() {
	if p.finishing && !p.launching && p.pumps == 0 && p.callbacks == 0 && !p.consuming && !p.complete {
		p.complete = true
		p.closeFiles()
		clear(p.input)
		p.input = nil
	}
	if p.complete && !p.released {
		p.released = true
		return func() {
			p.release()
			close(p.done)
		}
	}
	return nil
}

// Start keeps the inherited files open until the physical launch has returned.
// The launch callback must not call Wait. A concurrent Finish never waits for
// the callback while holding a mutex or releases its unfinished launch custody.
func (p *commandIO) Start(launch func() error) error {
	if launch == nil {
		return containment.ErrInvalid
	}
	p.mu.Lock()
	if p.attempted || p.finishing {
		p.mu.Unlock()
		return containment.ErrInvalid
	}
	p.attempted, p.launching = true, true
	p.mu.Unlock()
	err := p.ctx.Err()
	if err == nil {
		err = launch()
	}
	p.mu.Lock()
	p.launching = false
	p.started = err == nil
	for _, file := range p.child {
		_ = file.Close()
	}
	if err == nil && !p.finishing {
		p.callbacks = 1
		p.stopOutput = context.AfterFunc(p.ctx, func() {
			_ = p.outputRead.Close()
			p.outputCallbackDone()
		})
		p.pumps = 1
		if p.diagnosticRead != nil {
			p.pumps++
		}
		go p.writeInput()
		if p.diagnosticRead != nil {
			go p.readDiagnostic()
		}
	} else {
		p.closeFiles()
		clear(p.input)
		p.input = nil
	}
	release := p.maybeCompleteLocked()
	p.mu.Unlock()
	if release != nil {
		release()
	}
	return err
}

func (p *commandIO) pumpDone(err error) {
	p.mu.Lock()
	if err != nil {
		// Do not retain raw reader/writer errors: they may contain source data.
		p.ioErr = query.NewError("QUERY_FAILED", "Worker input or diagnostic output did not complete")
	}
	p.pumps--
	release := p.maybeCompleteLocked()
	p.mu.Unlock()
	if release != nil {
		release()
	}
}

func (p *commandIO) writeInput() {
	callbackDone := make(chan struct{})
	stop := context.AfterFunc(p.ctx, func() {
		defer close(callbackDone)
		_ = p.inputWrite.Close()
	})
	_, err := io.Copy(p.inputWrite, bytes.NewReader(p.input))
	if stop() {
		close(callbackDone)
	}
	<-callbackDone
	_ = p.inputWrite.Close()
	// A child can reject input and exit without consuming its whole envelope.
	// Match os/exec's stdin EPIPE treatment; its exit/outcome remains authoritative.
	if errors.Is(err, syscall.EPIPE) || errors.Is(err, os.ErrClosed) {
		err = nil
	}
	p.mu.Lock()
	clear(p.input)
	p.input = nil
	p.mu.Unlock()
	p.pumpDone(err)
}

func (p *commandIO) readDiagnostic() {
	// Expose Write only: an embedded bytes.Buffer.ReadFrom must not bypass a
	// bounded diagnostic writer's Write limit through io.Copy's fast path.
	_, err := io.Copy(struct{ io.Writer }{p.diagnostic}, p.diagnosticRead)
	_ = p.diagnosticRead.Close()
	if errors.Is(err, os.ErrClosed) {
		err = nil
	}
	p.pumpDone(err)
}

// Consume runs the caller's Arrow/file decoder synchronously. A blocked sink
// still owns capacity even if another goroutine has already stopped the child.
func (p *commandIO) Consume(read func(io.Reader) error) error {
	if read == nil {
		return containment.ErrInvalid
	}
	p.mu.Lock()
	if !p.started || p.consumed || p.finishing {
		p.mu.Unlock()
		return containment.ErrInvalid
	}
	p.consumed, p.consuming = true, true
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.consuming = false
		release := p.maybeCompleteLocked()
		p.mu.Unlock()
		if release != nil {
			release()
		}
	}()
	return read(p.outputRead)
}

func (p *commandIO) outputCallbackDone() {
	p.outputOnce.Do(func() {
		p.mu.Lock()
		p.callbacks--
		release := p.maybeCompleteLocked()
		p.mu.Unlock()
		if release != nil {
			release()
		}
	})
}

// Diagnostics are readable only after their writer and cancellation callbacks
// have joined. An uncertain cleanup must not decode a concurrently written buffer.
func decodeCommandOutcome(p *commandIO, diagnostic *boundedBuffer) (Outcome, error) {
	select {
	case <-p.done:
		var outcome Outcome
		err := json.Unmarshal(bytes.TrimSpace(diagnostic.Bytes()), &outcome)
		return outcome, err
	default:
		return Outcome{}, containment.ErrQuarantined
	}
}

// Finish is called after native cleanup and output consumption. A deadline
// interrupts owned pipes but cannot force an arbitrary writer/consumer to return.
// Its custody remains held until that callback actually finishes.
func (p *commandIO) Finish(parent context.Context) error {
	if parent == nil {
		return containment.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(parent, query.WorkerWaitDelay)
	defer cancel()
	p.mu.Lock()
	p.finishing = true
	stopOutput := p.stopOutput
	if !p.launching && !p.started {
		p.closeFiles()
	}
	release := p.maybeCompleteLocked()
	p.mu.Unlock()
	if stopOutput != nil && stopOutput() {
		p.outputCallbackDone()
	}
	if release != nil {
		release()
	}
	select {
	case <-p.done:
		p.closeFiles()
		return p.ioErr
	case <-ctx.Done():
		select {
		case <-p.done:
			return p.ioErr
		default:
		}
		p.mu.Lock()
		// Child descriptors must stay valid through an in-flight launch.
		launching := p.launching
		p.mu.Unlock()
		_ = p.inputWrite.Close()
		_ = p.outputRead.Close()
		if p.diagnosticRead != nil {
			_ = p.diagnosticRead.Close()
		}
		if !launching {
			for _, file := range p.child {
				_ = file.Close()
			}
		}
		if p.quarantine != nil {
			p.quarantine()
		}
		return containment.ErrQuarantined
	}
}
