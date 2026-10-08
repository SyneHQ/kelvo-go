//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

func TestNamespacePolicyKeepsParentHeadroomInsideContainerCap(t *testing.T) {
	policy := NamespacePolicy{Container: kernelLimits(), Native: kernelLimits(), ParentMemoryBytes: 32 << 20,
		CancellationGrace: 6 * time.Second, CleanupTimeout: time.Second}
	policy.Container.MemoryBytes = 256 << 20
	policy.Native.MemoryBytes = 128 << 20
	if policy.validate() != nil {
		t.Fatal("bounded native and parent budgets were rejected")
	}
	for _, change := range []func(*NamespacePolicy){
		func(p *NamespacePolicy) { p.Native.MemoryBytes = p.Container.MemoryBytes },
		func(p *NamespacePolicy) { p.ParentMemoryBytes = 0 },
		func(p *NamespacePolicy) { p.Native.CPUQuotaMicros = 2 * p.Container.CPUQuotaMicros },
		func(p *NamespacePolicy) { p.CancellationGrace = -1 },
		func(p *NamespacePolicy) { p.CleanupTimeout = time.Minute },
	} {
		changed := policy
		change(&changed)
		if changed.validate() == nil {
			t.Fatal("invalid namespace policy was accepted", changed)
		}
	}
}

func TestNamespaceCommandRejectsImplicitPumpsAndExecOptions(t *testing.T) {
	null, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer null.Close()
	command := func() *exec.Cmd {
		c := exec.Command("/probe/child", "run")
		c.Dir = "/"
		c.Env = []string{"LANG=C"}
		c.Stdin, c.Stdout, c.Stderr = null, null, null
		return c
	}
	valid := command()
	valid.ExtraFiles = []*os.File{nil, null}
	spec, err := namespaceCommand(valid)
	if err != nil || len(spec.Files) != 5 || spec.Files[3] != nil || spec.Files[4] != null {
		t.Fatal("optional descriptor numbers changed", err)
	}
	for name, change := range map[string]func(*exec.Cmd){
		"input-pump":         func(c *exec.Cmd) { c.Stdin = bytes.NewReader(nil) },
		"output-pump":        func(c *exec.Cmd) { c.Stdout = &bytes.Buffer{} },
		"diagnostic-pump":    func(c *exec.Cmd) { c.Stderr = &bytes.Buffer{} },
		"implicit-file":      func(c *exec.Cmd) { c.Stderr = nil },
		"context-watcher":    func(c *exec.Cmd) { c.Cancel = func() error { return nil } },
		"wait-delay":         func(c *exec.Cmd) { c.WaitDelay = time.Second },
		"process-group":      func(c *exec.Cmd) { c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} },
		"relative-directory": func(c *exec.Cmd) { c.Dir = "." },
		"path-error":         func(c *exec.Cmd) { c.Err = os.ErrNotExist },
	} {
		t.Run(name, func(t *testing.T) {
			c := command()
			change(c)
			if _, err := namespaceCommand(c); !errors.Is(err, ErrInvalid) {
				t.Fatal("unsupported command was accepted", err)
			}
		})
	}
}

func TestNamespaceDomainRejectsAmbientHost(t *testing.T) {
	if os.Getpid() == 1 {
		t.Skip("ordinary host rejection requires a non-init process")
	}
	if _, err := OpenNamespaceDomain(NamespacePolicy{}, nil); !errors.Is(err, ErrInvalid) {
		t.Fatal("missing admission and budgets were accepted", err)
	}
	if _, err := (&NamespaceDomain{}).PrepareProcess(nil, Limits{}, func() {}); err == nil {
		t.Fatal("uninitialized domain prepared a process")
	}
	if err := (&NamespaceDomain{}).Close(context.Background()); !errors.Is(err, ErrInvalid) {
		t.Fatal("uninitialized domain accepted cleanup", err)
	}
}
